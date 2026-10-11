package sqlread

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSnapshotRequesterAdmission(t *testing.T) {
	_, path, cfg, sc := snapshotFixture(t)
	sc.MaxConcurrent = 2
	sc.MaxConcurrentPerRequester = 1
	s := newTestSnapshot(t, path, cfg, sc)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- s.ReadFiltered(context.Background(), "alice", func(*FilteredTx) error { close(started); <-release; return nil })
	}()
	<-started
	// Even another read API shares the authenticated requester's admission.
	_, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay")
	if !errors.Is(err, ErrSnapshotRequesterLimit) {
		t.Errorf("same requester: %v", err)
	}
	result, _, err := s.Query(context.Background(), "bob", "SELECT id FROM pay")
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "c" {
		t.Errorf("other requester: %+v %v", result, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
		t.Fatal(err)
	}
	if len(s.requesters) != 0 {
		t.Fatalf("idle requesters retained: %v", s.requesters)
	}
	cleanSnapshot(t, s)
}

func TestSnapshotRequesterReleaseOnFailures(t *testing.T) {
	_, path, cfg, sc := snapshotFixture(t)
	cfg.Timeout = 20 * time.Millisecond
	sc.MaxConcurrent = 1
	sc.MaxConcurrentPerRequester = 1
	s := newTestSnapshot(t, path, cfg, sc)
	for _, sql := range []string{"SELECT secret FROM pay", "WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x) SELECT sum(n) FROM x"} {
		if _, _, err := s.Query(context.Background(), "alice", sql); err == nil {
			t.Fatal("expected rejection")
		}
		if len(s.requesters) != 0 {
			t.Fatal("failed query retained admission")
		}
		cleanSnapshot(t, s)
	}
	// Occupy global capacity and cancel an admitted waiter.
	s.slots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := s.Query(ctx, "alice", "SELECT id FROM pay"); done <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		s.requesterMu.Lock()
		admitted := s.requesters["alice"] == 1
		s.requesterMu.Unlock()
		if admitted {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-s.slots
			t.Fatal("waiter never admitted")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-s.slots
	if len(s.requesters) != 0 {
		t.Fatal("canceled waiter retained admission")
	}
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadFiltered(context.Background(), "alice", func(*FilteredTx) error { return errors.New("synthetic failure") }); err == nil {
		t.Fatal("expected callback error")
	}
	if len(s.requesters) != 0 {
		t.Fatal("callback retained admission")
	}
}

func TestSnapshotRequesterConfig(t *testing.T) {
	for _, limit := range []int{-1, 3} {
		sc := SnapshotConfig{MaxConcurrent: 2, MaxConcurrentPerRequester: limit}
		if NormalizeSnapshotConfig(&sc) == nil {
			t.Fatalf("accepted %d", limit)
		}
	}
	sc := SnapshotConfig{}
	if err := NormalizeSnapshotConfig(&sc); err != nil || sc.MaxConcurrentPerRequester != 0 {
		t.Fatalf("default changed: %+v %v", sc, err)
	}
}

func TestSnapshotRequesterLimitDisabled(t *testing.T) {
	_, path, cfg, sc := snapshotFixture(t)
	sc.MaxConcurrent = 2
	s := newTestSnapshot(t, path, cfg, sc)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- s.ReadFiltered(context.Background(), "alice", func(*FilteredTx) error { close(started); <-release; return nil })
	}()
	<-started
	_, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay")
	close(release)
	if first := <-done; first != nil {
		t.Fatal(first)
	}
	if err != nil {
		t.Fatalf("default forbids existing concurrency: %v", err)
	}
	if len(s.requesters) != 0 {
		t.Fatal("disabled limit tracks requesters")
	}
}
