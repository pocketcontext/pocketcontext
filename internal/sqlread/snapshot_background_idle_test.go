package sqlread

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackgroundIdleTimeoutConfig(t *testing.T) {
	for _, ms := range []int{-1, 0, 59999, 60000, 86400000, 86400001} {
		cfg := BackgroundSnapshotConfig{IdleTimeoutMS: ms}
		valid := ms == 0 || (ms >= 60000 && ms <= 86400000)
		if err := normalizeBackground(&cfg); (err == nil) != valid || cfg.IdleTimeoutMS != ms {
			t.Fatalf("idleTimeoutMs %d: normalized to %d, err %v", ms, cfg.IdleTimeoutMS, err)
		}
	}
}

func TestBackgroundIdleRequesterRetiredDuringWrites(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	sc = backgroundConfig(sc)
	sc.Background.IdleTimeoutMS = 60000
	s := newTestSnapshot(t, path, c, sc)
	started, resume := make(chan struct{}), make(chan struct{})
	var builds atomic.Int32
	s.SetSnapshotInitializer(func(ctx context.Context, _ *SnapshotLease) error {
		if builds.Add(1) == 2 {
			close(started)
			select {
			case <-resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ctx := context.Background()
	lease, err := s.AcquireSnapshot(ctx, "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	original := lease.Metadata().Generation
	if _, err := db.Exec("UPDATE pay SET salary=201 WHERE id='b'"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not start")
	}
	// Simulate a requester going idle while the worker is busy. Another
	// relevant commit would normally queue an immediate follow-up build.
	b := s.background
	b.mu.Lock()
	b.users["alice"].used = time.Now().Add(-2 * time.Minute)
	b.mu.Unlock()
	if _, err := db.Exec("DELETE FROM managers WHERE requester='alice'"); err != nil {
		t.Fatal(err)
	}
	close(resume)
	awaitBackground(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.users["alice"] == nil
	})
	if builds.Load() != 2 || s.BackgroundMetrics().IdleRetirements != 1 {
		t.Fatal("expected one idle retirement without another rebuild")
	}
	dir := lease.Directory()
	if _, err := os.Stat(dir); err != nil {
		t.Fatal("leased directory removed", err)
	}
	lease.Close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("lease release did not remove retired directory", err)
	}
	r, m, err := s.QueryWithOptions(ctx, "alice", "SELECT count(*) FROM pay", SnapshotReadOptions{})
	if err != nil || builds.Load() != 3 || r.Rows[0][0] != int64(0) || m.Generation == original {
		t.Fatalf("returning requester cold build: %#v %#v %v", r, m, err)
	}
}

func TestBackgroundIdleExpiryWhileReadWaits(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc = backgroundConfig(sc)
	sc.Background.IdleTimeoutMS = 60000
	s := newTestSnapshot(t, path, c, sc)
	var builds atomic.Int32
	started, resume := make(chan struct{}), make(chan struct{})
	s.SetSnapshotInitializer(func(ctx context.Context, _ *SnapshotLease) error {
		if builds.Add(1) == 1 {
			close(started)
			select {
			case <-resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	done := make(chan error, 1)
	go func() {
		_, _, err := s.QueryWithOptions(context.Background(), "alice", "SELECT id FROM pay", SnapshotReadOptions{Consistency: "fresh"})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("cold build did not start")
	}
	// The running read still wants its generation. Bob's status request
	// queued a cold build but already returned to its caller.
	if _, err := s.SnapshotStatus(context.Background(), "bob"); err != nil {
		t.Fatal(err)
	}
	b := s.background
	b.mu.Lock()
	b.users["alice"].used = time.Now().Add(-2 * time.Minute)
	b.users["bob"].used = time.Now().Add(-2 * time.Minute)
	b.mu.Unlock()
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	awaitBackground(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.users["bob"] == nil
	})
	if builds.Load() != 1 {
		t.Fatalf("one waiting reader and one expired queue entry needed %d builds", builds.Load())
	}
	b.mu.Lock()
	dir := b.users["alice"].current.dir
	b.users["alice"].used = time.Now().Add(-2 * time.Minute)
	b.mu.Unlock()
	awaitBackground(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.users["alice"] == nil
	})
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("unused idle generation remained on disk", err)
	}
}
