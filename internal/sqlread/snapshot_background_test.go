package sqlread

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkBackgroundSnapshotLarge exercises a corpus beyond both former Wiki
// ceilings. Data and indexes are synthetic, local and removed by the harness.
func BenchmarkBackgroundSnapshotLarge(b *testing.B) {
	path := filepath.Join(b.TempDir(), "source.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE documents(id TEXT PRIMARY KEY, body TEXT);
	WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<120000)
	INSERT INTO documents SELECT printf('%015d',i),printf('%01024d',i) FROM n`)
	if err != nil {
		b.Fatal(err)
	}
	cfg := Config{Tables: map[string][]string{"documents": {"id", "body"}}, MaxRows: 500, MaxBytes: 1 << 20}
	sc := SnapshotConfig{Filters: map[string]string{"documents": "1"}, MaxRows: 100000, MaxBytes: 64 << 20,
		Background: &BackgroundSnapshotConfig{MaxDiskBytes: 512 << 20, MinFreeBytes: 1, BuildTimeoutMS: 120000}}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s, err := NewSnapshotSource(path, cfg, sc)
		if err != nil {
			b.Fatal(err)
		}
		r, m, err := s.QueryWithOptions(context.Background(), "synthetic", "SELECT count(*),sum(length(body)) FROM documents", SnapshotReadOptions{Consistency: "fresh"})
		s.Close()
		if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != int64(120000) || r.Rows[0][1] != int64(122880000) {
			b.Fatalf("incomplete corpus: %#v %v", r, err)
		}
		b.ReportMetric(float64(m.DiskBytes), "snapshot_bytes")
	}
}

func backgroundConfig(sc SnapshotConfig) SnapshotConfig {
	sc.Background = &BackgroundSnapshotConfig{MaxDiskBytes: 16 << 20, MinFreeBytes: 1, BuildTimeoutMS: 5000, RefreshIntervalMS: 10, RetryDelayMS: 10}
	return sc
}

func awaitBackground(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background condition did not complete")
}

func TestBackgroundSnapshotIgnoresCorpusCapsPreservesQueryLimits(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	sc = backgroundConfig(sc)
	sc.MaxRows, sc.MaxBytes = 1, 1024
	c.MaxRows, c.MaxBytes = 1, 4096
	large := strings.Repeat("x", 8192)
	if _, err := db.Exec("UPDATE pay SET salary=? WHERE id='a'", large); err != nil {
		t.Fatal(err)
	}
	s := newTestSnapshot(t, path, c, sc)
	r, m, err := s.QueryWithOptions(context.Background(), "alice", "SELECT count(*) FROM pay", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil || r.Rows[0][0] != int64(2) || m.Rows != 2 {
		t.Fatalf("complete corpus: %#v %#v %v", r, m, err)
	}
	read, err := s.PrepareSnapshotRead(FilteredReadSpec{Table: "pay", Columns: []string{"id", "salary"}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	err = lease.Read(context.Background(), read, []string{"a"}, func(values []any) error { found = values[1] == large; return nil })
	lease.Close()
	if err != nil || !found {
		t.Fatalf("trusted scalar read: %v %v", found, err)
	}
	if _, _, err := s.QueryWithOptions(context.Background(), "alice", "SELECT salary FROM pay WHERE id='a'", SnapshotReadOptions{}); err == nil {
		t.Fatal("ordinary SQL scalar limit lost")
	}
	r, _, err = s.QueryWithOptions(context.Background(), "alice", "SELECT id FROM pay", SnapshotReadOptions{})
	if err != nil || !r.Truncated || len(r.Rows) != 1 {
		t.Fatalf("output cap: %#v %v", r, err)
	}
	if _, err := s.PrepareSnapshotRead(FilteredReadSpec{Table: "pay", Columns: []string{"id"}, Where: "EXISTS(SELECT 1 FROM managers)"}); err == nil {
		t.Fatal("policy-only table exposed")
	}
}

func TestBackgroundSnapshotPublishesDuringWritesAndKeepsOldLeases(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, backgroundConfig(sc))
	started, release := make(chan struct{}), make(chan struct{})
	var builds atomic.Int32
	s.SetSnapshotInitializer(func(ctx context.Context, l *SnapshotLease) error {
		if builds.Add(1) == 2 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	old, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldGen, oldDir := old.Metadata().Generation, old.Directory()
	if _, err := db.Exec("DELETE FROM managers WHERE requester='alice'"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh not started")
	}
	r, m, err := s.QueryWithOptions(context.Background(), "alice", "SELECT count(*) FROM pay", SnapshotReadOptions{})
	if err != nil || r.Rows[0][0] != int64(2) || m.Generation != oldGen || !m.Refreshing {
		t.Fatalf("old availability: %#v %#v %v", r, m, err)
	}
	// Another commit during index construction must not discard this complete
	// generation. A later refresh follows it without starving publication.
	if _, err := db.Exec("UPDATE pay SET salary=901 WHERE id='c'"); err != nil {
		t.Fatal(err)
	}
	close(release)
	r, m, err = s.QueryWithOptions(context.Background(), "alice", "SELECT count(*) FROM pay", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil || r.Rows[0][0] != int64(0) || m.Generation == oldGen {
		t.Fatalf("eventual revocation: %#v %#v %v", r, m, err)
	}
	if _, err := os.Stat(oldDir); err != nil {
		t.Fatal("active old lease removed", err)
	}
	old.Close()
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatal("retired generation retained", err)
	}
	if _, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{ExpectedGeneration: oldGen}); !errors.Is(err, ErrSnapshotGeneration) {
		t.Fatal("generation conflict", err)
	}
	if _, err := s.AcquireSnapshot(context.Background(), "bob", SnapshotReadOptions{AtLeast: m.Generation}); !errors.Is(err, ErrSnapshotGeneration) {
		t.Fatal("requester-bound token", err)
	}
}

func TestBackgroundSnapshotFailureRetainsCurrentAndRecovers(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, backgroundConfig(sc))
	var fail atomic.Bool
	s.SetSnapshotInitializer(func(context.Context, *SnapshotLease) error {
		if fail.Load() {
			return ErrSnapshotResource
		}
		return nil
	})
	_, first, err := s.QueryWithOptions(context.Background(), "alice", "SELECT sum(salary) FROM pay", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if _, err := db.Exec("UPDATE pay SET salary=101 WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	awaitBackground(t, func() bool {
		m, _ := s.SnapshotStatus(context.Background(), "alice")
		return m.LastError == "storage_budget"
	})
	r, m, err := s.QueryWithOptions(context.Background(), "alice", "SELECT sum(salary) FROM pay", SnapshotReadOptions{})
	if err != nil || m.Generation != first.Generation || r.Rows[0][0] != int64(300) || !m.Stale {
		t.Fatalf("failed refresh availability: %#v %#v %v", r, m, err)
	}
	if _, _, err := s.QueryWithOptions(context.Background(), "alice", "SELECT id FROM pay", SnapshotReadOptions{Consistency: "fresh"}); !errors.Is(err, ErrSnapshotResource) {
		t.Fatal("fresh failure classification", err)
	}
	fail.Store(false)
	awaitBackground(t, func() bool {
		r, _, err := s.QueryWithOptions(context.Background(), "alice", "SELECT sum(salary) FROM pay", SnapshotReadOptions{})
		return err == nil && r.Rows[0][0] == int64(301)
	})
}

func TestBackgroundLeaseAdmissionAndClose(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc = backgroundConfig(sc)
	sc.MaxConcurrentPerRequester = 1
	s := newTestSnapshot(t, path, c, sc)
	lease, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{}); !errors.Is(err, ErrSnapshotRequesterLimit) {
		t.Fatal("lease admission", err)
	}
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("closed an active lease")
	case <-time.After(20 * time.Millisecond):
	}
	lease.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not drain")
	}
}

func TestBackgroundVolatileFreshBarrierAndPeriodicRefresh(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc = backgroundConfig(sc)
	sc.Filters["pay"] += " AND datetime('now') IS NOT NULL"
	s := newTestSnapshot(t, path, c, sc)
	_, a, err := s.QueryWithOptions(context.Background(), "alice", "SELECT id FROM pay", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := s.QueryWithOptions(context.Background(), "alice", "SELECT id FROM pay", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil || a.Generation == b.Generation {
		t.Fatal("fresh reused volatile policy", err)
	}
	awaitBackground(t, func() bool {
		m, _ := s.SnapshotStatus(context.Background(), "alice")
		return m.Generation != b.Generation
	})
}
