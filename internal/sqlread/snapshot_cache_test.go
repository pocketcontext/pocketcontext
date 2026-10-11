package sqlread

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestSnapshotCacheReuseAndCommitInvalidation(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	read := func() time.Time {
		t.Helper()
		_, at, err := s.Query(context.Background(), "alice", "SELECT salary FROM pay")
		if err != nil {
			t.Fatal(err)
		}
		return at
	}
	first := read()
	if next := read(); next != first {
		t.Fatal("unchanged source rebuilt")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("UPDATE pay SET salary=400 WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if next := read(); next != first {
		t.Fatal("rollback invalidated")
	}
	if _, err = db.Exec("UPDATE pay SET salary=400 WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	if next := read(); next == first {
		t.Fatal("commit did not invalidate")
	}
	r, _, err := s.Query(context.Background(), "alice", "SELECT sum(salary) FROM pay")
	if err != nil || r.Rows[0][0] != int64(600) {
		t.Fatalf("stale: %#v %v", r, err)
	}
}

func TestSnapshotCacheConcurrentBuildReuse(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc.MaxConcurrent = 4
	s := newTestSnapshot(t, path, c, sc)
	var wg sync.WaitGroup
	start := make(chan struct{})
	ats := make(chan time.Time, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, at, err := s.Query(context.Background(), "alice", "SELECT id FROM pay")
			if err != nil {
				t.Error(err)
				return
			}
			ats <- at
		}()
	}
	close(start)
	wg.Wait()
	close(ats)
	var first time.Time
	for at := range ats {
		if first.IsZero() {
			first = at
		}
		if at != first {
			t.Fatal("concurrent requests rebuilt snapshot")
		}
	}
}

func TestSnapshotCacheEvictionKeepsActiveReader(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc.MaxConcurrent = 1
	s := newTestSnapshot(t, path, c, sc)
	entry, err := s.acquireSnapshot(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.acquireSnapshot(context.Background(), "bob")
	if err != nil {
		t.Fatal(err)
	}
	s.releaseSnapshot(other)
	if len(s.cache) != 1 || entry.retained {
		t.Fatal("cache limit not enforced")
	}
	r, err := entry.engine.Query(context.Background(), "SELECT sum(salary) FROM pay")
	if err != nil || r.Rows[0][0] != int64(300) {
		t.Fatalf("active reader lost: %#v %v", r, err)
	}
	if _, err = os.Stat(entry.dir); err != nil {
		t.Fatal(err)
	}
	s.releaseSnapshot(entry)
	if _, err = os.Stat(entry.dir); !os.IsNotExist(err) {
		t.Fatal("evicted directory remains", err)
	}
}

func TestSnapshotCacheVolatilePolicyBypass(t *testing.T) {
	for _, fn := range []string{"datetime('now')", `"datetime"('now')`, "strftime('%s','now')"} {
		t.Run(fn, func(t *testing.T) {
			_, path, c, sc := snapshotFixture(t)
			sc.Filters["pay"] += " AND " + fn + " IS NOT NULL"
			s := newTestSnapshot(t, path, c, sc)
			if s.cacheable {
				t.Fatal("time-dependent policy cached")
			}
			for i := 0; i < 2; i++ {
				if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
					t.Fatal(err)
				}
			}
			if len(s.cache) != 0 {
				t.Fatal("volatile cache retained")
			}
			cleanSnapshot(t, s)
		})
	}
}

func TestSnapshotCacheWaitCancellation(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	s.building["alice"] = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _, err := s.Query(ctx, "alice", "SELECT id FROM pay")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(s.slots) != 0 {
		t.Fatal("capacity leaked")
	}
}

func TestSnapshotCacheMonitorFailureDoesNotServeCachedData(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
		t.Fatal(err)
	}
	if err := s.monitor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); !errors.Is(err, ErrSnapshotBuild) {
		t.Fatalf("failed version monitor served cache: %v", err)
	}
}
