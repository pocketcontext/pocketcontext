package sqlread

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestBackgroundCachedUserEvictionPreservesActiveLease(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc = backgroundConfig(sc)
	sc.Background.MaxCachedUsers = 1
	s := newTestSnapshot(t, path, c, sc)
	alice, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Close()
	bob, err := s.AcquireSnapshot(context.Background(), "bob", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Close()
	s.background.mu.Lock()
	correct := len(s.background.users) == 1 && s.background.users["bob"] != nil && s.background.retired[alice.entry]
	s.background.mu.Unlock()
	if !correct {
		t.Fatal("cache did not evict Alice while retaining her active generation")
	}
	if _, err := os.Stat(alice.Directory()); err != nil {
		t.Fatal("eviction removed a leased generation", err)
	}
	r, err := alice.entry.engine.Query(context.Background(), "SELECT sum(salary) FROM pay")
	if err != nil || r.Rows[0][0] != int64(300) {
		t.Fatalf("evicted lease lost its authorized data: %#v %v", r, err)
	}
	alice.Close()
	if _, err := os.Stat(alice.Directory()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("evicted generation survived its final lease", err)
	}
	if _, err := os.Stat(bob.Directory()); err != nil {
		t.Fatal("closing retired generation removed the current user", err)
	}
}

func TestBackgroundRetiredGenerationConsumesDiskBudgetUntilReleased(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	value := strings.Repeat("x", 65536)
	if _, err := db.Exec("UPDATE pay SET salary=? WHERE id='a'", value); err != nil {
		t.Fatal(err)
	}
	sc = backgroundConfig(sc)
	// Measure the real SQLite page footprint, then allow two generations plus
	// the reservation admission margin, but not a third complete generation.
	probe := newTestSnapshot(t, path, c, sc)
	p, err := probe.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	bytes := p.Metadata().DiskBytes
	p.Close()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if bytes <= 32768 {
		t.Fatalf("fixture must exceed the reservation admission margin: %d", bytes)
	}
	sc.Background.MaxDiskBytes = 2*bytes + 32768
	s := newTestSnapshot(t, path, c, sc)
	old, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err := db.Exec("UPDATE pay SET salary=? WHERE id='a'", strings.Repeat("y", len(value))); err != nil {
		t.Fatal(err)
	}
	current, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	generation := current.Metadata().Generation
	current.Close()
	if _, err := db.Exec("UPDATE pay SET salary=? WHERE id='a'", strings.Repeat("z", len(value))); err != nil {
		t.Fatal(err)
	}
	third, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
	if third != nil {
		third.Close()
	}
	if !errors.Is(err, ErrSnapshotResource) {
		t.Fatalf("pinned retired bytes were not charged: %v", err)
	}
	r, m, err := s.QueryWithOptions(context.Background(), "alice", "SELECT count(*) FROM pay", SnapshotReadOptions{})
	if err != nil || r.Rows[0][0] != int64(2) || m.Generation != generation {
		t.Fatalf("quota failure lost the current generation: %#v %#v %v", r, m, err)
	}
	old.Close()
	awaitBackground(t, func() bool {
		lease, err := s.AcquireSnapshot(context.Background(), "alice", SnapshotReadOptions{Consistency: "fresh"})
		if err != nil {
			return false
		}
		defer lease.Close()
		return lease.Metadata().Generation != generation
	})
	used, err := directoryBytes(s.root)
	if err != nil || used > sc.Background.MaxDiskBytes {
		t.Fatalf("disk charge after recovery: %d > %d, %v", used, sc.Background.MaxDiskBytes, err)
	}
}
