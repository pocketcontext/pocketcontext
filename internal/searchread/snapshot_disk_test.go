package searchread

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func backgroundSearchConfig() *sqlread.BackgroundSnapshotConfig {
	return &sqlread.BackgroundSnapshotConfig{
		MaxDiskBytes: 64 << 20, MinFreeBytes: 1, BuildTimeoutMS: 5000,
		MaxBuilders: 1, MaxCachedUsers: 8, RefreshIntervalMS: 60000, RetryDelayMS: 100,
	}
}

func snapshotSearchLease(t *testing.T, source *sqlread.SnapshotSource, user string) *sqlread.SnapshotLease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lease, err := source.AcquireSnapshot(ctx, user, sqlread.SnapshotReadOptions{Consistency: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Close)
	return lease
}

func TestSnapshotDiskSearchPreparedAndLazyScopes(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	idx := privateConfig().Indexes["visible"]
	idx.Weights = []float64{5, 1}
	idx.SnapshotScopeWhere = "manifests.id = 'm1'"
	e, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"pages": idx}})
	lease := snapshotSearchLease(t, source, "alice")
	files, err := filepath.Glob(filepath.Join(lease.Directory(), "search-*.db"))
	if err != nil || len(files) != 1 {
		t.Fatalf("primary scope was not prepared before publication: %v %v", files, err)
	}
	info, err := os.Stat(files[0])
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() == 0 {
		t.Fatalf("private on-disk index: %+v %v", info, err)
	}
	db, err := sql.Open("sqlite3", files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var members int
	if err = db.QueryRow("SELECT count(*) FROM private_search").Scan(&members); err != nil || members != 3 {
		t.Fatalf("index must contain only selected publication members: %d %v", members, err)
	}
	current, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1", SnapshotGeneration: lease.Metadata().Generation})
	if err != nil || ids(current) != "d1,d2" || current.Snapshot == nil || current.Snapshot.Generation != lease.Metadata().Generation {
		t.Fatalf("prepared search: %+v %v", current, err)
	}
	history, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m3", SnapshotGeneration: lease.Metadata().Generation})
	if err != nil || ids(history) != "d1" {
		t.Fatalf("lazy historical search: %+v %v", history, err)
	}
	if history.Snapshot == nil || history.Snapshot.DiskBytes <= current.Snapshot.DiskBytes {
		t.Fatalf("lazy index missing from response disk usage: current=%+v historical=%+v", current.Snapshot, history.Snapshot)
	}
	files, err = filepath.Glob(filepath.Join(lease.Directory(), "search-*.db"))
	if err != nil || len(files) != 2 {
		t.Fatalf("historical disk cache: %v %v", files, err)
	}
	for _, scope := range []string{"m2", "absent"} {
		if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: scope}); !errors.Is(err, ErrScopeNotFound) {
			t.Fatalf("unavailable scope %q: %v", scope, err)
		}
	}
	if _, _, err := source.Query(context.Background(), "alice", "SELECT * FROM private_search"); err == nil {
		t.Fatal("FTS internal tables became SQL-readable")
	}
	staging, _ := filepath.Glob(filepath.Join(lease.Directory(), ".search-building-*"))
	if len(staging) != 0 {
		t.Fatalf("staging files leaked: %v", staging)
	}
}

func TestSnapshotDiskSearchExceedsLegacyCorpusLimit(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	if _, err := db.Exec("UPDATE docs SET body=? WHERE id='d1'", strings.Repeat("synthetic rocket ", 2048)); err != nil {
		t.Fatal(err)
	}
	idx := privateConfig().Indexes["all"]
	cfg := Config{Indexes: map[string]Index{"all": idx}, MaxIndexBytes: 1024, CacheBytes: 4096}
	legacy, _ := newPrivate(t, path, c, sc, cfg)
	if _, err := legacy.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"}); !errors.Is(err, ErrIndexLimit) {
		t.Fatalf("legacy limit: %v", err)
	}
	sc.Background = backgroundSearchConfig()
	sc.MaxRows, sc.MaxBytes = 2, 1024
	e, source := newPrivate(t, path, c, sc, cfg)
	lease := snapshotSearchLease(t, source, "alice")
	result, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket", SnapshotGeneration: lease.Metadata().Generation})
	if err != nil || len(result.Hits) != 3 {
		t.Fatalf("disk search still used legacy corpus limits: %+v %v", result, err)
	}
	if entries, _ := e.cache.stats(); entries != 0 {
		t.Fatal("background search populated the in-memory FTS cache")
	}
}

func TestSnapshotDiskSearchPreservesScopedRanking(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	cfg := Config{Indexes: map[string]Index{"pages": privateConfig().Indexes["pages"]}}
	legacy, _ := newPrivate(t, path, c, sc, cfg)
	want, err := legacy.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	sc.Background = backgroundSearchConfig()
	e, source := newPrivate(t, path, c, sc, cfg)
	lease := snapshotSearchLease(t, source, "alice")
	got, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1", SnapshotGeneration: lease.Metadata().Generation})
	if err != nil || got.Generation != want.Generation || !reflect.DeepEqual(got.Hits, want.Hits) {
		t.Fatalf("disk ranking differs: %+v / %+v %v", got, want, err)
	}
	lease.Close()
	// Hidden and visible-but-unselected documents must not affect BM25 statistics.
	if _, err := db.Exec("UPDATE docs SET body=? WHERE id IN ('b1','d3')", strings.Repeat("rocket ", 2000)); err != nil {
		t.Fatal(err)
	}
	updated := snapshotSearchLease(t, source, "alice")
	after, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1", SnapshotGeneration: updated.Metadata().Generation, ExpectedGeneration: got.Generation})
	if err != nil || after.Generation != got.Generation || !reflect.DeepEqual(after.Hits, got.Hits) {
		t.Fatalf("unrelated content changed scoped ranking: %+v %v", after, err)
	}
}

func TestSnapshotDiskSearchFailedPrimaryRetainsGeneration(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	idx := privateConfig().Indexes["pages"]
	idx.SnapshotScopeWhere = "manifests.id = 'm1'"
	e, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"pages": idx}})
	lease := snapshotSearchLease(t, source, "alice")
	before := lease.Metadata().Generation
	lease.Close()
	if _, err := db.Exec("UPDATE docs SET body=x'00ff' WHERE id='d1'"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if fresh, err := source.AcquireSnapshot(ctx, "alice", sqlread.SnapshotReadOptions{Consistency: "fresh"}); err == nil {
		fresh.Close()
		t.Fatal("generation with invalid primary index was published")
	}
	old, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"})
	if err != nil || old.Snapshot == nil || old.Snapshot.Generation != before || ids(old) != "d1,d2" {
		t.Fatalf("last complete SQL/search generation was lost: %+v %v", old, err)
	}
}

func TestSnapshotDiskIndexLivesExactlyWithGenerationLease(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	idx := privateConfig().Indexes["all"]
	_, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"all": idx}})
	old := snapshotSearchLease(t, source, "alice")
	dir := old.Directory()
	if _, err := db.Exec("UPDATE docs SET title='Changed' WHERE id='d1'"); err != nil {
		t.Fatal(err)
	}
	newer := snapshotSearchLease(t, source, "alice")
	if newer.Metadata().Generation == old.Metadata().Generation {
		t.Fatal("fresh read did not advance the snapshot")
	}
	files, err := filepath.Glob(filepath.Join(dir, "search-*.db"))
	if err != nil || len(files) != 1 {
		t.Fatalf("active old generation lost its index: %v %v", files, err)
	}
	old.Close()
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released old index generation was not removed: %v", err)
	}
}

func TestSnapshotDiskLazyFailureLeavesNoPartialIndex(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	idx := privateConfig().Indexes["pages"]
	e, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"pages": idx}})
	lease := snapshotSearchLease(t, source, "alice")
	// Invalid index data is a lazy-build failure; the completed SQL generation
	// remains queryable and the failed FTS file must never become a cache hit.
	lease.Close()
	if _, err := db.Exec("UPDATE docs SET body=x'00ff' WHERE id='d1'"); err != nil {
		t.Fatal(err)
	}
	lease = snapshotSearchLease(t, source, "alice")
	if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1", SnapshotGeneration: lease.Metadata().Generation}); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("invalid lazy build: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(lease.Directory(), "search-*.db"))
	staging, _ := filepath.Glob(filepath.Join(lease.Directory(), ".search-building-*"))
	if len(files) != 0 || len(staging) != 0 {
		t.Fatalf("partial index retained: files=%v staging=%v", files, staging)
	}
	if _, _, err := source.Query(context.Background(), "alice", "SELECT count(*) FROM docs"); err != nil {
		t.Fatalf("lazy index failure invalidated SQL: %v", err)
	}
}

func TestSnapshotDiskLazyBudgetLeavesNoPartialIndex(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	sc.Background.MaxDiskBytes = 65536
	idx := privateConfig().Indexes["pages"]
	e, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"pages": idx}})
	lease := snapshotSearchLease(t, source, "alice")
	if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1", SnapshotGeneration: lease.Metadata().Generation}); !errors.Is(err, sqlread.ErrSnapshotResource) {
		t.Fatalf("disk budget was not enforced: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(lease.Directory(), "search-*.db"))
	staging, _ := filepath.Glob(filepath.Join(lease.Directory(), ".search-building-*"))
	if len(files) != 0 || len(staging) != 0 {
		t.Fatalf("partial index retained: files=%v staging=%v", files, staging)
	}
	if _, _, err := source.Query(context.Background(), "alice", "SELECT count(*) FROM docs"); err != nil {
		t.Fatalf("search budget failure invalidated SQL: %v", err)
	}
}

func TestSnapshotDiskPrimaryBudgetReportedAsStorageFailure(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	sc.Background.MaxDiskBytes = 65536
	_, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"all": privateConfig().Indexes["all"]}})
	lease, err := source.AcquireSnapshot(context.Background(), "alice", sqlread.SnapshotReadOptions{Consistency: "fresh"})
	if lease != nil {
		lease.Close()
	}
	if !errors.Is(err, sqlread.ErrSnapshotResource) {
		t.Fatalf("primary FTS quota must be a snapshot storage failure: %v", err)
	}
}

func TestSnapshotDiskSearchStreamsValuesAboveQueryScalarLimit(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	if _, err := db.Exec("UPDATE docs SET body=? WHERE id='d1'", strings.Repeat("synthetic ", 130000)+"rocket"); err != nil {
		t.Fatal(err)
	}
	sc.Background = backgroundSearchConfig()
	e, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"all": privateConfig().Indexes["all"]}})
	lease := snapshotSearchLease(t, source, "alice")
	result, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket", SnapshotGeneration: lease.Metadata().Generation})
	if err != nil || len(result.Hits) != 3 {
		t.Fatalf("trusted streamed read inherited ordinary query scalar limit: %+v %v", result, err)
	}
}

func TestSnapshotDiskLazyWaitsForConcurrentGenerationReservation(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	sc.Background = backgroundSearchConfig()
	e, source := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"pages": privateConfig().Indexes["pages"]}})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	source.SetSnapshotInitializer(func(ctx context.Context, lease *sqlread.SnapshotLease) error {
		if lease.Requester() == "bob" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	alice := snapshotSearchLease(t, source, "alice")
	bobDone := make(chan error, 1)
	go func() {
		lease, err := source.AcquireSnapshot(context.Background(), "bob", sqlread.SnapshotReadOptions{Consistency: "fresh"})
		if lease != nil {
			lease.Close()
		}
		bobDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("other generation did not reserve storage")
	}
	type outcome struct {
		result Result
		err    error
	}
	searchDone := make(chan outcome, 1)
	go func() {
		result, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1", SnapshotGeneration: alice.Metadata().Generation})
		searchDone <- outcome{result, err}
	}()
	select {
	case got := <-searchDone:
		t.Fatalf("temporary reservation failed instead of waiting: %+v %v", got.result, got.err)
	case <-time.After(30 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case err := <-bobDone:
		if err != nil {
			t.Fatal("other generation failed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("other generation did not finish")
	}
	select {
	case got := <-searchDone:
		if got.err != nil || ids(got.result) != "d1,d2" {
			t.Fatalf("lazy search did not recover after storage release: %+v %v", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lazy search did not finish")
	}
}
