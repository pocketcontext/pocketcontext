package sqlread

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pocketcontext/pocketcontext/internal/snapshotstate"
)

func installTestTracking(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, definition := range snapshotstate.Definitions(tables) {
		if _, err := db.Exec(definition.SQL); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range tables {
		if _, err := db.Exec(snapshotstate.SeedSQL(table)); err != nil {
			t.Fatal(err)
		}
	}
}

func trackedSnapshot(t *testing.T) (*sql.DB, *SnapshotSource) {
	t.Helper()
	db, path, cfg, sc := snapshotFixture(t)
	_, err := db.Exec(`CREATE TABLE notes(id TEXT PRIMARY KEY, employee TEXT);
 CREATE TABLE stable(id TEXT PRIMARY KEY);
 CREATE TABLE accounts(id TEXT PRIMARY KEY, disabled INTEGER);
 CREATE TABLE unrelated(id TEXT PRIMARY KEY);
 INSERT INTO notes VALUES('note-a','a'),('note-c','c');
 INSERT INTO stable VALUES('static'); INSERT INTO accounts VALUES('alice',0);`)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tables["notes"] = []string{"id", "employee"}
	cfg.Tables["stable"] = []string{"id"}
	sc.Filters["notes"] = "EXISTS (SELECT 1 FROM pay JOIN managers ON employee=pay.id WHERE pay.id=notes.employee AND requester=:requester AND salary<500)"
	sc.Filters["stable"] = "length(:requester)>0"
	sc.TrackChanges, sc.AuthorizationTable = true, "accounts"
	installTestTracking(t, db, "pay", "managers", "notes", "stable", "accounts")
	return db, newTestSnapshot(t, path, cfg, sc)
}

func TestTrackedSnapshotsIncrementalDependenciesAndRevocation(t *testing.T) {
	db, s := trackedSnapshot(t)
	s.source.db.SetMaxOpenConns(1)
	conn, err := s.source.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = conn.Raw(func(raw any) error {
		return raw.(roConn).c.RegisterFunc("length", func(value string) int { calls++; return len(value) }, false)
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	read := func(query string) (Result, *snapshotEntry) {
		t.Helper()
		result, _, err := s.Query(context.Background(), "alice", query)
		if err != nil {
			t.Fatal(err)
		}
		return result, s.cache["alice"]
	}
	result, original := read("SELECT id FROM notes")
	if len(result.Rows) != 1 || calls != 1 {
		t.Fatalf("initial export: %#v calls=%d", result, calls)
	}
	// External SQLite writers run exactly the same transactional triggers.
	external, err := sql.Open("sqlite3", filepath.Join(filepath.Dir(s.sourcePathForTest()), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	if _, err := external.Exec("INSERT INTO unrelated VALUES('irrelevant')"); err != nil {
		t.Fatal(err)
	}
	_, next := read("SELECT id FROM notes")
	if next != original {
		t.Fatal("unrelated write invalidated snapshot")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE pay SET salary=700 WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	_, next = read("SELECT id FROM notes")
	if next != original {
		t.Fatal("rollback invalidated snapshot")
	}
	// Hold an old reader while a content field revokes a dependent table's row.
	held, err := s.acquireSnapshot(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := external.Exec("UPDATE pay SET salary=700 WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	result, next = read("SELECT id FROM notes")
	if len(result.Rows) != 0 || next == original {
		t.Fatal("content dependency failed to invalidate notes")
	}
	if calls != 1 {
		t.Fatal("unchanged table was reexported")
	}
	old, err := held.engine.Query(context.Background(), "SELECT id FROM notes")
	if err != nil || len(old.Rows) != 1 {
		t.Fatal("active old reader changed", err)
	}
	s.releaseSnapshot(held)
	if _, err := os.Stat(original.dir); !os.IsNotExist(err) {
		t.Fatal("old snapshot retained after last reader")
	}
	if _, err := db.Exec("DELETE FROM managers WHERE requester='alice'"); err != nil {
		t.Fatal(err)
	}
	result, _ = read("SELECT id FROM pay")
	if len(result.Rows) != 0 {
		t.Fatal("membership revocation served old data")
	}
	if calls != 1 {
		t.Fatal("unrelated export rebuilt after policy update")
	}
	if _, err := db.Exec("UPDATE accounts SET disabled=1 WHERE id='alice'"); err != nil {
		t.Fatal(err)
	}
	read("SELECT id FROM stable")
	if calls != 2 {
		t.Fatal("account change did not force complete revalidation")
	}
	for _, query := range []string{"SELECT * FROM _context_snapshot_versions", "SELECT * FROM managers", "SELECT secret FROM pay"} {
		if _, _, err := s.Query(context.Background(), "alice", query); err == nil {
			t.Fatal("exposed internal data")
		}
	}
	cleanSnapshot(t, s)
}

// The database path is obtained from SQLite's fixed connection metadata only in
// this trusted test helper; application queries cannot access this pragma.
func (s *SnapshotSource) sourcePathForTest() string {
	var path string
	_ = s.trustedMonitor(func() error {
		var seq int
		var name string
		return s.monitor.QueryRowContext(context.Background(), "PRAGMA database_list").Scan(&seq, &name, &path)
	})
	return path
}

func TestTrackedSnapshotsConcurrentCommitNeverReturnsMixedState(t *testing.T) {
	db, s := trackedSnapshot(t)
	s.source.db.SetMaxOpenConns(1)
	// Both notes and pay depend on this function, allowing a deterministic write
	// after the source transaction is pinned during incremental refresh.
	s.exports["pay"] += " AND length(:requester)>0"
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM notes"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE pay SET salary=300 WHERE id='a'"); err != nil {
		t.Fatal(err)
	}
	conn, err := s.source.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	err = conn.Raw(func(raw any) error {
		return raw.(roConn).c.RegisterFunc("length", func(value string) (int, error) {
			var writeErr error
			once.Do(func() { _, writeErr = db.Exec("DELETE FROM managers WHERE requester='alice'") })
			return len(value), writeErr
		}, false)
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := s.Query(context.Background(), "alice", "SELECT id FROM notes")
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("returned old/mixed incremental data: %#v %v", result, err)
	}
	cleanSnapshot(t, s)
}

func TestTrackedSnapshotsMissingMetadataAndSchemaChangeFailClosed(t *testing.T) {
	for _, change := range []string{
		"DELETE FROM _context_snapshot_versions WHERE table_name='managers'",
		"CREATE TABLE new_schema(id TEXT)",
	} {
		t.Run(change, func(t *testing.T) {
			db, s := trackedSnapshot(t)
			if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(change); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); !errors.Is(err, ErrSnapshotBuild) {
				t.Fatalf("served cache after metadata failure: %v", err)
			}
		})
	}
}

func TestTrackedSnapshotsIncrementalLimitsKeepOriginalIntact(t *testing.T) {
	db, s := trackedSnapshot(t)
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
		t.Fatal(err)
	}
	original := s.cache["alice"]
	// All retained unchanged rows still count toward the aggregate limits.
	s.snapshot.MaxRows = 4
	if _, err := db.Exec("INSERT INTO stable VALUES('second'),('third')"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM stable"); !errors.Is(err, ErrSnapshotLimit) {
		t.Fatalf("incremental row limit: %v", err)
	}
	if s.cache["alice"] != original {
		t.Fatal("failed refresh replaced original")
	}
	s.snapshot.MaxRows = 100000
	result, _, err := s.Query(context.Background(), "alice", "SELECT id FROM stable")
	if err != nil || len(result.Rows) != 3 {
		t.Fatal("failed refresh damaged recovery", err)
	}
	cleanSnapshot(t, s)
}
