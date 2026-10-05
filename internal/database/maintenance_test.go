package database_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/database"
)

func fixture(t *testing.T) (*database.Controller, *sql.DB, string) {
	t.Helper()
	dir := t.TempDir()
	c, err := database.NewController(dir)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := c.Connect(filepath.Join(dir, "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	db := pool.DB()
	t.Cleanup(func() { db.Close() })
	if _, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test(value) VALUES ('original')"); err != nil {
		t.Fatal(err)
	}
	return c, db, dir
}
func freeze(t *testing.T, c *database.Controller) {
	t.Helper()
	if _, err := c.BeginFreeze(c.Status().Generation); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.CompleteFreeze(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestFreezePreparedStatementsAndNewConnections(t *testing.T) {
	c, db, _ := fixture(t)
	prepared, err := db.Prepare("INSERT INTO test(value) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	pragma, err := db.Prepare("PRAGMA user_version=42")
	if err != nil {
		t.Fatal(err)
	}
	defer pragma.Close()
	freeze(t, c)
	if _, err = prepared.Exec("denied"); err == nil {
		t.Fatal("cached write succeeded")
	}
	if _, err = pragma.Exec(); err == nil {
		t.Fatal("cached pragma succeeded")
	}
	for _, q := range []string{"INSERT INTO test(value) VALUES ('denied')", "UPDATE test SET value='denied'", "DELETE FROM test", "DROP TABLE test", "PRAGMA query_only=OFF; INSERT INTO test(value) VALUES ('denied')", "PRAGMA wal_checkpoint(TRUNCATE)", "ATTACH ':memory:' AS other"} {
		if _, err = db.Exec(q); err == nil {
			t.Errorf("write succeeded: %s", q)
		}
	}
	db.SetMaxIdleConns(0)
	var value string
	if err = db.QueryRow("SELECT value FROM test").Scan(&value); err != nil || value != "original" {
		t.Fatalf("frozen read: %q %v", value, err)
	}
	if _, err = db.Exec("INSERT INTO test(value) VALUES ('new connection')"); err == nil {
		t.Fatal("new connection wrote")
	}
	if _, err = c.Unfreeze(context.Background(), c.Status().Generation); err != nil {
		t.Fatal(err)
	}
	if _, err = prepared.Exec("resumed"); err != nil {
		t.Fatal(err)
	}
}
func TestTransactionDrainAndRetry(t *testing.T) {
	c, db, _ := fixture(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT INTO test(value) VALUES ('committed')"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.BeginFreeze(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = c.CompleteFreeze(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain should time out: %v", err)
	}
	if c.Status().State != "draining" {
		t.Fatal(c.Status())
	}
	if _, err = db.Exec("INSERT INTO test(value) VALUES ('blocked')"); err == nil {
		t.Fatalf("new operation: %v", err)
	}
	if _, err = tx.Exec("INSERT INTO test(value) VALUES ('already admitted')"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.BeginFreeze(c.Status().Generation); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CompleteFreeze(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow("SELECT count(*) FROM test").Scan(&count); err != nil || count != 3 {
		t.Fatalf("count %d: %v", count, err)
	}
	if _, err = c.Unfreeze(context.Background(), 0); !errors.Is(err, database.ErrGenerationConflict) {
		t.Fatalf("stale transition: %v", err)
	}
}
func TestRejectUntrackedSQLTransactions(t *testing.T) {
	_, db, _ := fixture(t)
	for _, q := range []string{"BEGIN", "BEGIN; INSERT INTO test(value) VALUES ('bad')", "SAVEPOINT untracked"} {
		if _, err := db.Exec(q); err == nil {
			t.Fatalf("untracked transaction allowed: %s", q)
		}
	}
}
func TestReturningRowsAreDrained(t *testing.T) {
	c, db, _ := fixture(t)
	rows, err := db.Query("INSERT INTO test(value) VALUES ('returning') RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	if _, err = c.BeginFreeze(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = c.CompleteFreeze(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rows not drained: %v", err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CompleteFreeze(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestMarkerValidationAndRestart(t *testing.T) {
	for _, data := range []string{"{}", "{\"readOnly\":false}", "{\"generation\":0}", "{\"readOnly\":null,\"generation\":0}", "{\"readOnly\":true,\"generation\":1} {}", "{\"readOnly\":true,\"generation\":1,\"other\":true}"} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, database.MarkerName), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := database.NewController(dir); err == nil {
			t.Fatalf("accepted malformed marker: %s", data)
		}
	}
	c, db, dir := fixture(t)
	freeze(t, c)
	db.Close()
	restarted, err := database.NewController(dir)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Status().State != "read_only" || restarted.Status().Generation != 1 {
		t.Fatal(restarted.Status())
	}
	pool, err := restarted.Connect(filepath.Join(dir, "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = pool.DB().Exec("INSERT INTO test(value) VALUES ('restart')"); err == nil {
		t.Fatal("restart wrote")
	}
	missing, err := restarted.Connect(filepath.Join(dir, "missing.db"))
	if err == nil {
		defer missing.Close()
		err = missing.DB().Ping()
	}
	if err == nil {
		t.Fatal("frozen startup created missing database")
	}
}
func TestFrozenPocketBaseBootstrap(t *testing.T) {
	dir := t.TempDir()
	c, err := database.NewController(dir)
	if err != nil {
		t.Fatal(err)
	}
	app := pocketbase.NewWithConfig(pocketbase.Config{DefaultDataDir: dir, DBConnect: c.Connect})
	if err = app.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	freeze(t, c)
	if err = app.ResetBootstrapState(); err != nil {
		t.Fatal(err)
	}
	restarted, err := database.NewController(dir)
	if err != nil {
		t.Fatal(err)
	}
	app2 := pocketbase.NewWithConfig(pocketbase.Config{DefaultDataDir: dir, DBConnect: restarted.Connect})
	if err = app2.Bootstrap(); err != nil {
		t.Fatalf("compatible frozen bootstrap: %v", err)
	}
	defer app2.ResetBootstrapState()
	if err = app2.RunAllMigrations(); err != nil {
		t.Fatalf("compatible frozen migrations: %v", err)
	}
	pending := core.MigrationsList{}
	pending.Register(func(tx core.App) error {
		_, err := tx.DB().NewQuery("CREATE TABLE must_not_exist (id TEXT)").Execute()
		return err
	}, nil, "9999999999_read_only_test.go")
	if _, err = core.NewMigrationsRunner(app2, pending).Up(); err == nil {
		t.Fatal("pending migration succeeded while frozen")
	}
	if _, err = app2.AuxDB().NewQuery("CREATE TABLE must_not_exist (id TEXT)").Execute(); err == nil {
		t.Fatal("auxiliary database accepted writes")
	}

}

func TestFailedMarkerWriteClosesWritesAndCanRetry(t *testing.T) {
	c, db, dir := fixture(t)
	path := filepath.Join(dir, database.MarkerName)
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginFreeze(0); err == nil {
		t.Fatal("marker write unexpectedly succeeded")
	}
	if _, err := db.Exec("INSERT INTO test(value) VALUES ('after disk failure')"); err == nil {
		t.Fatal("writes still allowed after marker failure")
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM test").Scan(&count); err != nil {
		t.Fatalf("authentication-style reads unavailable: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginFreeze(c.Status().Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CompleteFreeze(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted, err := database.NewController(dir)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Status().State != "read_only" {
		t.Fatal(restarted.Status())
	}
}

func TestDrainAllowsAuthenticationReadsAndRollback(t *testing.T) {
	c, db, _ := fixture(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT INTO test(value) VALUES ('rolled back')"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.BeginFreeze(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = c.CompleteFreeze(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow("SELECT count(*) FROM test").Scan(&count); err != nil || count != 1 {
		t.Fatalf("read during drain: %d %v", count, err)
	}
	if _, err = tx.Exec("COMMIT"); err == nil {
		t.Fatal("unmanaged commit accepted")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.CompleteFreeze(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM test").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rolled back count: %d %v", count, err)
	}
}

func TestFrozenTransactionRemainsFrozenAcrossUnfreeze(t *testing.T) {
	c, db, _ := fixture(t)
	freeze(t, c)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = c.Unfreeze(context.Background(), c.Status().Generation); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec("INSERT INTO test(value) VALUES ('old read-only transaction')"); err == nil {
		t.Fatal("old read-only transaction gained write access")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO test(value) VALUES ('new write')"); err != nil {
		t.Fatal(err)
	}
}

func TestFrozenExistingPoolCannotRecreateMissingDatabase(t *testing.T) {
	c, db, dir := fixture(t)
	db.SetMaxIdleConns(0)
	freeze(t, c)
	path := filepath.Join(dir, "data.db")
	if err := os.Rename(path, path+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err == nil {
		t.Fatal("frozen pool recreated missing database")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("missing database unexpectedly created: %v", err)
	}
}
