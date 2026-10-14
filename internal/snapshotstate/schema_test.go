package snapshotstate

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestTransactionalTokensCoverCascadesAndRollback(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA foreign_keys=ON;
 CREATE TABLE parents(id INTEGER PRIMARY KEY);
 CREATE TABLE children(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) ON DELETE CASCADE);
 CREATE TABLE "quoted' table"(id INTEGER PRIMARY KEY);
 INSERT INTO parents VALUES(1); INSERT INTO children VALUES(1,1);`); err != nil {
		t.Fatal(err)
	}
	for _, definition := range Definitions([]string{"parents", "children", "quoted' table"}) {
		if _, err := db.Exec(definition.SQL); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"parents", "children", "quoted' table"} {
		if _, err := db.Exec(SeedSQL(table)); err != nil {
			t.Fatal(err)
		}
	}
	token := func(table string) string {
		t.Helper()
		var value string
		if err := db.QueryRow("SELECT version FROM "+Quote(Table)+" WHERE table_name=?", table).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := token("children")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("DELETE FROM parents"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if token("children") != before {
		t.Fatal("rollback changed committed token")
	}
	if _, err := db.Exec("DELETE FROM parents"); err != nil {
		t.Fatal(err)
	}
	if token("children") == before {
		t.Fatal("cascading deletion did not rotate child token")
	}
	before = token("quoted' table")
	if _, err := db.Exec(`INSERT INTO "quoted' table" VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	if token("quoted' table") == before {
		t.Fatal("quoted table was not tracked")
	}
}
