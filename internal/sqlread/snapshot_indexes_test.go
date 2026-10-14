package sqlread

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotIndexesUseOnlyExportedOrdinaryKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
CREATE TABLE documents(id TEXT PRIMARY KEY, owner TEXT, title TEXT, rank INTEGER, secret TEXT,
  derived TEXT GENERATED ALWAYS AS (lower(title)) STORED);
CREATE INDEX private_named_public_index ON documents(owner, title COLLATE NOCASE DESC);
CREATE INDEX title_trim ON documents(title COLLATE RTRIM);
CREATE INDEX duplicate_id ON documents(id);
CREATE INDEX private_key ON documents(secret);
CREATE INDEX partly_private_key ON documents(id, secret);
CREATE INDEX private_predicate ON documents(rank) WHERE secret <> '';
CREATE INDEX public_predicate ON documents(rank) WHERE rank > 0;
CREATE INDEX expression_key ON documents(lower(title));
CREATE INDEX generated_key ON documents(derived);
CREATE TABLE grants(requester TEXT PRIMARY KEY, private_role TEXT);
CREATE TABLE numbers(id INTEGER PRIMARY KEY, value TEXT);
CREATE TABLE composite(a TEXT, b TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID;
CREATE TABLE "quoted""table"("id""key" TEXT PRIMARY KEY);
INSERT INTO documents(id,owner,title,rank,secret) VALUES
 ('a','alice','Alpha',1,'private-a'),('b','alice','beta',2,'private-b'),('c','bob','Gamma',3,'private-c');
INSERT INTO grants VALUES('alice','private-role');
INSERT INTO numbers VALUES(77,'number');
INSERT INTO composite VALUES('a','b');
INSERT INTO "quoted""table" VALUES('quoted');`)
	if err != nil {
		t.Fatal(err)
	}
	var before int
	if err := db.QueryRow("PRAGMA schema_version").Scan(&before); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Tables: map[string][]string{
		"documents": {"id", "owner", "title", "rank"}, "numbers": {"id", "value"},
		"composite": {"a", "b"}, `quoted"table`: {`id"key`},
	}}
	sc := SnapshotConfig{Filters: map[string]string{
		"documents": "owner=:requester AND EXISTS(SELECT 1 FROM grants WHERE requester=:requester)",
		"numbers":   "1", "composite": "1", `quoted"table`: "1",
	}, PolicyTables: map[string][]string{"grants": {"requester"}}}
	source := newTestSnapshot(t, path, cfg, sc)
	counts := map[string]int{}
	for _, index := range source.indexes {
		counts[index.table]++
		if strings.Contains(strings.Join(index.keys, ","), "secret") || index.table == "grants" {
			t.Fatalf("private index copied: %#v", index)
		}
	}
	for table, want := range map[string]int{"documents": 3, "numbers": 1, "composite": 1, `quoted"table`: 1} {
		if counts[table] != want {
			t.Fatalf("%s indexes: got %d, want %d (%#v)", table, counts[table], want, source.indexes)
		}
	}
	ctx := context.Background()
	for _, query := range []string{
		`SELECT title FROM documents WHERE id='a'`,
		`SELECT title FROM documents WHERE owner='alice' ORDER BY title COLLATE NOCASE DESC`,
		`SELECT value FROM numbers WHERE id=77`,
		`SELECT b FROM composite WHERE a='a'`,
		`SELECT "id""key" FROM "quoted""table" WHERE "id""key"='quoted'`,
	} {
		plan, _, err := source.Query(ctx, "alice", "EXPLAIN QUERY PLAN "+query)
		if err != nil || plan.Truncated {
			t.Fatalf("plan %s: %#v, %v", query, plan, err)
		}
		used := false
		for _, row := range plan.Rows {
			detail := row[3].(string)
			if strings.Contains(detail, "private_named") {
				t.Fatalf("source index name leaked: %s", detail)
			}
			used = used || strings.Contains(detail, "_snapshot_index_")
		}
		if !used {
			t.Fatalf("no persistent index used for %s: %#v", query, plan.Rows)
		}
	}
	r, _, err := source.Query(ctx, "alice", "SELECT id FROM documents ORDER BY id")
	if err != nil || len(r.Rows) != 2 || r.Rows[0][0] != "a" || r.Rows[1][0] != "b" {
		t.Fatalf("indexed snapshot privacy: %#v, %v", r, err)
	}
	for _, query := range []string{
		"SELECT secret FROM documents", "SELECT * FROM grants", "SELECT * FROM sqlite_schema",
		"PRAGMA index_list(documents)", "CREATE INDEX forbidden ON documents(rank)",
	} {
		if _, _, err := source.Query(ctx, "alice", query); err == nil {
			t.Fatalf("accepted unauthorized query: %s", query)
		}
	}
	var after int
	if err := db.QueryRow("PRAGMA schema_version").Scan(&after); err != nil || before != after {
		t.Fatalf("source schema mutated: before %d, after %d, error %v", before, after, err)
	}
}

func TestSnapshotIndexFailurePublishesNothing(t *testing.T) {
	_, path, cfg, sc := snapshotFixture(t)
	source := newTestSnapshot(t, path, cfg, sc)
	// Simulate an index construction failure after all authorized rows have
	// been copied. A partially built file must never become a query result.
	source.indexes = []snapshotIndex{{table: "nonexistent", keys: []string{`"id" COLLATE "BINARY" ASC`}}}
	r, _, err := source.Query(context.Background(), "alice", "SELECT id FROM pay")
	if !errors.Is(err, ErrSnapshotBuild) || len(r.Rows) != 0 {
		t.Fatalf("index failure exposed partial snapshot: %#v, %v", r, err)
	}
	if len(source.cache) != 0 {
		t.Fatal("failed index build retained a snapshot")
	}
	cleanSnapshot(t, source)
}

func TestSnapshotIndexesRespectPhysicalStorageLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE documents(id INTEGER, a TEXT, b TEXT, c TEXT, d TEXT)"); err != nil {
		t.Fatal(err)
	}
	// Many different wide keys fit the encoded-row budget but exceed the
	// unchanged physical page budget. Index amplification must fail closed.
	columns := []string{"a", "b", "c", "d"}
	n := 0
	for a := range columns {
		for b := range columns {
			for c := range columns {
				for d := range columns {
					if a == b || a == c || a == d || b == c || b == d || c == d {
						continue
					}
					ddl := fmt.Sprintf("CREATE INDEX wide%d ON documents(%s,%s,%s,%s)", n, columns[a], columns[b], columns[c], columns[d])
					if _, err := db.Exec(ddl); err != nil {
						t.Fatal(err)
					}
					n++
				}
			}
		}
	}
	for i := 0; i < 8; i++ {
		value := strings.Repeat("synthetic", 85)
		if _, err := db.Exec("INSERT INTO documents VALUES(?,?,?,?,?)", i, value, value, value, value); err != nil {
			t.Fatal(err)
		}
	}
	source := newTestSnapshot(t, path, Config{Tables: map[string][]string{"documents": {"id", "a", "b", "c", "d"}}}, SnapshotConfig{
		Filters: map[string]string{"documents": "1"}, MaxBytes: 32768,
	})
	r, _, err := source.Query(context.Background(), "alice", "SELECT id FROM documents")
	if !errors.Is(err, ErrSnapshotLimit) || len(r.Rows) != 0 {
		t.Fatalf("index storage did not enforce snapshot limit: %#v, %v", r, err)
	}
	cleanSnapshot(t, source)
}
