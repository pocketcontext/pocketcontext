package searchread

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func fixture(t *testing.T, limits Limits) (*Engine, *sql.DB, Index, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	idx := Index{Table: "documents_search", Collection: "documents", Columns: []string{"title", "body"}, SnippetColumn: "body", Weights: []float64{3, 1}}
	ddl, err := CanonicalDDL(idx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE documents(id TEXT PRIMARY KEY,title TEXT,body TEXT);` + ddl + `; INSERT INTO documents VALUES('a','Alpha','alpha one'),('b','Beta','alpha two'),('c','Gamma','other'); INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents;`); err != nil {
		t.Fatal(err)
	}
	e, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, db, idx, path
}
func TestSearch(t *testing.T) {
	e, _, _, _ := fixture(t, Limits{})
	r, err := e.Search(context.Background(), "docs", "alpha", 1)
	if err != nil || len(r.Hits) != 1 || r.Hits[0].ID != "a" || !r.Truncated {
		t.Fatalf("%#v %v", r, err)
	}
	r, err = e.Search(context.Background(), "docs", "alpha two", 0)
	if err != nil || len(r.Hits) != 1 || r.Hits[0].ID != "b" || r.Hits[0].Excerpt != "alpha two" {
		t.Fatalf("%#v %v", r, err)
	}
	for _, q := range []string{`alpha OR other`, `title:alpha`, `" OR "`, `*`, `NEAR(alpha)`, `'; DROP TABLE documents;--`} {
		if _, err = e.Search(context.Background(), "docs", q, 0); err != nil {
			t.Fatalf("literal %q: %v", q, err)
		}
	}
	for n := 0; n < 3; n++ {
		e.db.SetMaxIdleConns(0)
		if _, err = e.Search(context.Background(), "docs", "alpha", 0); err != nil {
			t.Fatalf("cold connection: %v", err)
		}
	}
}
func TestInvalidRequests(t *testing.T) {
	e, _, _, _ := fixture(t, Limits{})
	for _, q := range []string{"", "  ", strings.Repeat("x", MaxQueryBytes+1), strings.Repeat("x ", MaxTerms+1), "x\x00", string([]byte{255})} {
		if _, err := e.Search(context.Background(), "docs", q, 0); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("%q: %v", q, err)
		}
	}
	for _, limit := range []int{-1, 101} {
		if _, err := e.Search(context.Background(), "docs", "x", limit); !errors.Is(err, ErrInvalidQuery) {
			t.Fatal(err)
		}
	}
	if _, err := e.Search(context.Background(), "unknown", "x", 0); !errors.Is(err, ErrInvalidQuery) {
		t.Fatal(err)
	}
}
func TestSchemaAndIDsFailClosed(t *testing.T) {
	for _, mutation := range []string{`INSERT INTO documents_search(record_id,title,body) VALUES('a','duplicate','alpha')`, `INSERT INTO documents_search(record_id,title,body) VALUES('','empty','alpha')`, `INSERT INTO documents_search(record_id,title,body) VALUES('orphan','missing','alpha')`, `DROP TABLE documents_search; CREATE TABLE documents_search(record_id,title,body)`, `ALTER TABLE documents_search_content ADD COLUMN private TEXT`} {
		t.Run(mutation, func(t *testing.T) {
			e, db, idx, path := fixture(t, Limits{})
			if _, err := db.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			if _, err := e.Search(context.Background(), "docs", "alpha", 0); !errors.Is(err, ErrIndexInvalid) {
				t.Fatalf("runtime %v", err)
			}
			if e2, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{}); err == nil {
				e2.Close()
				t.Fatal("startup accepted invalid index")
			}
		})
	}
}
func TestIndexLifecycle(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{})
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE documents SET body='replacement' WHERE id='a'; DELETE FROM documents_search WHERE record_id='a'; INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents WHERE id='a'`); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	r, err := e.Search(context.Background(), "docs", "alpha", 0)
	if err != nil || len(r.Hits) != 2 {
		t.Fatalf("rollback %#v %v", r, err)
	}
	tx, _ = db.Begin()
	if _, err = tx.Exec(`DELETE FROM documents_search; INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents`); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	if _, err = e.Search(context.Background(), "docs", "alpha", 0); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`BEGIN; DELETE FROM documents_search WHERE record_id='a'; DELETE FROM documents WHERE id='a'; COMMIT`); err != nil {
		t.Fatal(err)
	}
	r, err = e.Search(context.Background(), "docs", "alpha", 0)
	if err != nil || len(r.Hits) != 1 || r.Hits[0].ID != "b" {
		t.Fatalf("delete %#v %v", r, err)
	}
}
func TestCancellation(t *testing.T) {
	e, _, _, _ := fixture(t, Limits{Timeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Search(ctx, "docs", "alpha", 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := e.Search(context.Background(), "docs", "alpha", 0); err != nil {
		t.Fatal(err)
	}
}
func TestCanonicalConfig(t *testing.T) {
	base := Index{Table: "search_docs", Collection: "docs", Columns: []string{"body"}, SnippetColumn: "body"}
	for _, mutate := range []func(*Index){func(i *Index) { i.Table = "bad\";DROP" }, func(i *Index) { i.Columns = []string{"record_id"} }, func(i *Index) { i.Columns = []string{"body", "BODY"} }, func(i *Index) { i.SnippetColumn = "secret" }, func(i *Index) { i.Weights = []float64{-1} }, func(i *Index) { i.Table = i.Collection }} {
		i := base
		mutate(&i)
		if _, err := CanonicalDDL(i); err == nil {
			t.Fatalf("accepted %#v", i)
		}
	}
}

func TestReadOnlyPoolAndBoundedResponse(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{MaxBytes: 128})
	for _, q := range []string{`DELETE FROM documents_search`, `SELECT load_extension('bad')`, `SELECT * FROM pragma_table_info('documents')`, `PRAGMA query_only=OFF`, `SELECT title FROM documents`, `ATTACH ':memory:' AS other`} {
		if rows, err := e.db.Query(q); err == nil {
			rows.Close()
			t.Fatalf("permitted unsafe pool statement %s", q)
		}
	}
	if _, err := db.Exec(`UPDATE documents SET body=? WHERE id='a';`, strings.Repeat("alpha ", 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM documents_search WHERE record_id='a'; INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents WHERE id='a'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Search(context.Background(), "docs", "alpha", 0); !errors.Is(err, ErrResponseLimit) {
		t.Fatalf("byte limit: %v", err)
	}
}

func TestRejectOtherFTSShapes(t *testing.T) {
	for _, suffix := range []string{`, content=''`, `, content='documents'`, `, tokenize='porter'`, `, prefix='2 3'`, `, detail=none`} {
		t.Run(suffix, func(t *testing.T) {
			e, db, idx, path := fixture(t, Limits{})
			e.Close()
			ddl := `CREATE VIRTUAL TABLE documents_search USING fts5(record_id UNINDEXED,title,body` + suffix + `)`
			if _, err := db.Exec(`DROP TABLE documents_search;` + ddl); err != nil {
				t.Fatal(err)
			}
			if e, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{}); err == nil {
				e.Close()
				t.Fatal("accepted noncanonical DDL")
			}
		})
	}
}

func TestRunningCancellationAndPoolRecovery(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{})
	if _, err := db.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10000) INSERT INTO documents SELECT 'bulk'||n,'alpha','alpha body' FROM seq; INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents WHERE id LIKE 'bulk%'`); err != nil {
		t.Fatal(err)
	}
	e.limits.Timeout = time.Millisecond
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Go(func() {
			if _, err := e.Search(context.Background(), "docs", "alpha", 0); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("running deadline: %v", err)
			}
		})
	}
	wg.Wait()
	e.limits.Timeout = 2 * time.Second
	if _, err := e.Search(context.Background(), "docs", "alpha", 1); err != nil {
		t.Fatalf("recovery: %v", err)
	}
}

func TestSchemaCommitDuringSearch(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{})
	e.db.SetMaxOpenConns(1)
	conn, err := e.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	err = conn.Raw(func(raw any) error {
		return raw.(readConn).c.RegisterFunc("pocketcontext_utf8_valid", func(value string) (bool, error) {
			var mutationErr error
			once.Do(func() {
				_, mutationErr = db.Exec(`DROP TABLE documents_search; CREATE TABLE documents_search(record_id,title,body); INSERT INTO documents_search VALUES('private','private','private')`)
			})
			return utf8.ValidString(value), mutationErr
		}, true)
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.Search(context.Background(), "docs", "alpha", 0)
	if err != nil || len(r.Hits) != 2 {
		t.Fatalf("in-flight snapshot %#v %v", r, err)
	}
	if _, err = e.Search(context.Background(), "docs", "alpha", 0); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("next request accepted schema change: %v", err)
	}
}

func TestNullExcerptAndInvalidIDEncoding(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{})
	if _, err := db.Exec(`INSERT INTO documents VALUES('nullbody','nullable',NULL); INSERT INTO documents_search(record_id,title,body) VALUES('nullbody','nullable',NULL)`); err != nil {
		t.Fatal(err)
	}
	r, err := e.Search(context.Background(), "docs", "nullable", 0)
	if err != nil || len(r.Hits) != 1 || r.Hits[0].Excerpt != "" {
		t.Fatalf("null excerpt %#v %v", r, err)
	}
	invalid := string([]byte{255})
	if _, err := db.Exec(`INSERT INTO documents VALUES(?, 'alpha', 'alpha')`, invalid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO documents_search(record_id,title,body) VALUES(?, 'alpha', 'alpha')`, invalid); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Search(context.Background(), "docs", "alpha", 0); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("invalid UTF8 ID: %v", err)
	}
}

func TestSchemaValidationBusyClassification(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{Timeout: time.Second})
	// Rollback-journal EXCLUSIVE prevents the source-schema validation read.
	// BEGIN itself is deferred, so the failure comes from schema inspection.
	e.db.SetMaxIdleConns(0)
	if _, err := db.Exec(`PRAGMA journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	e.db.SetMaxIdleConns(4)
	if _, err := e.Search(context.Background(), "docs", "alpha", 0); err != nil {
		t.Fatal(err)
	}
	writer, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err = writer.ExecContext(context.Background(), `BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	defer writer.ExecContext(context.Background(), `ROLLBACK`)
	_, err = e.Search(context.Background(), "docs", "alpha", 0)
	if !errors.Is(err, ErrBusy) || !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("schema busy identity not preserved: %v", err)
	}
	if _, err = writer.ExecContext(context.Background(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Search(context.Background(), "docs", "alpha", 0); err != nil {
		t.Fatalf("after writer released: %v", err)
	}
}
