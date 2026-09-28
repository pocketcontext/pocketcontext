//go:build fts5_probe && sqlite_fts5

package sqlread

import (
	"database/sql/driver"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// TestFTS5AuthorizerProbe records why simply allowing FTS5 shadow tables in
// Engine's authorizer is unsafe. This deliberately permissive prototype is
// excluded from ordinary builds and must not become a production policy.
func TestFTS5AuthorizerProbe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts.db")
	d := &sqlite3.SQLiteDriver{}
	writer, err := d.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.(*sqlite3.SQLiteConn).Exec(`
 CREATE VIRTUAL TABLE search USING fts5(record_id UNINDEXED, title, body);
 INSERT INTO search VALUES ('a', 'alpha', 'beta alpha'), ('b', 'gamma', 'delta');`, nil)
	writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	open := func(t *testing.T) *sqlite3.SQLiteConn {
		t.Helper()
		raw, err := d.Open("file:" + path + "?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		c := raw.(*sqlite3.SQLiteConn)
		t.Cleanup(func() { c.Close() })
		if _, err = c.Exec("PRAGMA query_only=1", nil); err != nil {
			t.Fatal(err)
		}
		return c
	}
	const query = `SELECT record_id, bm25(search), snippet(search,2,'[',']','...',8)
 FROM search WHERE search MATCH 'alpha' ORDER BY bm25(search)`
	type event struct {
		op                      int
		table, column, database string
	}
	t.Run("cold_initialization", func(t *testing.T) {
		c := open(t)
		events := []event{}
		c.RegisterAuthorizer(func(op int, a, b, db string) int {
			events = append(events, event{op, a, b, db})
			return sqlite3.SQLITE_OK
		})
		rows, err := ftsProbeRows(c, query)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0][0] != "a" || rows[0][2] != "beta [alpha]" {
			t.Fatalf("unexpected search result: %v", rows)
		}
		seenConfig, seenPragma := false, false
		for _, e := range events {
			t.Logf("authorizer: %+v", e)
			seenConfig = seenConfig || (e.op == sqlite3.SQLITE_READ && e.table == "search_config")
			seenPragma = seenPragma || (e.op == sqlite3.SQLITE_PRAGMA && e.table == "data_version")
		}
		if !seenConfig || !seenPragma {
			t.Fatalf("expected internal config reads and data_version: config=%t pragma=%t", seenConfig, seenPragma)
		}
	})
	t.Run("deny_runtime_pragma", func(t *testing.T) {
		c := open(t)
		if _, err := ftsProbeRows(c, "SELECT rowid FROM search LIMIT 0"); err != nil {
			t.Fatal(err)
		}
		denied := false
		c.RegisterAuthorizer(func(op int, a, b, db string) int {
			if op == sqlite3.SQLITE_PRAGMA && a == "data_version" {
				denied = true
				return sqlite3.SQLITE_DENY
			}
			return sqlite3.SQLITE_OK
		})
		if _, err := ftsProbeRows(c, query); err == nil || !denied {
			t.Fatalf("expected runtime data_version denial: denied=%t error=%v", denied, err)
		}
	})
	for _, shadows := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow_shadows_%t", shadows), func(t *testing.T) {
			c := open(t)
			// Trusted per-connection initialization, before installing a policy.
			if _, err := ftsProbeRows(c, "SELECT rowid FROM search LIMIT 0"); err != nil {
				t.Fatal(err)
			}
			events := []event{}
			c.RegisterAuthorizer(func(op int, a, b, db string) int {
				events = append(events, event{op, a, b, db})
				if op == sqlite3.SQLITE_PRAGMA && a == "data_version" && b == "" && db == "main" {
					return sqlite3.SQLITE_OK
				}
				if op == sqlite3.SQLITE_SELECT {
					return sqlite3.SQLITE_OK
				}
				if op == sqlite3.SQLITE_FUNCTION && (b == "match" || b == "bm25" || b == "snippet") {
					return sqlite3.SQLITE_OK
				}
				if op == sqlite3.SQLITE_READ && db == "main" {
					if a == "search" {
						return sqlite3.SQLITE_OK
					}
					if shadows {
						switch a {
						case "search_data", "search_idx", "search_content", "search_docsize", "search_config":
							return sqlite3.SQLITE_OK
						}
					}
				}
				return sqlite3.SQLITE_DENY
			})
			rows, err := ftsProbeRows(c, query)
			if !shadows {
				if err == nil {
					t.Fatal("expected search failure with denied shadow reads")
				}
				t.Logf("search denied: %v", err)
				if _, err = ftsProbeRows(c, "SELECT c2 FROM search_content WHERE id=1"); err == nil {
					t.Fatal("direct shadow read succeeded")
				}
				return
			}
			if err != nil || len(rows) != 1 {
				t.Fatalf("search: rows=%v error=%v", rows, err)
			}
			searchEvents := append([]event(nil), events...)
			events = nil
			rows, err = ftsProbeRows(c, "SELECT c2 FROM search_content WHERE id=1")
			if err != nil || len(rows) != 1 || rows[0][0] != "beta alpha" {
				t.Fatalf("expected direct shadow exposure: rows=%v error=%v", rows, err)
			}
			same := false
			for _, direct := range events {
				for _, internal := range searchEvents {
					if direct == internal && direct.op == sqlite3.SQLITE_READ && direct.table == "search_content" {
						same = true
					}
				}
			}
			if !same {
				t.Fatal("expected indistinguishable internal and direct shadow-read callbacks")
			}
			t.Log("search succeeds, but the same policy permits direct shadow reads")
		})
	}
}

func ftsProbeRows(c *sqlite3.SQLiteConn, query string) ([][]driver.Value, error) {
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if !stmt.(*sqlite3.SQLiteStmt).Readonly() {
		return nil, fmt.Errorf("probe statement is not read-only")
	}
	rows, err := stmt.Query(nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result [][]driver.Value
	for {
		values := make([]driver.Value, len(rows.Columns()))
		err = rows.Next(values)
		if err == io.EOF {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		result = append(result, values)
	}
}
