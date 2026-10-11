package sqlread

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// BenchmarkSnapshotWikiHistory models a reader loading small results from a
// much larger immutable history. Data is entirely synthetic: 350 pages, 3,500
// revisions and 7,000 evidence passages, with approximately 27 MiB of text.
// Run with -benchtime=3x -count=3 to compare cold and reused snapshots without
// allowing benchmark calibration to conceal the first-build cost.
func BenchmarkSnapshotWikiHistory(b *testing.B) {
	for _, mode := range []string{"cold", "warm", "concurrent_same_user", "write_invalidated"} {
		b.Run(mode, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "source.db")
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			_, err = db.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE members(user TEXT, space TEXT);
INSERT INTO members VALUES ('alice','company');
CREATE TABLE pages(id INTEGER, space TEXT, title TEXT);
CREATE TABLE revisions(id INTEGER, space TEXT, page INTEGER, body TEXT);
CREATE TABLE passages(id INTEGER, space TEXT, body TEXT);
CREATE TABLE unrelated(value INTEGER);
INSERT INTO unrelated VALUES(0);
WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<350)
INSERT INTO pages SELECT x,'company','Synthetic page '||x FROM n;`)
			if err != nil {
				b.Fatal(err)
			}
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			for _, table := range []struct {
				name         string
				count, bytes int
			}{{"revisions", 3500, 4096}, {"passages", 7000, 2048}} {
				query := "INSERT INTO " + table.name + " VALUES(?,'company',?)"
				if table.name == "revisions" {
					query = "INSERT INTO revisions VALUES(?,'company',1,?)"
				}
				stmt, err := tx.Prepare(query)
				if err != nil {
					b.Fatal(err)
				}
				body := strings.Repeat("synthetic evidence ", table.bytes/19+1)[:table.bytes]
				for i := 1; i <= table.count; i++ {
					if _, err := stmt.Exec(i, body); err != nil {
						b.Fatal(err)
					}
				}
				stmt.Close()
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
			cfg := Config{Tables: map[string][]string{"pages": {"id", "space", "title"}, "revisions": {"id", "space", "page", "body"}, "passages": {"id", "space", "body"}}, Timeout: 5 * time.Second, MaxRows: 100, MaxBytes: 1 << 20}
			sc := SnapshotConfig{Filters: map[string]string{}, PolicyTables: map[string][]string{"members": {"user", "space"}}, MaxBytes: 64 << 20, TimeoutMS: 30000}
			for table := range cfg.Tables {
				sc.Filters[table] = fmt.Sprintf("EXISTS (SELECT 1 FROM members WHERE user=:requester AND space=%s.space)", table)
			}
			source, err := NewSnapshotSource(path, cfg, sc)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { source.Close() }()
			query := func() error {
				r, _, err := source.Query(context.Background(), "alice", "SELECT title FROM pages WHERE id=1")
				if err != nil {
					return err
				}
				if len(r.Rows) != 1 || r.Rows[0][0] != "Synthetic page 1" {
					return fmt.Errorf("unexpected result: %#v", r)
				}
				return nil
			}
			if mode != "cold" {
				if err := query(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "cold" {
					b.StopTimer()
					source.Close()
					source, err = NewSnapshotSource(path, cfg, sc)
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				if mode == "write_invalidated" {
					if _, err := db.Exec("UPDATE unrelated SET value=value+1"); err != nil {
						b.Fatal(err)
					}
				}
				if mode == "concurrent_same_user" {
					var wg sync.WaitGroup
					errs := make(chan error, 4)
					for j := 0; j < 4; j++ {
						wg.Add(1)
						go func() { defer wg.Done(); errs <- query() }()
					}
					wg.Wait()
					close(errs)
					for err := range errs {
						if err != nil {
							b.Fatal(err)
						}
					}
				} else if err := query(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
