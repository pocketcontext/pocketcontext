package sqlread

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// BenchmarkSnapshotWikiJoins includes publication validation, page navigation,
// page detail, evidence and both link directions over synthetic immutable
// history. Compare with/without persistent snapshot indexes on the same data;
// SQLite remains free to build automatic query indexes in either case.
func BenchmarkSnapshotWikiJoins(b *testing.B) {
	path, cfg, sc := snapshotWikiJoinFixture(b)
	for _, indexed := range []bool{false, true} {
		label := "without_indexes"
		if indexed {
			label = "with_indexes"
		}
		b.Run(label, func(b *testing.B) {
			for _, mode := range []string{"cold_lookup", "cold", "warm"} {
				b.Run(mode, func(b *testing.B) {
					var source *SnapshotSource
					open := func() {
						if source != nil {
							source.Close()
						}
						var err error
						source, err = NewSnapshotSource(path, cfg, sc)
						if err != nil {
							b.Fatal(err)
						}
						if !indexed {
							source.indexes = nil // Benchmark the prior snapshot schema.
						}
					}
					open()
					defer func() { source.Close() }()
					queries := []struct {
						sql  string
						rows int
					}{
						{`SELECT count(m.key), count(p.id), count(r.id) FROM publications pub
LEFT JOIN json_each(pub.manifest) m ON 1=1 LEFT JOIN pages p ON p.id=m.key
LEFT JOIN revisions r ON r.id=m.value AND r.page=p.id WHERE pub.id='pub'`, 1},
						{`SELECT p.id,p.slug,r.title FROM publications pub JOIN json_each(pub.manifest) m
JOIN pages p ON p.id=m.key JOIN revisions r ON r.id=m.value AND r.page=p.id
WHERE pub.id='pub' ORDER BY p.slug`, 350},
						{`SELECT p.id,r.title,r.body FROM publications pub JOIN json_each(pub.manifest) m
JOIN pages p ON p.id=m.key JOIN revisions r ON r.id=m.value AND r.page=p.id
WHERE pub.id='pub' AND p.slug='page-001'`, 1},
						{`SELECT c.id,t.body,s.title FROM citations c JOIN passages t ON t.id=c.passage
JOIN renditions e ON e.id=t.rendition JOIN sources s ON s.id=e.source
WHERE c.revision='r000010' ORDER BY c.id`, 2},
						{`SELECT p.id,r.title FROM publications pub JOIN json_each(pub.manifest) m
JOIN pages p ON p.id=m.key JOIN revisions r ON r.id=m.value AND r.page=p.id
WHERE pub.id='pub' AND r.id IN (SELECT l.revision FROM links l WHERE l.target='p001') ORDER BY p.slug`, 2},
						{`SELECT p.id,r.title FROM publications pub JOIN json_each(pub.manifest) m
JOIN pages p ON p.id=m.key JOIN revisions r ON r.id=m.value AND r.page=p.id
WHERE pub.id='pub' AND p.id IN (SELECT l.target FROM links l WHERE l.revision='r000010') ORDER BY p.slug`, 2},
					}
					if mode == "cold_lookup" {
						// A trivial result isolates construction cost from joins.
						queries = queries[:1]
						queries[0].sql = "SELECT id FROM pages WHERE id='p001'"
					}
					read := func() {
						for _, query := range queries {
							r, _, err := source.Query(context.Background(), "alice", query.sql)
							if err != nil || r.Truncated || len(r.Rows) != query.rows {
								b.Fatalf("query returned %d rows (want %d), truncated=%v: %v", len(r.Rows), query.rows, r.Truncated, err)
							}
						}
					}
					if mode == "warm" {
						read()
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if mode != "warm" {
							b.StopTimer()
							open()
							b.StartTimer()
						}
						read()
					}
					b.StopTimer()
					entry := source.cache["alice"]
					if entry == nil {
						b.Fatal("benchmark snapshot was not retained")
					}
					info, err := os.Stat(filepath.Join(entry.dir, "snapshot.db"))
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(info.Size()), "snapshot-bytes")
					b.ReportMetric(float64(len(source.indexes)), "indexes")
				})
			}
		})
	}
}

func snapshotWikiJoinFixture(b *testing.B) (string, Config, SnapshotConfig) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "source.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE members(user TEXT, space TEXT);
INSERT INTO members VALUES('alice','company');
CREATE TABLE pages(id TEXT PRIMARY KEY, space TEXT, slug TEXT);
CREATE UNIQUE INDEX pages_slug ON pages(space,slug);
CREATE TABLE revisions(id TEXT PRIMARY KEY, space TEXT, page TEXT, title TEXT, body TEXT);
CREATE INDEX revisions_page ON revisions(page);
CREATE TABLE passages(id TEXT PRIMARY KEY, space TEXT, rendition TEXT, ordinal INTEGER, body TEXT);
CREATE UNIQUE INDEX passages_rendition ON passages(rendition,ordinal);
CREATE TABLE citations(id TEXT PRIMARY KEY, space TEXT, revision TEXT, passage TEXT, marker TEXT);
CREATE UNIQUE INDEX citations_revision ON citations(revision,marker);
CREATE TABLE sources(id TEXT PRIMARY KEY, space TEXT, title TEXT);
CREATE TABLE renditions(id TEXT PRIMARY KEY, space TEXT, source TEXT);
CREATE INDEX renditions_source ON renditions(source);
CREATE TABLE links(id TEXT PRIMARY KEY, space TEXT, revision TEXT, target TEXT);
CREATE UNIQUE INDEX links_revision ON links(revision,target);
CREATE TABLE publications(id TEXT PRIMARY KEY, space TEXT, sequence INTEGER, manifest TEXT);
CREATE UNIQUE INDEX publications_sequence ON publications(space,sequence);`)
	if err != nil {
		b.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	columns := map[string][]string{
		"pages": {"id", "space", "slug"}, "revisions": {"id", "space", "page", "title", "body"},
		"passages": {"id", "space", "rendition", "ordinal", "body"}, "citations": {"id", "space", "revision", "passage", "marker"},
		"sources": {"id", "space", "title"}, "renditions": {"id", "space", "source"},
		"links": {"id", "space", "revision", "target"}, "publications": {"id", "space", "sequence", "manifest"},
	}
	statements := map[string]*sql.Stmt{}
	for table, cols := range columns {
		stmt, err := tx.Prepare("INSERT INTO " + quote(table) + " VALUES(" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")")
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		statements[table] = stmt
	}
	insert := func(table string, values ...any) {
		if _, err := statements[table].Exec(values...); err != nil {
			b.Fatal(err)
		}
	}
	manifest := map[string]string{}
	body := strings.Repeat("synthetic evidence ", 250)
	for page := 1; page <= 350; page++ {
		pid, sid, eid := fmt.Sprintf("p%03d", page), fmt.Sprintf("s%03d", page), fmt.Sprintf("e%03d", page)
		insert("pages", pid, "company", fmt.Sprintf("page-%03d", page))
		insert("sources", sid, "company", "Synthetic source "+pid)
		insert("renditions", eid, "company", sid)
		for revision := 1; revision <= 10; revision++ {
			n := (page-1)*10 + revision
			rid := fmt.Sprintf("r%06d", n)
			insert("revisions", rid, "company", pid, "Synthetic page "+pid, body[:4096])
			manifest[pid] = rid
			for offset := 1; offset <= 2; offset++ {
				id := fmt.Sprintf("evidence%06d", (n-1)*2+offset)
				insert("passages", id, "company", eid, (revision-1)*2+offset, body[:2048])
				insert("citations", id, "company", rid, id, fmt.Sprint(offset))
				insert("links", id, "company", rid, fmt.Sprintf("p%03d", (page-1+offset)%350+1))
			}
		}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		b.Fatal(err)
	}
	insert("publications", "pub", "company", 1, string(encoded))
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	cfg := Config{Tables: columns, Timeout: 5 * time.Second, MaxRows: 500, MaxBytes: 1 << 20}
	sc := SnapshotConfig{Filters: map[string]string{}, PolicyTables: map[string][]string{"members": {"user", "space"}}, MaxBytes: 64 << 20, TimeoutMS: 30000}
	for table := range columns {
		sc.Filters[table] = fmt.Sprintf("EXISTS (SELECT 1 FROM members WHERE user=:requester AND space=%s.space)", table)
	}
	return path, cfg, sc
}
