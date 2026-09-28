package searchread

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This corpus is synthetic. It exercises published pages, unpublished drafts,
// cross-topic references and unequal body lengths; it contains no wiki data.
var benchmarkTopics = []string{
	"publication rollback", "invoice receipt", "trace latency", "channel membership",
	"workspace oauth", "task dependency", "database restore", "json array",
	"employee transfer", "investor commitment",
}

const benchmarkPageCount = 3000

var benchmarkSink any

type benchmarkFixture struct {
	path       string
	db         *sql.DB
	engine     *Engine
	index      Index
	baseBytes  int64
	indexBytes int64
	buildTime  time.Duration
}

func newBenchmarkFixture(tb testing.TB, indexed bool) *benchmarkFixture {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "synthetic.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	tb.Cleanup(func() { db.Close() })
	if _, err = db.Exec(`CREATE TABLE documents (
 id TEXT PRIMARY KEY, title TEXT NOT NULL, summary TEXT NOT NULL,
 body TEXT NOT NULL, published INTEGER NOT NULL)`); err != nil {
		tb.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	insert, err := tx.Prepare(`INSERT INTO documents VALUES(?,?,?,?,?)`)
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < benchmarkPageCount; i++ {
		topic := i % len(benchmarkTopics)
		title := fmt.Sprintf("%s: operational guide %04d", benchmarkTopics[topic], i)
		summary := fmt.Sprintf("How to validate %s with repeatable checks and ownership.", benchmarkTopics[topic])
		var body strings.Builder
		for paragraph := 0; paragraph < 5+(i/len(benchmarkTopics))%11; paragraph++ {
			fmt.Fprintf(&body, "Section %d describes %s. The team records evidence, checks inputs, runs a bounded operation, and verifies the resulting state. Example %d uses synthetic records and explains recovery and expected outcomes. ", paragraph, benchmarkTopics[topic], i)
		}
		// Cross-topic mentions create distractors for body matches, while titles and
		// summaries identify the intended topic. Vary distractor frequency and size.
		other := (topic + 1 + (i/len(benchmarkTopics))%9) % len(benchmarkTopics)
		for mention := 0; mention < (i/len(benchmarkTopics))%4; mention++ {
			fmt.Fprintf(&body, " Related reading: %s.", benchmarkTopics[other])
		}
		published := 1
		if (i/len(benchmarkTopics))%10 == 0 {
			published = 0
			title = "DRAFT " + title
		}
		if _, err = insert.Exec(fmt.Sprintf("page%010d", i), title, summary, body.String(), published); err != nil {
			tb.Fatal(err)
		}
	}
	insert.Close()
	if err = tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	f := &benchmarkFixture{path: path, db: db, index: Index{Table: "page_search", Collection: "documents", Columns: []string{"title", "summary", "body"}, SnippetColumn: "body", Weights: []float64{8, 3, 1}}}
	f.baseBytes = benchmarkDatabaseBytes(tb, db)
	if indexed {
		ddl, err := CanonicalDDL(f.index)
		if err != nil {
			tb.Fatal(err)
		}
		started := time.Now()
		tx, err = db.Begin()
		if err != nil {
			tb.Fatal(err)
		}
		if _, err = tx.Exec(ddl); err != nil {
			tb.Fatal(err)
		}
		if _, err = tx.Exec(`INSERT INTO page_search(rowid,record_id,title,summary,body) SELECT rowid,id,title,summary,body FROM documents WHERE published=1`); err != nil {
			tb.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			tb.Fatal(err)
		}
		f.buildTime = time.Since(started)
		f.indexBytes = benchmarkDatabaseBytes(tb, db) - f.baseBytes
		f.engine, err = New(path, Config{Indexes: map[string]Index{"pages": f.index}}, Limits{Timeout: 5 * time.Second, MaxRows: 100, MaxBytes: 1 << 20})
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { f.engine.Close() })
	}
	return f
}

func benchmarkDatabaseBytes(tb testing.TB, db *sql.DB) int64 {
	tb.Helper()
	var pages, size int64
	if err := db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		tb.Fatal(err)
	}
	if err := db.QueryRow("PRAGMA page_size").Scan(&size); err != nil {
		tb.Fatal(err)
	}
	return pages * size
}

// The baseline performs a single batched scan with simple field-weighted
// ranking. Its substring matching differs from FTS token matching; the corpus
// queries deliberately use whole ASCII words valid under both semantics.
func benchmarkLexical(ctx context.Context, db *sql.DB, query string) ([]string, error) {
	terms := strings.Fields(strings.ToLower(query))
	var where, score []string
	var args []any
	for _, term := range terms {
		score = append(score, `8*(instr(lower(title),?)>0)+3*(instr(lower(summary),?)>0)+(instr(lower(body),?)>0)`)
		args = append(args, term, term, term)
		where = append(where, `(instr(lower(title),?)>0 OR instr(lower(summary),?)>0 OR instr(lower(body),?)>0)`)
	}
	for _, term := range terms {
		args = append(args, term, term, term)
	}
	rows, err := db.QueryContext(ctx, `SELECT id, `+strings.Join(score, "+")+` AS score, substr(body,1,240)
 FROM documents WHERE published=1 AND `+strings.Join(where, " AND ")+` ORDER BY score DESC,id LIMIT 20`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id, excerpt string
		var score float64
		if err = rows.Scan(&id, &score, &excerpt); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func TestSyntheticSearchRelevance(t *testing.T) {
	f := newBenchmarkFixture(t, true)
	reader, err := sql.Open("sqlite3", "file:"+f.path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var published int
	if err = f.db.QueryRow("SELECT count(*) FROM documents WHERE published=1").Scan(&published); err != nil {
		t.Fatal(err)
	}
	for _, query := range benchmarkTopics {
		t.Run(strings.ReplaceAll(query, " ", "_"), func(t *testing.T) {
			lexical, err := benchmarkLexical(context.Background(), reader, query)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.engine.Search(context.Background(), "pages", query, 20)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(result.Hits))
			for i, hit := range result.Hits {
				ids[i] = hit.ID
			}
			for method, hits := range map[string][]string{"lexical": lexical, "fts": ids} {
				if len(hits) != 20 {
					t.Fatalf("%s returned %d hits", method, len(hits))
				}
				for _, id := range hits {
					var title string
					var public int
					if err = f.db.QueryRow("SELECT title,published FROM documents WHERE id=?", id).Scan(&title, &public); err != nil {
						t.Fatal(err)
					}
					if public != 1 || !strings.HasPrefix(title, query+":") {
						t.Errorf("%s irrelevant/unpublished hit %s: %q", method, id, title)
					}
				}
			}
		})
	}
	t.Logf("synthetic pages=%d published=%d source_bytes=%d extra_index_bytes=%d index_build=%s", benchmarkPageCount, published, f.baseBytes, f.indexBytes, f.buildTime)
}

// benchmarkRawFTS is diagnostic only: it deliberately omits engine validation,
// authorization and response bounds, and is not a proposed replacement API.
func benchmarkRawFTS(ctx context.Context, db *sql.DB, query string) ([]string, error) {
	terms := strings.Fields(query)
	for i, term := range terms {
		terms[i] = `"` + strings.ReplaceAll(term, `"`, `""`) + `"`
	}
	rows, err := db.QueryContext(ctx, `SELECT record_id,bm25(page_search,0,8,3,1),snippet(page_search,3,'','',' … ',32)
 FROM page_search WHERE page_search MATCH ? ORDER BY 2,record_id LIMIT 21`, strings.Join(terms, " AND "))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id, excerpt string
		var score float64
		if err = rows.Scan(&id, &score, &excerpt); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func BenchmarkSyntheticSearch(b *testing.B) {
	f := newBenchmarkFixture(b, true)
	reader, err := sql.Open("sqlite3", "file:"+f.path+"?mode=ro")
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(4)
	scenarios := []struct {
		name    string
		queries []string
	}{
		{"Topic", benchmarkTopics}, {"Broad", []string{"evidence"}}, {"Missing", []string{"quasar hyperdrive"}},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.Run("BatchedLexical", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					hits, err := benchmarkLexical(context.Background(), reader, scenario.queries[i%len(scenario.queries)])
					if err != nil {
						b.Fatal(err)
					}
					benchmarkSink = hits
				}
			})
			b.Run("FTS", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					hits, err := f.engine.Search(context.Background(), "pages", scenario.queries[i%len(scenario.queries)], 20)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkSink = hits
				}
			})
			b.Run("RawFTSDiagnostic", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					hits, err := benchmarkRawFTS(context.Background(), reader, scenario.queries[i%len(scenario.queries)])
					if err != nil {
						b.Fatal(err)
					}
					benchmarkSink = hits
				}
			})
		})
	}
}

// Every operation replaces 20 already-published bodies in one transaction.
// Source-only and source-plus-index use separate otherwise-identical fixtures.
func BenchmarkSyntheticPublication(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		name := "SourceOnly"
		if indexed {
			name = "SourceAndFTS"
		}
		b.Run(name, func(b *testing.B) {
			f := newBenchmarkFixture(b, indexed)
			bodies := make([]string, 20)
			for j := range bodies {
				if err := f.db.QueryRow("SELECT body FROM documents WHERE id=?", fmt.Sprintf("page%010d", 10+j)).Scan(&bodies[j]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := f.db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				for j, body := range bodies {
					id := fmt.Sprintf("page%010d", 10+j)
					if _, err = tx.Exec("UPDATE documents SET body=? WHERE id=?", fmt.Sprintf("%s Revision %d.", body, i), id); err != nil {
						tx.Rollback()
						b.Fatal(err)
					}
					if indexed {
						if _, err = tx.Exec("DELETE FROM page_search WHERE rowid=(SELECT rowid FROM documents WHERE id=?)", id); err != nil {
							tx.Rollback()
							b.Fatal(err)
						}
						if _, err = tx.Exec(`INSERT INTO page_search(rowid,record_id,title,summary,body) SELECT rowid,id,title,summary,body FROM documents WHERE id=?`, id); err != nil {
							tx.Rollback()
							b.Fatal(err)
						}
					}
				}
				if err = tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if info, err := os.Stat(f.path); err == nil {
				b.ReportMetric(float64(info.Size()), "database-bytes")
			}
		})
	}
}
