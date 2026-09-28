package searchread

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Publication and validation must share a coherent committed corpus, including
// when every reader needs a new connection and a writer rebuilds the index.
func TestSearchConcurrentPublicationSnapshot(t *testing.T) {
	e, db, _, _ := fixture(t, Limits{})
	if _, err := db.Exec(`BEGIN;
 UPDATE documents SET body='coherent generation 0';
 DELETE FROM documents_search;
 INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents;
 COMMIT;`); err != nil {
		t.Fatal(err)
	}
	e.db.SetMaxIdleConns(0)
	start := make(chan struct{})
	failures := make(chan error, 5)
	var workers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 30; iteration++ {
				result, err := e.Search(context.Background(), "docs", "coherent", 10)
				if err != nil {
					failures <- err
					return
				}
				if len(result.Hits) != 3 {
					failures <- fmt.Errorf("partial publication: %+v", result)
					return
				}
				for _, hit := range result.Hits {
					if hit.Excerpt != result.Hits[0].Excerpt {
						failures <- fmt.Errorf("mixed publication: %+v", result)
						return
					}
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for generation := 1; generation <= 20; generation++ {
			tx, err := db.Begin()
			if err != nil {
				failures <- err
				return
			}
			_, err = tx.Exec("UPDATE documents SET body=?", fmt.Sprintf("coherent generation %d", generation))
			if err == nil {
				_, err = tx.Exec("DELETE FROM documents_search")
			}
			if err == nil {
				_, err = tx.Exec("INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents")
			}
			if err != nil {
				tx.Rollback()
				failures <- err
				return
			}
			if err = tx.Commit(); err != nil {
				failures <- err
				return
			}
		}
	}()
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestSearchPopulatedBackupRestore(t *testing.T) {
	e, db, idx, _ := fixture(t, Limits{})
	before, err := e.Search(context.Background(), "docs", "alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	restore := filepath.Join(t.TempDir(), "restored.db")
	// VACUUM INTO belongs to the trusted synthetic writer, never the search pool.
	if _, err = db.Exec("VACUUM INTO ?", restore); err != nil {
		t.Fatal(err)
	}
	restored, err := New(restore, Config{Indexes: map[string]Index{"docs": idx}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	after, err := restored.Search(context.Background(), "docs", "alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Hits) != len(after.Hits) {
		t.Fatalf("restore count: before=%+v after=%+v", before, after)
	}
	for i := range before.Hits {
		if before.Hits[i] != after.Hits[i] {
			t.Fatalf("restore content/ranking: before=%+v after=%+v", before, after)
		}
	}
}

func TestSearchOperatorsRemainLiteral(t *testing.T) {
	e, _, _, _ := fixture(t, Limits{})
	for _, query := range []string{`alpha OR other`, `title:alpha`, `alpha*`, `NEAR(alpha)`, `" OR "`, `'; DROP TABLE documents;--`} {
		result, err := e.Search(context.Background(), "docs", query, 10)
		if err != nil {
			t.Fatalf("literal %q: %v", query, err)
		}
		// The tokenizer treats punctuation as separators; "alpha*" remains the
		// literal token alpha, while the other expressions require absent terms.
		expected := 0
		if query == "alpha*" {
			expected = 2
		}
		if len(result.Hits) != expected {
			t.Errorf("query %q executed as operators: %+v", query, result)
		}
	}
	result, err := e.Search(context.Background(), "docs", "alpha", 10)
	if err != nil || len(result.Hits) != 2 {
		t.Fatalf("query text changed data: %+v %v", result, err)
	}
}

// Invalid IDs outside the requested match must reject the entire corpus. The
// direct canonical _content.c0 validation must preserve virtual-table checks.
func TestSearchRejectsMalformedIDs(t *testing.T) {
	cases := []struct {
		name string
		id   any
	}{
		{"null", nil}, {"integer", int64(123)}, {"blob", []byte("blob-id")},
		{"too_many_bytes", strings.Repeat("é", 65)}, {"too_many_ascii_bytes", strings.Repeat("a", 129)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, db, idx, path := fixture(t, Limits{})
			if _, err := db.Exec("INSERT INTO documents(id,title,body) VALUES(?, 'unrelated', 'unrelated')", tc.id); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO documents_search(record_id,title,body) VALUES(?, 'unrelated', 'unrelated')", tc.id); err != nil {
				t.Fatal(err)
			}
			if _, err := e.Search(context.Background(), "docs", "alpha", 10); !errors.Is(err, ErrIndexInvalid) {
				t.Fatalf("runtime malformed ID: %v", err)
			}
			if reopened, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{}); !errors.Is(err, ErrIndexInvalid) {
				if reopened != nil {
					reopened.Close()
				}
				t.Fatalf("startup malformed ID: %v", err)
			}
		})
	}
}
