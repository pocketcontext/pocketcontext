package searchread

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestScopedSearch(t *testing.T) {
	_, db, idx, path := fixture(t, Limits{})
	_, err := db.Exec(`CREATE TABLE publications(id TEXT PRIMARY KEY,manifest TEXT); CREATE TABLE search_state(id TEXT PRIMARY KEY,generation TEXT); INSERT INTO publications VALUES('current','{"page":"b"}'),('history','{"page":"a","other":"b","archived":"c"}'); INSERT INTO search_state VALUES('state','one'); DELETE FROM documents_search WHERE record_id='c';`)
	if err != nil {
		t.Fatal(err)
	}
	idx.Scope = &Scope{Collection: "publications", Field: "manifest"}
	idx.Generation = &Generation{Collection: "search_state", Record: "state", Field: "generation"}
	e, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	req := Request{Index: "docs", Query: "alpha", Scope: "current", Limit: 1}
	result, err := e.SearchRequest(context.Background(), req)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].ID != "b" || result.HasMore || len(result.Generation) != 64 {
		t.Fatalf("membership before limit: %+v %v", result, err)
	}
	req.Scope = "history"
	result, err = e.SearchRequest(context.Background(), req)
	if err != nil || !result.HasMore || result.Hits[0].ID != "a" {
		t.Fatalf("history: %+v %v", result, err)
	}
	req.Offset = 1
	req.ExpectedGeneration = result.Generation
	result, err = e.SearchRequest(context.Background(), req)
	if err != nil || result.HasMore || result.Hits[0].ID != "b" {
		t.Fatalf("page two: %+v %v", result, err)
	}
	if _, err = db.Exec(`UPDATE search_state SET generation='two'`); err != nil {
		t.Fatal(err)
	}
	if _, err = e.SearchRequest(context.Background(), req); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("generation: %v", err)
	}
	req.ExpectedGeneration = ""
	req.Offset = 0
	fresh, err := e.SearchRequest(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.ExpectedGeneration = fresh.Generation
	req.Offset = 1
	if _, err = e.SearchRequest(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Request{{Index: "docs", Query: "alpha"}, {Index: "docs", Query: "alpha", Scope: "history", Offset: 1}, {Index: "docs", Query: "alpha", Scope: "history", Offset: MaxOffset + 1, ExpectedGeneration: "two"}} {
		if _, err = e.SearchRequest(context.Background(), bad); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("request: %v", err)
		}
	}
	req.Offset = 0
	for _, raw := range []string{`[]`, `null`, `{"a":"a","a":"b"}`, `{"a":null}`, `{"a":123}`, `{"":"a"}`, `{"a":"missing"}`, `{"a":"a"} true`} {
		if _, err = db.Exec(`UPDATE publications SET manifest=? WHERE id='history'`, raw); err != nil {
			t.Fatal(err)
		}
		if _, err = e.SearchRequest(context.Background(), req); !errors.Is(err, ErrIndexInvalid) {
			t.Fatalf("scope %s: %v", raw, err)
		}
	}
	if _, err = db.Exec(`UPDATE publications SET manifest='{}' WHERE id='history'`); err != nil {
		t.Fatal(err)
	}
	req.ExpectedGeneration = ""
	result, err = e.SearchRequest(context.Background(), req)
	if err != nil || len(result.Hits) != 0 {
		t.Fatalf("empty: %+v %v", result, err)
	}
}
func TestScopeBounds(t *testing.T) {
	for _, raw := range []string{`{"a":"\ud800"}`, `{"a":"\udfff"}`, `{"a":"\ud800\u0041"}`} {
		if _, err := scopeMembers(raw); !errors.Is(err, ErrIndexInvalid) {
			t.Fatalf("invalid surrogate accepted: %s", raw)
		}
	}
	for _, raw := range []string{`{"a":"\ud83d\ude00"}`, `{"a":"\\ud800"}`, `{"a":"\ufffd"}`} {
		if _, err := scopeMembers(raw); err != nil {
			t.Fatalf("valid escape rejected %s: %v", raw, err)
		}
	}

	for _, raw := range []string{string([]byte{255}), `{"a":"b` + string([]byte{255}) + `"}`, `{"a":"b\u0000"}`} {
		if _, err := scopeMembers(raw); !errors.Is(err, ErrIndexInvalid) {
			t.Fatalf("invalid unicode/token accepted: %v", err)
		}
	}
	entries := make([]string, MaxScopeMembers)
	for n := range entries {
		entries[n] = fmt.Sprintf("\"p%d\":\"a\"", n)
	}
	if values, err := scopeMembers("{" + strings.Join(entries, ",") + "}"); err != nil || len(values) != MaxScopeMembers {
		t.Fatal(err)
	}
	if _, err := scopeMembers("{" + strings.Join(entries, ",") + ",\"extra\":\"a\"}"); !errors.Is(err, ErrIndexInvalid) {
		t.Fatal(err)
	}
}

func TestScopedConcurrentPublication(t *testing.T) {
	_, db, idx, path := fixture(t, Limits{})
	if _, err := db.Exec(`CREATE TABLE publications(id TEXT PRIMARY KEY,manifest TEXT); CREATE TABLE search_state(id TEXT PRIMARY KEY,generation TEXT); INSERT INTO publications VALUES('scope','{"first":"a","second":"b"}'); INSERT INTO search_state VALUES('state','a'); DELETE FROM documents_search WHERE record_id!='a';`); err != nil {
		t.Fatal(err)
	}
	idx.Scope = &Scope{Collection: "publications", Field: "manifest"}
	idx.Generation = &Generation{Collection: "search_state", Record: "state", Field: "generation"}
	e, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	done := make(chan error, 1)
	go func() {
		for n := 0; n < 50; n++ {
			id := []string{"a", "b"}[n%2]
			tx, err := db.Begin()
			if err != nil {
				done <- err
				return
			}
			_, err = tx.Exec(`DELETE FROM documents_search; INSERT INTO documents_search(record_id,title,body) SELECT id,title,body FROM documents WHERE id=?; UPDATE search_state SET generation=?`, id, id)
			if err != nil {
				tx.Rollback()
				done <- err
				return
			}
			if err = tx.Commit(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for n := 0; n < 50; n++ {
		r, err := e.SearchRequest(context.Background(), Request{Index: "docs", Query: "alpha", Scope: "scope"})
		if err != nil || len(r.Hits) != 1 || exposedGeneration(idx, r.Hits[0].ID, "scope", `["a","b"]`) != r.Generation {
			t.Fatalf("mixed transaction: %+v %v", r, err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestScopedSchemaAndGenerationFailClosed(t *testing.T) {
	for _, mutation := range []string{
		`DELETE FROM search_state`,
		`UPDATE search_state SET generation=''`,
		`UPDATE search_state SET generation=CAST(x'ff' AS TEXT)`,
		`DROP TABLE publications; CREATE VIEW publications AS SELECT 'scope' AS id, '{"p":"a"}' AS manifest`,
		`CREATE TABLE json_each(value TEXT,json TEXT)`,
	} {
		t.Run(mutation, func(t *testing.T) {
			_, db, idx, path := fixture(t, Limits{})
			if _, err := db.Exec(`CREATE TABLE publications(id TEXT PRIMARY KEY,manifest TEXT); CREATE TABLE search_state(id TEXT PRIMARY KEY,generation TEXT); INSERT INTO publications VALUES('scope','{"p":"a"}'); INSERT INTO search_state VALUES('state','one');`); err != nil {
				t.Fatal(err)
			}
			idx.Scope = &Scope{Collection: "publications", Field: "manifest"}
			idx.Generation = &Generation{Collection: "search_state", Record: "state", Field: "generation"}
			e, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if _, err = db.Exec(mutation); err != nil {
				t.Fatal(err)
			}
			if _, err = e.SearchRequest(context.Background(), Request{Index: "docs", Query: "alpha", Scope: "scope"}); !errors.Is(err, ErrIndexInvalid) {
				t.Fatalf("fail open: %v", err)
			}
		})
	}
}

func TestGenerationBindsRankingConfiguration(t *testing.T) {
	_, db, idx, path := fixture(t, Limits{})
	if _, err := db.Exec(`CREATE TABLE publications(id TEXT PRIMARY KEY,manifest TEXT); CREATE TABLE search_state(id TEXT PRIMARY KEY,generation TEXT); INSERT INTO publications VALUES('scope','{"a":"a","b":"b"}'); INSERT INTO search_state VALUES('state','same');`); err != nil {
		t.Fatal(err)
	}
	idx.Scope = &Scope{Collection: "publications", Field: "manifest"}
	idx.Generation = &Generation{Collection: "search_state", Record: "state", Field: "generation"}
	request := Request{Index: "docs", Query: "alpha", Scope: "scope", Limit: 1}
	open := func(index Index) *Engine {
		e, err := New(path, Config{Indexes: map[string]Index{"docs": index}}, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { e.Close() })
		return e
	}
	first, err := open(idx).SearchRequest(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.ExpectedGeneration = first.Generation
	request.Offset = 1
	restart, err := open(idx).SearchRequest(context.Background(), request)
	if err != nil || restart.Generation != first.Generation {
		t.Fatalf("same config restart: %+v %v", restart, err)
	}
	changed := idx
	changed.Weights = []float64{1, 20}
	if _, err := open(changed).SearchRequest(context.Background(), request); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("changed weights accepted old token: %v", err)
	}
	changed = idx
	changed.SnippetColumn = "title"
	if _, err := open(changed).SearchRequest(context.Background(), request); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("changed snippet accepted old token: %v", err)
	}
}

func TestGenerationBindsMembership(t *testing.T) {
	_, db, idx, path := fixture(t, Limits{})
	if _, err := db.Exec(`CREATE TABLE publications(id TEXT PRIMARY KEY,manifest TEXT); CREATE TABLE search_state(id TEXT PRIMARY KEY,generation TEXT); INSERT INTO publications VALUES('scope','{"a":"a","b":"b"}'); INSERT INTO search_state VALUES('state','same');`); err != nil {
		t.Fatal(err)
	}
	idx.Scope = &Scope{Collection: "publications", Field: "manifest"}
	idx.Generation = &Generation{Collection: "search_state", Record: "state", Field: "generation"}
	e, err := New(path, Config{Indexes: map[string]Index{"docs": idx}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	req := Request{Index: "docs", Query: "alpha", Scope: "scope", Limit: 1}
	first, err := e.SearchRequest(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE publications SET manifest='{"a":"b"}'`); err != nil {
		t.Fatal(err)
	}
	req.Offset = 1
	req.ExpectedGeneration = first.Generation
	if _, err = e.SearchRequest(context.Background(), req); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("changed membership accepted old token: %v", err)
	}
}
