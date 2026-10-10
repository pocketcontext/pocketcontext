package searchread

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

// Synthetic spaces: alice and erin belong to s1, dave to s1 and s2, bob to s2.
func privateFixture(t *testing.T) (*sql.DB, string, sqlread.Config, sqlread.SnapshotConfig) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL;
CREATE TABLE docs(id TEXT PRIMARY KEY, space TEXT, title TEXT, body TEXT, rank_hint INTEGER);
CREATE TABLE manifests(id TEXT PRIMARY KEY, space TEXT, members TEXT);
CREATE TABLE space_members(user TEXT, space TEXT);
INSERT INTO space_members VALUES('alice','s1'),('erin','s1'),('dave','s1'),('dave','s2'),('bob','s2');
INSERT INTO docs VALUES
 ('d1','s1','Rocket guide','synthetic rocket launch checklist',1),
 ('d2','s1','Garden notes','rocket rocket rocket in the garden',2),
 ('d3','s1','Draft','unpublished rocket draft',3),
 ('d4','s1','Null body',NULL,4),
 ('b1','s2','Bob rocket','bob private rocket plans',5);
INSERT INTO manifests VALUES
 ('m1','s1','{"guide":"d1","garden":"d2","null":"d4"}'),
 ('m2','s2','{"bob":"b1"}'),
 ('m3','s1','{"guide":"d1","foreign":"b1","again":"d1"}');`)
	if err != nil {
		t.Fatal(err)
	}
	filter := "EXISTS (SELECT 1 FROM space_members m WHERE m.user = :requester AND m.space = %s.space)"
	return db, path,
		sqlread.Config{Tables: map[string][]string{"docs": {"id", "space", "title", "body", "rank_hint"}, "manifests": {"id", "space", "members"}}, Timeout: time.Second, MaxRows: 100, MaxBytes: 1 << 20},
		sqlread.SnapshotConfig{Filters: map[string]string{"docs": fmt.Sprintf(filter, "docs"), "manifests": fmt.Sprintf(filter, "manifests")}, PolicyTables: map[string][]string{"space_members": {"user", "space"}}}
}

func privateConfig() Config {
	return Config{Indexes: map[string]Index{
		"pages":   {Isolation: IsolationPrivate, Collection: "docs", Columns: []string{"title", "body"}, SnippetColumn: "body", Weights: []float64{5, 1}, Scope: &Scope{Collection: "manifests", Field: "members"}},
		"visible": {Isolation: IsolationPrivate, Collection: "docs", Columns: []string{"title", "body"}, SnippetColumn: "body", Scope: &Scope{Collection: "manifests", Field: "members"}, Members: MembersVisible},
		"all":     {Isolation: IsolationPrivate, Collection: "docs", Columns: []string{"title", "body"}, SnippetColumn: "body", Weights: []float64{5, 1}},
		"titles":  {Isolation: IsolationPrivate, Collection: "docs", Columns: []string{"title"}, SnippetColumn: "title", Where: "docs.title <> 'Draft'"},
	}}
}

func newPrivate(t *testing.T, path string, c sqlread.Config, sc sqlread.SnapshotConfig, cfg Config) (*PrivateEngine, *sqlread.SnapshotSource) {
	t.Helper()
	source, err := sqlread.NewSnapshotSource(path, c, sc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close() })
	e, err := NewPrivate(source, cfg, Limits{Timeout: time.Second, MaxRows: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, source
}

func ids(r Result) string {
	out := []string{}
	for _, h := range r.Hits {
		out = append(out, h.ID)
	}
	return strings.Join(out, ",")
}

func TestPrivateScopeAuthorization(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	ctx := context.Background()
	r, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"})
	if err != nil || ids(r) != "d1,d2" || r.Scope != "m1" || len(r.Generation) != 64 || !strings.Contains(r.Hits[0].Excerpt, "rocket") {
		t.Fatalf("own scope: %+v %v", r, err)
	}
	// Another space's scope and a nonexistent scope are indistinguishable.
	_, invisible := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "m2"})
	_, missing := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "nonexistent"})
	if !errors.Is(invisible, ErrScopeNotFound) || !errors.Is(missing, ErrScopeNotFound) || invisible.Error() != missing.Error() {
		t.Fatalf("scope errors: %v / %v", invisible, missing)
	}
	if r, err := e.SearchAs(ctx, "bob", Request{Index: "pages", Query: "rocket", Scope: "m2"}); err != nil || ids(r) != "b1" {
		t.Fatalf("bob: %+v %v", r, err)
	}
	// members=all never returns partial results; members=visible excludes.
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "m3"}); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("members all: %v", err)
	}
	if r, err := e.SearchAs(ctx, "alice", Request{Index: "visible", Query: "rocket", Scope: "m3"}); err != nil || ids(r) != "d1" {
		t.Fatalf("members visible: %+v %v", r, err)
	}
	if r, err := e.SearchAs(ctx, "dave", Request{Index: "pages", Query: "rocket", Scope: "m3"}); err != nil || ids(r) != "b1,d1" && ids(r) != "d1,b1" {
		t.Fatalf("members all visible: %+v %v", r, err)
	}
	for _, req := range []Request{
		{Index: "pages", Query: "rocket"},
		{Index: "all", Query: "rocket", Scope: "m1"},
		{Index: "pages", Query: "rocket", Scope: "m1", Offset: 1},
		{Index: "pages", Query: "rocket", Scope: "m1", Offset: MaxOffset + 1, ExpectedGeneration: "x"},
		{Index: "pages", Query: "", Scope: "m1"},
		{Index: "pages", Query: "rocket", Scope: "m1", Limit: 101},
		{Index: "missing", Query: "rocket"},
	} {
		if _, err := e.SearchAs(ctx, "alice", req); !errors.Is(err, ErrInvalidQuery) {
			t.Fatalf("%+v: %v", req, err)
		}
	}
	if _, err := e.SearchAs(ctx, "", Request{Index: "all", Query: "rocket"}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatal(err)
	}
}

func TestPrivateScopeInvalidMembership(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	for _, members := range []string{`[]`, `{"a":"d1","a":"d2"}`, `{"a":1}`, `not json`} {
		if _, err := db.Exec("UPDATE manifests SET members=? WHERE id='m1'", members); err != nil {
			t.Fatal(err)
		}
		if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"}); !errors.Is(err, ErrIndexInvalid) {
			t.Fatalf("%s: %v", members, err)
		}
	}
	if _, err := db.Exec("UPDATE manifests SET members=NULL WHERE id='m1'"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"}); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("null membership: %v", err)
	}
	if _, err := db.Exec("UPDATE manifests SET members='{}' WHERE id='m1'; UPDATE docs SET body=x'00ff' WHERE id='d1'"); err != nil {
		t.Fatal(err)
	}
	if r, err := e.SearchAs(context.Background(), "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"}); err != nil || len(r.Hits) != 0 {
		t.Fatalf("empty scope: %+v %v", r, err)
	}
	if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"}); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("blob value: %v", err)
	}
	numeric, _ := newPrivate(t, path, c, sc, Config{Indexes: map[string]Index{"hints": {Isolation: IsolationPrivate, Collection: "docs", Columns: []string{"rank_hint"}, SnippetColumn: "rank_hint"}}})
	if _, err := numeric.SearchAs(context.Background(), "alice", Request{Index: "hints", Query: "1"}); !errors.Is(err, ErrIndexInvalid) {
		t.Fatalf("integer value: %v", err)
	}
}

func TestPrivateScopelessVisibleRows(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	for _, tc := range []struct{ user, index, want string }{
		{"alice", "all", "d1,d2,d3"}, {"bob", "all", "b1"}, {"nobody", "all", ""},
		{"alice", "titles", "d1"}, {"dave", "titles", "b1,d1"},
	} {
		r, err := e.SearchAs(context.Background(), tc.user, Request{Index: tc.index, Query: "rocket"})
		if err != nil || ids(r) != tc.want || r.Scope != "" || len(r.Generation) != 64 {
			t.Fatalf("%s %s: %+v %v", tc.user, tc.index, r, err)
		}
	}
	r, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "private"})
	if err != nil || len(r.Hits) != 0 {
		t.Fatalf("invisible text: %+v %v", r, err)
	}
}

func TestPrivateRankingIsolation(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	ctx := context.Background()
	search := func(user string, req Request) Result {
		t.Helper()
		r, err := e.SearchAs(ctx, user, req)
		if err != nil {
			t.Fatalf("%s: %v", user, err)
		}
		return r
	}
	scoped := Request{Index: "pages", Query: "rocket", Scope: "m1"}
	scopeless := Request{Index: "all", Query: "rocket"}
	beforeScoped, beforeAll := search("alice", scoped), search("alice", scopeless)
	// Other users see many extra documents containing the query terms.
	for i := 0; i < 200; i++ {
		if _, err := db.Exec("INSERT INTO docs VALUES(?, 's2', 'rocket', 'rocket rocket launch checklist garden', 0)", fmt.Sprintf("x%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, user := range []string{"alice", "erin"} {
		if r := search(user, scoped); !reflect.DeepEqual(r, beforeScoped) {
			t.Fatalf("%s scoped ranking changed:\n%+v\n%+v", user, r, beforeScoped)
		}
		if r := search(user, scopeless); !reflect.DeepEqual(r, beforeAll) {
			t.Fatalf("%s scopeless ranking changed:\n%+v\n%+v", user, r, beforeAll)
		}
	}
	// dave sees the same scope content, so receives identical scores and token.
	if r := search("dave", scoped); !reflect.DeepEqual(r, beforeScoped) {
		t.Fatalf("dave scoped: %+v", r)
	}
	if r := search("dave", scopeless); reflect.DeepEqual(r.Generation, beforeAll.Generation) || len(r.Hits) != 20 {
		t.Fatalf("dave scopeless must reflect his own visible set: %+v", r)
	}
}

func TestPrivatePaginationAndRevocation(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	ctx := context.Background()
	first, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket", Limit: 1})
	if err != nil || len(first.Hits) != 1 || !first.HasMore || !first.Truncated {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket", Limit: 1, Offset: 1, ExpectedGeneration: first.Generation})
	if err != nil || len(second.Hits) != 1 || second.Hits[0].ID == first.Hits[0].ID || second.Generation != first.Generation {
		t.Fatalf("second: %+v %v", second, err)
	}
	// Changes invisible to alice do not affect her generation.
	if _, err := db.Exec("UPDATE docs SET body='changed rocket' WHERE id='b1'"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket", Limit: 1, Offset: 2, ExpectedGeneration: first.Generation}); err != nil {
		t.Fatalf("invisible change: %v", err)
	}
	if _, err := db.Exec("UPDATE docs SET body='changed rocket' WHERE id='d3'"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket", Limit: 1, Offset: 1, ExpectedGeneration: first.Generation}); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("visible change: %v", err)
	}
	// Revocation through policy data applies on the next request despite the cache.
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SearchAs(ctx, "erin", Request{Index: "pages", Query: "rocket", Scope: "m1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DELETE FROM space_members WHERE user='alice'"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "m1"}); !errors.Is(err, ErrScopeNotFound) {
		t.Fatalf("scoped revocation: %v", err)
	}
	if r, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket"}); err != nil || len(r.Hits) != 0 {
		t.Fatalf("scopeless revocation: %+v %v", r, err)
	}
	if r, err := e.SearchAs(ctx, "erin", Request{Index: "pages", Query: "rocket", Scope: "m1"}); err != nil || ids(r) != "d1,d2" {
		t.Fatalf("erin unaffected: %+v %v", r, err)
	}
}

func TestPrivateIndexByteLimit(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	cfg := privateConfig()
	cfg.MaxIndexBytes = 2048
	e, _ := newPrivate(t, path, c, sc, cfg)
	if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE docs SET body=? WHERE id='d3'", strings.Repeat("rocket ", 400)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"}); !errors.Is(err, ErrIndexLimit) {
		t.Fatalf("index limit: %v", err)
	}
	// Bob's smaller visible set is unaffected.
	if r, err := e.SearchAs(context.Background(), "bob", Request{Index: "all", Query: "rocket"}); err != nil || ids(r) != "b1" {
		t.Fatalf("bob: %+v %v", r, err)
	}
	sc.MaxRows = 2
	limited, _ := newPrivate(t, path, c, sc, privateConfig())
	if _, err := limited.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"}); !errors.Is(err, ErrIndexLimit) {
		t.Fatalf("snapshot limit: %v", err)
	}
}

func TestPrivateCacheBoundsAndLostConnection(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	cfg := privateConfig()
	cfg.MaxIndexBytes, cfg.CacheBytes = 1024, 100<<10
	e, _ := newPrivate(t, path, c, sc, cfg)
	ctx := context.Background()
	want := map[string]string{}
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("n%02d", i)
		if _, err := db.Exec("INSERT INTO docs VALUES(?, 's1', 'note', ?, 0); INSERT INTO manifests VALUES(?, 's1', ?)", id, "distinct rocket "+id, "scope"+id, `{"a":"`+id+`"}`); err != nil {
			t.Fatal(err)
		}
		want["scope"+id] = id
		r, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "scope" + id})
		if err != nil || ids(r) != id {
			t.Fatalf("%s: %+v %v", id, r, err)
		}
		entries, bytes := e.cache.stats()
		if entries < 1 || bytes > cfg.CacheBytes || entries > MaxCacheEntries {
			t.Fatalf("cache bounds: %d entries %d bytes", entries, bytes)
		}
	}
	if entries, _ := e.cache.stats(); entries >= 12 {
		t.Fatalf("no eviction: %d", entries)
	}
	// Evicted and cached scopes both return the same correct results.
	for scope, id := range want {
		if r, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: scope}); err != nil || ids(r) != id {
			t.Fatalf("%s: %+v %v", scope, r, err)
		}
	}
	// A failed pinned connection yields an error, never empty results, and is
	// replaced on the next request.
	r, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "scopen00"})
	if err != nil {
		t.Fatal(err)
	}
	entry := e.cache.acquire(r.Generation)
	if entry == nil {
		t.Fatal("entry not cached")
	}
	entry.conn.Close()
	e.cache.release(entry)
	if r, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "scopen00"}); err == nil {
		t.Fatalf("lost connection returned results: %+v", r)
	}
	if r, err := e.SearchAs(ctx, "alice", Request{Index: "pages", Query: "rocket", Scope: "scopen00"}); err != nil || ids(r) != "n00" {
		t.Fatalf("rebuild: %+v %v", r, err)
	}
	e.Close()
	if entries, bytes := e.cache.stats(); entries != 0 || bytes != 0 {
		t.Fatalf("close: %d %d", entries, bytes)
	}
}

func TestPrivateConcurrentSearches(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func(i int) {
			user, want := "alice", "d1,d2,d3"
			if i%2 == 1 {
				user, want = "bob", "b1"
			}
			r, err := e.SearchAs(context.Background(), user, Request{Index: "all", Query: "rocket"})
			if err == nil && ids(r) != want {
				err = fmt.Errorf("%s: %s", user, ids(r))
			}
			errs <- err
		}(i)
	}
	for i := 0; i < 32; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPrivateConfigRejected(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	source, err := sqlread.NewSnapshotSource(path, c, sc)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	base := Index{Isolation: IsolationPrivate, Collection: "docs", Columns: []string{"title"}, SnippetColumn: "title"}
	for name, mutate := range map[string]func(*Index, *Config){
		"shared index":      func(i *Index, _ *Config) { i.Isolation = "" },
		"unknown isolation": func(i *Index, _ *Config) { i.Isolation = "shared" },
		"table":             func(i *Index, _ *Config) { i.Table = "docs_search" },
		"generation": func(i *Index, _ *Config) {
			i.Generation = &Generation{Collection: "manifests", Record: "m1", Field: "members"}
		},
		"members without scope": func(i *Index, _ *Config) { i.Members = MembersAll },
		"bad members": func(i *Index, _ *Config) {
			i.Scope, i.Members = &Scope{Collection: "manifests", Field: "members"}, "some"
		},
		"blank where":         func(i *Index, _ *Config) { i.Where = " " },
		"bad where":           func(i *Index, _ *Config) { i.Where = "1); DELETE FROM docs; --" },
		"where binds":         func(i *Index, _ *Config) { i.Where = "title = :other" },
		"where policy column": func(i *Index, _ *Config) { i.Where = "EXISTS (SELECT secret FROM space_members)" },
		"where schema":        func(i *Index, _ *Config) { i.Where = "EXISTS (SELECT 1 FROM sqlite_schema)" },
		"unconfigured column": func(i *Index, _ *Config) { i.Columns, i.SnippetColumn = []string{"secret"}, "secret" },
		"policy source": func(i *Index, _ *Config) {
			i.Collection, i.Columns, i.SnippetColumn = "space_members", []string{"space"}, "space"
		},
		"unknown scope":          func(i *Index, _ *Config) { i.Scope = &Scope{Collection: "space_members", Field: "space"} },
		"unconfigured scope":     func(i *Index, _ *Config) { i.Scope = &Scope{Collection: "manifests", Field: "secret"} },
		"reserved column":        func(i *Index, _ *Config) { i.Columns, i.SnippetColumn = []string{"rank"}, "rank" },
		"internal table column":  func(i *Index, _ *Config) { i.Columns, i.SnippetColumn = []string{"private_search"}, "private_search" },
		"snippet":                func(i *Index, _ *Config) { i.SnippetColumn = "body" },
		"weights":                func(i *Index, _ *Config) { i.Weights = []float64{1, 2} },
		"small maxIndexBytes":    func(_ *Index, c *Config) { c.MaxIndexBytes = 10 },
		"large maxIndexBytes":    func(_ *Index, c *Config) { c.MaxIndexBytes = MaxMaxIndexBytes + 1 },
		"cache below index size": func(_ *Index, c *Config) { c.MaxIndexBytes, c.CacheBytes = 4096, 2048 },
		"large cache":            func(_ *Index, c *Config) { c.CacheBytes = MaxCacheBytes + 1 },
	} {
		idx := base
		cfg := Config{}
		mutate(&idx, &cfg)
		cfg.Indexes = map[string]Index{"docs": idx}
		if e, err := NewPrivate(source, cfg, Limits{}); err == nil {
			e.Close()
			t.Fatalf("%s accepted", name)
		}
	}
	if e, err := NewPrivate(source, Config{Indexes: map[string]Index{"docs": base}}, Limits{}); err != nil {
		t.Fatal(err)
	} else {
		e.Close()
	}
	if _, err := NewPrivate(nil, Config{Indexes: map[string]Index{"docs": base}}, Limits{}); err == nil {
		t.Fatal("nil source accepted")
	}
	// Shared engines reject private configuration.
	shared := base
	shared.Isolation, shared.Table = IsolationPrivate, "docs_search"
	if e, err := New(path, Config{Indexes: map[string]Index{"docs": shared}}, Limits{}); err == nil {
		e.Close()
		t.Fatal("shared engine accepted private index")
	}
	shared.Isolation = ""
	if e, err := New(path, Config{Indexes: map[string]Index{"docs": shared}, MaxIndexBytes: 4096}, Limits{}); err == nil {
		e.Close()
		t.Fatal("shared engine accepted private limits")
	}
}

func TestPrivateGenerationBindsConfiguration(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	cfg := privateConfig()
	e, _ := newPrivate(t, path, c, sc, cfg)
	a, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"})
	if err != nil {
		t.Fatal(err)
	}
	idx := cfg.Indexes["all"]
	idx.Weights = []float64{1, 5}
	cfg.Indexes = map[string]Index{"all": idx}
	reweighted, _ := newPrivate(t, path, c, sc, cfg)
	b, err := reweighted.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"})
	if err != nil || a.Generation == b.Generation {
		t.Fatalf("generation ignores weights: %v", err)
	}
	if _, err := reweighted.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket", Offset: 1, ExpectedGeneration: a.Generation}); !errors.Is(err, ErrGenerationConflict) {
		t.Fatal(err)
	}
}

func TestPrivateAdmissionCancellation(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	for i := 0; i < privateRequests; i++ {
		e.requests <- struct{}{}
	}
	defer func() {
		for i := 0; i < privateRequests; i++ {
			<-e.requests
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admission wait: %v", err)
	}
	if entries, _ := e.cache.stats(); entries != 0 {
		t.Fatal("queued request built an index")
	}
}

func TestPrivateCachedQueryCancellation(t *testing.T) {
	_, path, c, sc := privateFixture(t)
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	r, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket"})
	if err != nil {
		t.Fatal(err)
	}
	entry := e.cache.acquire(r.Generation)
	defer e.cache.release(entry)
	entry.queries <- struct{}{}
	defer func() { <-entry.queries }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := e.SearchAs(ctx, "alice", Request{Index: "all", Query: "rocket"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cached query wait: %v", err)
	}
}

// Bounded synthetic load: every request re-reads about 1 MiB of visible text.
// This measures local behavior; it is not a public deployment capacity claim.
func TestPrivateSyntheticConcurrentLoad(t *testing.T) {
	db, path, c, sc := privateFixture(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	body := strings.Repeat("synthetic rocket launch checklist ", 32)
	for i := 0; i < 1000; i++ {
		if _, err = tx.Exec("INSERT INTO docs VALUES(?,?,?,?,?)", fmt.Sprintf("load%04d", i), "s1", "Synthetic load", body, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	e, _ := newPrivate(t, path, c, sc, privateConfig())
	e.limits.Timeout = 5 * time.Second
	const requests = 16
	for _, phase := range []string{"cold", "warm"} {
		started := time.Now()
		results := make(chan error, requests)
		for i := 0; i < requests; i++ {
			go func() {
				r, err := e.SearchAs(context.Background(), "alice", Request{Index: "all", Query: "rocket", Limit: 10})
				if err == nil && (len(r.Hits) != 10 || !r.HasMore) {
					err = fmt.Errorf("unexpected search result: %d hits, hasMore=%v", len(r.Hits), r.HasMore)
				}
				results <- err
			}()
		}
		for i := 0; i < requests; i++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		entries, bytes := e.cache.stats()
		t.Logf("%s: %d concurrent requests, 1000 x %d-byte bodies, wall=%v, cached indexes=%d, cached bytes=%d", phase, requests, len(body), time.Since(started), entries, bytes)
	}
}
