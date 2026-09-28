package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketcontext/pocketcontext/internal/searchread"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func searchFixture(t *testing.T) (*tests.TestApp, Config, string, string) {
	t.Helper()
	app, token, outsider := fixture(t)
	index := searchread.Index{Table: "deal_search", Collection: "deals", Columns: []string{"title"}, SnippetColumn: "title", Weights: []float64{1}}
	// Trusted application migrations own index creation and maintenance.
	ddl, err := searchread.CanonicalDDL(index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.DB().NewQuery(ddl).Execute(); err != nil {
		t.Fatal(err)
	}
	collection, _ := app.FindCollectionByNameOrId("deals")
	for i, title := range []string{"Synthetic rocket project", "Synthetic rocket launch"} {
		record := core.NewRecord(collection)
		record.Set("id", fmt.Sprintf("searchrecord%03d", i))
		record.Set("title", title)
		record.Set("secret", "unsearchable-private-value")
		if err := app.Save(record); err != nil {
			t.Fatal(err)
		}
		if _, err := app.DB().NewQuery(`INSERT INTO deal_search(record_id,title) VALUES({:id},{:title})`).Bind(dbx.Params{"id": record.Id, "title": title}).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{AuthCollection: "agents", Tables: map[string][]string{"deals": {"id", "title"}}, TimeoutMS: 1000, MaxRows: 100, MaxBytes: 4096, Search: &searchread.Config{Indexes: map[string]searchread.Index{"deals": index}}}
	return app, cfg, token, outsider
}

func TestSearchHTTP(t *testing.T) {
	app, cfg, token, outsider := searchFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := app.FindCollectionByNameOrId("_superusers")
	if err != nil {
		t.Fatal(err)
	}
	superuser := core.NewRecord(collection)
	superuser.SetEmail("search-superuser@example.com")
	superuser.SetPassword("synthetic-password-123")
	if err := app.Save(superuser); err != nil {
		t.Fatal(err)
	}
	superToken, err := superuser.NewAuthToken()
	if err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"", outsider, superToken} {
		r := request(h, "POST", "/api/context/search", identity, `{"index":"deals","query":"rocket"}`)
		if r.Code != http.StatusUnauthorized && r.Code != http.StatusForbidden {
			t.Fatalf("unauthorized search: %d %s", r.Code, r.Body)
		}
	}
	r := request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"rocket","limit":1}`)
	var result searchread.Result
	if err := json.Unmarshal(r.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if r.Code != http.StatusOK || result.Index != "deals" || len(result.Hits) != 1 || !result.Truncated || !strings.Contains(result.Hits[0].Excerpt, "rocket") || !strings.HasPrefix(result.Hits[0].ID, "searchrecord") {
		t.Fatalf("search: %d %s", r.Code, r.Body)
	}
	if r.Header().Get("Cache-Control") != "no-store" || r.Header().Get("X-Context-Truncated") != "true" || !strings.Contains(r.Header().Get("Access-Control-Expose-Headers"), "X-Context-Truncated") {
		t.Fatalf("search headers: %v", r.Header())
	}
	r = request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"unsearchable-private-value"}`)
	if r.Code != http.StatusOK || strings.Contains(r.Body.String(), "searchrecord") {
		t.Fatalf("private text searchable: %d %s", r.Code, r.Body)
	}
	schema := request(h, "GET", "/api/context/schema", token, "")
	if schema.Code != http.StatusOK || !strings.Contains(schema.Body.String(), `"indexes":["deals"]`) || !strings.Contains(schema.Body.String(), `"maxTerms":16`) || strings.Contains(schema.Body.String(), "deal_search") || strings.Contains(schema.Body.String(), "secret") {
		t.Fatalf("search discovery: %d %s", schema.Code, schema.Body)
	}
	for _, query := range []string{
		`SELECT title FROM deal_search`,
		`SELECT * FROM deal_search_content`,
		`SELECT * FROM deal_search_config`,
		`SELECT * FROM deal_search_idx`,
		`SELECT * FROM deal_search_docsize`,
		`SELECT * FROM deal_search_data`,
		`SELECT name FROM sqlite_schema`,
		`PRAGMA data_version`,
	} {
		body, _ := json.Marshal(map[string]string{"sql": query})
		r := request(h, "POST", "/api/context/query", token, string(body))
		if r.Code != http.StatusBadRequest {
			t.Fatalf("search internals exposed through SQL: %s: %d %s", query, r.Code, r.Body)
		}
	}
	for _, body := range []string{
		`{"index":"deals","query":"rocket","sql":"SELECT secret FROM deals"}`,
		`{"index":"deals","query":"rocket","limit":-1}`,
		`{"index":"deals","query":"rocket","limit":101}`,
		`{"index":"deals","query":"rocket","limit":"1"}`,
		`{"index":"deals","query":""}`,
		`{"index":"deals","query":"rocket"} {}`,
		`{"index":"deal_search_content","query":"rocket"}`,
		`{"index":"deals","query":"` + strings.Repeat("x", 4097) + `"}`,
		`{"index":"deals","query":"` + strings.Repeat("word ", 17) + `"}`,
		`{"index":"deals","query":"` + strings.Repeat("x", 65536) + `"}`,
		`null`,
		`[]`,
	} {
		r := request(h, "POST", "/api/context/search", token, body)
		if r.Code != http.StatusBadRequest {
			t.Fatalf("bad search accepted: %d %s", r.Code, r.Body)
		}
		if strings.Contains(r.Body.String(), "deal_search") || strings.Contains(r.Body.String(), "sqlite") {
			t.Fatalf("search error exposed implementation: %s", r.Body)
		}
	}
	if _, err := app.DB().NewQuery(`DROP TABLE deal_search`).Execute(); err != nil {
		t.Fatal(err)
	}
	r = request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"rocket"}`)
	if r.Code != http.StatusServiceUnavailable || strings.Contains(r.Body.String(), "deal_search") || strings.Contains(r.Body.String(), "sqlite") {
		t.Fatalf("stale index failure: %d %s", r.Code, r.Body)
	}
}

func TestSearchRejectsUnsafeSources(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*Config)
	}{
		{"hidden field", func(c *Config) {
			index := c.Search.Indexes["deals"]
			index.Columns = []string{"secret"}
			index.SnippetColumn = "secret"
			c.Search.Indexes["deals"] = index
		}},
		{"unexposed public field", func(c *Config) { c.Tables["deals"] = []string{"id"} }},
		{"unexposed record id", func(c *Config) { c.Tables["deals"] = []string{"title"} }},
		{"auth source", func(c *Config) {
			index := c.Search.Indexes["deals"]
			index.Collection = "agents"
			c.Search.Indexes["deals"] = index
		}},
		{"system source", func(c *Config) {
			index := c.Search.Indexes["deals"]
			index.Collection = "_collections"
			c.Search.Indexes["deals"] = index
		}},
		{"missing source", func(c *Config) {
			index := c.Search.Indexes["deals"]
			index.Collection = "unknown"
			c.Search.Indexes["deals"] = index
		}},
		{"collection as index", func(c *Config) {
			index := c.Search.Indexes["deals"]
			index.Table = "deals"
			c.Search.Indexes["deals"] = index
		}},
		{"filtered snapshot", func(c *Config) {
			c.Snapshot = &sqlread.SnapshotConfig{Filters: map[string]string{"deals": "id=:requester"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			app, cfg, _, _ := searchFixture(t)
			test.change(&cfg)
			if _, err := startConfiguredRouter(t, app, cfg); err == nil {
				t.Fatal("unsafe search configuration accepted")
			}
		})
	}
}

func TestSearchImplicitPublicColumns(t *testing.T) {
	app, cfg, token, _ := searchFixture(t)
	collection, _ := app.FindCollectionByNameOrId("deals")
	collection.Fields.RemoveByName("secret")
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}
	cfg.Tables["deals"] = nil
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"rocket"}`)
	if r.Code != http.StatusOK {
		t.Fatalf("implicit public columns: %d %s", r.Code, r.Body)
	}
}

func TestSearchDisabled(t *testing.T) {
	app, token, _ := fixture(t)
	h, err := startRouter(t, app, map[string][]string{"deals": {"id", "title"}})
	if err != nil {
		t.Fatal(err)
	}
	r := request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"rocket"}`)
	if r.Code != http.StatusNotFound {
		t.Fatalf("unconfigured search enabled: %d %s", r.Code, r.Body)
	}
}

func TestSearchHTTPResponseLimit(t *testing.T) {
	app, cfg, token, _ := searchFixture(t)
	cfg.MaxBytes = 1024
	title := strings.Repeat("r", 1800)
	record, err := app.FindRecordById("deals", "searchrecord000")
	if err != nil {
		t.Fatal(err)
	}
	record.Set("title", title)
	if err := app.Save(record); err != nil {
		t.Fatal(err)
	}
	if _, err := app.DB().NewQuery(`UPDATE deal_search SET title={:title} WHERE record_id='searchrecord000'`).Bind(dbx.Params{"title": title}).Execute(); err != nil {
		t.Fatal(err)
	}
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]string{"index": "deals", "query": title})
	r := request(h, "POST", "/api/context/search", token, string(body))
	if r.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("search byte cap: %d %s", r.Code, r.Body)
	}
}

func scopedSearchFixture(t *testing.T) (*tests.TestApp, Config, string) {
	app, cfg, token, _ := searchFixture(t)
	scopes := core.NewBaseCollection("search_scopes")
	scopes.Fields.Add(&core.JSONField{Name: "members"}, &core.TextField{Name: "private", Hidden: true})
	state := core.NewBaseCollection("search_state")
	state.Fields.Add(&core.TextField{Name: "generation"})
	for _, c := range []*core.Collection{scopes, state} {
		if err := app.Save(c); err != nil {
			t.Fatal(err)
		}
	}
	r := core.NewRecord(scopes)
	r.Set("id", "scope0000000001")
	r.Set("members", map[string]string{"one": "searchrecord000", "two": "searchrecord001"})
	if err := app.Save(r); err != nil {
		t.Fatal(err)
	}
	r = core.NewRecord(state)
	r.Set("id", "state0000000001")
	r.Set("generation", "one")
	if err := app.Save(r); err != nil {
		t.Fatal(err)
	}
	cfg.Tables["search_scopes"] = []string{"id", "members"}
	cfg.Tables["search_state"] = []string{"id", "generation"}
	idx := cfg.Search.Indexes["deals"]
	idx.Scope = &searchread.Scope{Collection: "search_scopes", Field: "members"}
	idx.Generation = &searchread.Generation{Collection: "search_state", Record: "state0000000001", Field: "generation"}
	cfg.Search.Indexes["deals"] = idx
	return app, cfg, token
}
func TestSearchScopedHTTP(t *testing.T) {
	app, cfg, token := scopedSearchFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"rocket","scope":"scope0000000001","limit":1}`)
	var first searchread.Result
	if err = json.Unmarshal(r.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if r.Code != 200 || !first.HasMore || len(first.Generation) != 64 || first.Scope != "scope0000000001" {
		t.Fatalf("first: %d %s", r.Code, r.Body)
	}
	r = request(h, "POST", "/api/context/search", token, fmt.Sprintf(`{"index":"deals","query":"rocket","scope":"scope0000000001","limit":1,"offset":1,"expectedGeneration":%q}`, first.Generation))
	var second searchread.Result
	json.Unmarshal(r.Body.Bytes(), &second)
	if r.Code != 200 || second.HasMore || len(second.Hits) != 1 || second.Hits[0].ID == first.Hits[0].ID {
		t.Fatalf("second: %d %s", r.Code, r.Body)
	}
	state, err := app.FindRecordById("search_state", "state0000000001")
	if err != nil {
		t.Fatal(err)
	}
	state.Set("generation", "two")
	if err = app.Save(state); err != nil {
		t.Fatal(err)
	}
	r = request(h, "POST", "/api/context/search", token, fmt.Sprintf(`{"index":"deals","query":"rocket","scope":"scope0000000001","limit":1,"offset":1,"expectedGeneration":%q}`, first.Generation))
	if r.Code != 409 || strings.Contains(r.Body.String(), "searchrecord") {
		t.Fatalf("conflict: %d %s", r.Code, r.Body)
	}
	for _, body := range []string{`{"index":"deals","query":"rocket"}`, `{"index":"deals","query":"rocket","scope":"scope0000000001","offset":1}`} {
		r = request(h, "POST", "/api/context/search", token, body)
		if r.Code != 400 {
			t.Fatalf("missing scope/generation: %d %s", r.Code, r.Body)
		}
	}
	r = request(h, "POST", "/api/context/search", token, `{"index":"deals","query":"rocket","scope":"absent"}`)
	if r.Code != 503 {
		t.Fatalf("missing scope: %d %s", r.Code, r.Body)
	}
}
func TestSearchScopedAuthorization(t *testing.T) {
	for _, mode := range []string{"hidden", "unexposed", "auth", "system", "missing_generation", "missing_scope", "bad_record", "bad_field"} {
		t.Run(mode, func(t *testing.T) {
			app, cfg, _ := scopedSearchFixture(t)
			idx := cfg.Search.Indexes["deals"]
			switch mode {
			case "hidden":
				idx.Scope.Field = "private"
			case "unexposed":
				delete(cfg.Tables, "search_scopes")
			case "auth":
				idx.Scope.Collection = "agents"
			case "system":
				idx.Generation.Collection = "_superusers"
			case "missing_generation":
				idx.Generation = nil
			case "missing_scope":
				idx.Scope = nil
			case "bad_record":
				idx.Generation.Record = ""
			case "bad_field":
				idx.Scope.Field = "members;DROP TABLE deals"
			}
			cfg.Search.Indexes["deals"] = idx
			if _, err := startConfiguredRouter(t, app, cfg); err == nil {
				t.Fatal("unsafe scoped configuration accepted")
			}
		})
	}
}
