package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"github.com/pocketcontext/pocketcontext/internal/searchread"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

// Synthetic spaces: alice and erin read s1, bob reads s2.
func privateSearchFixture(t *testing.T) (*tests.TestApp, Config, map[string]string, map[string]*core.Record) {
	t.Helper()
	app, _, _ := fixture(t)
	auth, err := app.FindCollectionByNameOrId("agents")
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{}
	records := map[string]*core.Record{}
	for _, name := range []string{"alice", "erin", "bob"} {
		r := core.NewRecord(auth)
		r.SetEmail(name + "@example.com")
		r.SetPassword("test-password-12345")
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		token, err := r.NewAuthToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens[name], records[name] = token, r
	}
	for _, spec := range []struct {
		name   string
		fields []core.Field
	}{
		{"pages", []core.Field{&core.TextField{Name: "space"}, &core.TextField{Name: "title"}, &core.TextField{Name: "body"}, &core.TextField{Name: "notes", Hidden: true}}},
		{"publications", []core.Field{&core.TextField{Name: "space"}, &core.JSONField{Name: "manifest"}, &core.TextField{Name: "private", Hidden: true}}},
		{"space_members", []core.Field{&core.TextField{Name: "user"}, &core.TextField{Name: "space"}}},
	} {
		c := core.NewBaseCollection(spec.name)
		c.Fields.Add(spec.fields...)
		if err := app.Save(c); err != nil {
			t.Fatal(err)
		}
	}
	save := func(table, id string, values map[string]any) *core.Record {
		c, err := app.FindCollectionByNameOrId(table)
		if err != nil {
			t.Fatal(err)
		}
		r := core.NewRecord(c)
		if id != "" {
			r.Set("id", id)
		}
		for k, v := range values {
			r.Set(k, v)
		}
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	for _, p := range [][4]string{
		{"pagealpha000001", "s1", "Rocket guide", "synthetic rocket launch checklist"},
		{"pagealpha000002", "s1", "Garden", "rocket rocket in the garden"},
		{"pagealpha000003", "s1", "Unlisted", "unlisted rocket draft"},
		{"pagebravo000001", "s2", "Bob rocket", "bob private rocket plans"},
	} {
		save("pages", p[0], map[string]any{"space": p[1], "title": p[2], "body": p[3], "notes": "hidden rocket notes"})
	}
	save("publications", "publicalpha0001", map[string]any{"space": "s1", "manifest": map[string]string{"guide": "pagealpha000001", "garden": "pagealpha000002"}})
	save("publications", "publicbravo0001", map[string]any{"space": "s2", "manifest": map[string]string{"bob": "pagebravo000001"}})
	save("publications", "publicmixed0001", map[string]any{"space": "s1", "manifest": map[string]string{"guide": "pagealpha000001", "foreign": "pagebravo000001"}})
	for _, m := range [][2]string{{"alice", "s1"}, {"erin", "s1"}, {"bob", "s2"}} {
		records[m[0]+"_"+m[1]] = save("space_members", "", map[string]any{"user": records[m[0]].Id, "space": m[1]})
	}
	filter := "EXISTS (SELECT 1 FROM space_members m WHERE m.user = :requester AND m.space = %s.space)"
	cfg := Config{
		AuthCollection: "agents",
		Tables:         map[string][]string{"pages": {"id", "space", "title", "body"}, "publications": {"id", "space", "manifest"}},
		TimeoutMS:      1000, MaxRows: 100, MaxBytes: 8192,
		Snapshot: &sqlread.SnapshotConfig{
			Filters:      map[string]string{"pages": fmt.Sprintf(filter, "pages"), "publications": fmt.Sprintf(filter, "publications")},
			PolicyTables: map[string][]string{"space_members": {"user", "space"}},
		},
		Search: &searchread.Config{Indexes: map[string]searchread.Index{
			"published": {Isolation: "private", Collection: "pages", Columns: []string{"title", "body"}, SnippetColumn: "body", Weights: []float64{5, 1}, Scope: &searchread.Scope{Collection: "publications", Field: "manifest"}},
			"visible":   {Isolation: "private", Collection: "pages", Columns: []string{"title", "body"}, SnippetColumn: "body", Scope: &searchread.Scope{Collection: "publications", Field: "manifest"}, Members: "visible"},
			"pages":     {Isolation: "private", Collection: "pages", Columns: []string{"title", "body"}, SnippetColumn: "body", Where: "pages.title <> 'Unlisted'"},
		}},
	}
	return app, cfg, tokens, records
}

func searchAs(t *testing.T, h http.Handler, token, body string) (int, searchread.Result, string) {
	t.Helper()
	r := request(h, "POST", "/api/context/search", token, body)
	var result searchread.Result
	if r.Code == http.StatusOK {
		if err := json.Unmarshal(r.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
	}
	return r.Code, result, r.Body.String()
}

func hitIDs(r searchread.Result) string {
	out := []string{}
	for _, h := range r.Hits {
		out = append(out, h.ID)
	}
	return strings.Join(out, ",")
}

func TestPrivateSearchHTTP(t *testing.T) {
	app, cfg, tokens, records := privateSearchFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	code, own, body := searchAs(t, h, tokens["alice"], `{"index":"published","query":"rocket","scope":"publicalpha0001"}`)
	if code != 200 || hitIDs(own) != "pagealpha000001,pagealpha000002" || len(own.Generation) != 64 || own.Scope != "publicalpha0001" {
		t.Fatalf("own scope: %d %s", code, body)
	}
	// Erin sees identical content and receives identical scores and token.
	if code, erin, body := searchAs(t, h, tokens["erin"], `{"index":"published","query":"rocket","scope":"publicalpha0001"}`); code != 200 || !reflect.DeepEqual(erin, own) {
		t.Fatalf("erin: %d %s", code, body)
	}
	// Another space's scope and a nonexistent scope are indistinguishable.
	code, _, other := searchAs(t, h, tokens["alice"], `{"index":"published","query":"rocket","scope":"publicbravo0001"}`)
	missingCode, _, missing := searchAs(t, h, tokens["alice"], `{"index":"published","query":"rocket","scope":"publicmissing01"}`)
	if code != 404 || missingCode != 404 || other != missing || !strings.Contains(other, "Search scope not found") {
		t.Fatalf("scope 404: %d %s / %d %s", code, other, missingCode, missing)
	}
	if code, _, body := searchAs(t, h, tokens["alice"], `{"index":"published","query":"rocket","scope":"publicmixed0001"}`); code != 503 || strings.Contains(body, "page") {
		t.Fatalf("members all: %d %s", code, body)
	}
	if code, r, body := searchAs(t, h, tokens["alice"], `{"index":"visible","query":"rocket","scope":"publicmixed0001"}`); code != 200 || hitIDs(r) != "pagealpha000001" {
		t.Fatalf("members visible: %d %s", code, body)
	}
	if code, r, body := searchAs(t, h, tokens["alice"], `{"index":"pages","query":"rocket"}`); code != 200 || hitIDs(r) != "pagealpha000001,pagealpha000002" || r.Scope != "" {
		t.Fatalf("scopeless: %d %s", code, body)
	}
	if code, r, body := searchAs(t, h, tokens["bob"], `{"index":"pages","query":"rocket"}`); code != 200 || hitIDs(r) != "pagebravo000001" {
		t.Fatalf("bob scopeless: %d %s", code, body)
	}
	if code, r, body := searchAs(t, h, tokens["alice"], `{"index":"pages","query":"hidden notes"}`); code != 200 || len(r.Hits) != 0 {
		t.Fatalf("hidden field searchable: %d %s", code, body)
	}
	// Pagination is stable until alice's visible content changes.
	code, first, body := searchAs(t, h, tokens["alice"], `{"index":"pages","query":"rocket","limit":1}`)
	if code != 200 || !first.HasMore || len(first.Hits) != 1 {
		t.Fatalf("first page: %d %s", code, body)
	}
	page := func(token string) (int, string) {
		code, _, body := searchAs(t, h, token, fmt.Sprintf(`{"index":"pages","query":"rocket","limit":1,"offset":1,"expectedGeneration":%q}`, first.Generation))
		return code, body
	}
	bobPage, err := app.FindRecordById("pages", "pagebravo000001")
	if err != nil {
		t.Fatal(err)
	}
	bobPage.Set("body", "bob rocket rocket rocket")
	if err := app.Save(bobPage); err != nil {
		t.Fatal(err)
	}
	if code, body := page(tokens["alice"]); code != 200 || strings.Contains(body, "bravo") {
		t.Fatalf("invisible change: %d %s", code, body)
	}
	alicePage, err := app.FindRecordById("pages", "pagealpha000002")
	if err != nil {
		t.Fatal(err)
	}
	alicePage.Set("body", "rocket changed")
	if err := app.Save(alicePage); err != nil {
		t.Fatal(err)
	}
	if code, body := page(tokens["alice"]); code != 409 || strings.Contains(body, "page") {
		t.Fatalf("visible change: %d %s", code, body)
	}
	for _, req := range []string{
		`{"index":"published","query":"rocket"}`,
		`{"index":"pages","query":"rocket","scope":"publicalpha0001"}`,
		`{"index":"pages","query":"rocket","offset":1}`,
		`{"index":"pages","query":"rocket","requester":"x"}`,
	} {
		if code, _, body := searchAs(t, h, tokens["alice"], req); code != 400 {
			t.Fatalf("%s: %d %s", req, code, body)
		}
	}
	schema := request(h, "GET", "/api/context/schema", tokens["alice"], "").Body.String()
	if !strings.Contains(schema, `"isolation":"private"`) || !strings.Contains(schema, `"maxIndexBytes":8388608`) || !strings.Contains(schema, `"indexes":["pages","published","visible"]`) {
		t.Fatalf("discovery: %s", schema)
	}
	for _, secret := range []string{"space_members", ":requester", "Unlisted", "private_search", "where"} {
		if strings.Contains(schema, secret) {
			t.Fatalf("discovery leaks %s: %s", secret, schema)
		}
	}
	for _, token := range []string{"", "invalid"} {
		if r := request(h, "POST", "/api/context/search", token, `{"index":"pages","query":"rocket"}`); r.Code != 401 && r.Code != 403 {
			t.Fatalf("unauthenticated: %d %s", r.Code, r.Body)
		}
	}
	// Revocation takes effect on the next request despite cached indexes.
	if err := app.Delete(records["alice_s1"]); err != nil {
		t.Fatal(err)
	}
	if code, _, body := searchAs(t, h, tokens["alice"], `{"index":"published","query":"rocket","scope":"publicalpha0001"}`); code != 404 || body != missing {
		t.Fatalf("scoped revocation: %d %s", code, body)
	}
	if code, r, body := searchAs(t, h, tokens["alice"], `{"index":"pages","query":"rocket"}`); code != 200 || len(r.Hits) != 0 {
		t.Fatalf("scopeless revocation: %d %s", code, body)
	}
	if code, r, body := searchAs(t, h, tokens["erin"], `{"index":"published","query":"rocket","scope":"publicalpha0001"}`); code != 200 || len(r.Hits) != 2 {
		t.Fatalf("erin unaffected: %d %s", code, body)
	}
}

func TestPrivateSearchHTTPLimits(t *testing.T) {
	app, cfg, tokens, _ := privateSearchFixture(t)
	cfg.Search.MaxIndexBytes = 1024
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if code, _, body := searchAs(t, h, tokens["alice"], `{"index":"pages","query":"rocket"}`); code != 200 {
		t.Fatalf("before: %d %s", code, body)
	}
	page, err := app.FindRecordById("pages", "pagealpha000003")
	if err != nil {
		t.Fatal(err)
	}
	page.Set("title", "Large")
	page.Set("body", strings.Repeat("rocket ", 200))
	if err := app.Save(page); err != nil {
		t.Fatal(err)
	}
	code, _, body := searchAs(t, h, tokens["alice"], `{"index":"pages","query":"rocket"}`)
	if code != 413 || !strings.Contains(body, "Authorized search content exceeds configured limits") || strings.Contains(body, "page") {
		t.Fatalf("index limit: %d %s", code, body)
	}
	if code, _, body := searchAs(t, h, tokens["bob"], `{"index":"pages","query":"rocket"}`); code != 200 {
		t.Fatalf("bob: %d %s", code, body)
	}
}

func TestPrivateSearchHTTPRejectsConfiguration(t *testing.T) {
	for _, name := range []string{"private in shared mode", "shared in snapshot mode", "table", "generation", "policy source", "unlisted source", "unconfigured column", "hidden column", "unconfigured scope field", "hidden scope field", "policy scope", "bad where", "where binds", "where unlisted column", "members without scope", "limits in shared mode", "warm scopes without background", "warm scopes in shared mode", "bad limits"} {
		t.Run(name, func(t *testing.T) {
			app, cfg, _, _ := privateSearchFixture(t)
			idx := cfg.Search.Indexes["published"]
			switch name {
			case "private in shared mode":
				cfg.Snapshot = nil
			case "shared in snapshot mode":
				idx.Isolation, idx.Scope, idx.Table = "", nil, "pages_search"
			case "table":
				idx.Table = "pages_search"
			case "generation":
				idx.Generation = &searchread.Generation{Collection: "publications", Record: "publicalpha0001", Field: "space"}
			case "policy source":
				idx.Collection, idx.Columns, idx.SnippetColumn = "space_members", []string{"space"}, "space"
			case "unlisted source":
				idx.Collection, idx.Columns, idx.SnippetColumn = "deals", []string{"title"}, "title"
			case "unconfigured column":
				idx.Columns, idx.SnippetColumn, idx.Weights = []string{"title", "created"}, "title", nil
			case "hidden column":
				cfg.Tables["pages"] = append(cfg.Tables["pages"], "notes")
				idx.Columns, idx.SnippetColumn, idx.Weights = []string{"notes"}, "notes", nil
			case "unconfigured scope field":
				idx.Scope.Field = "updated"
			case "hidden scope field":
				cfg.Tables["publications"] = append(cfg.Tables["publications"], "private")
				idx.Scope.Field = "private"
			case "policy scope":
				idx.Scope = &searchread.Scope{Collection: "space_members", Field: "space"}
			case "bad where":
				idx.Where = "1); DELETE FROM pages; --"
			case "where binds":
				idx.Where = "pages.title = :title"
			case "where unlisted column":
				idx.Where = "EXISTS (SELECT 1 FROM space_members m WHERE m.created > '')"
			case "members without scope":
				idx.Scope, idx.Members = nil, "all"
			case "warm scopes without background":
				idx.SnapshotScopeWhere = "publications.space = 'alpha'"
			case "warm scopes in shared mode":
				cfg.Snapshot = nil
				idx.Isolation, idx.Members, idx.Where = "", "", ""
				idx.SnapshotScopeWhere = "publications.space = 'alpha'"
			case "limits in shared mode":
				cfg.Snapshot = nil
				cfg.Search = &searchread.Config{Indexes: map[string]searchread.Index{"deals": {Table: "deal_search", Collection: "pages", Columns: []string{"title"}, SnippetColumn: "title"}}, CacheBytes: 1 << 20}
			case "bad limits":
				cfg.Search.MaxIndexBytes = searchread.MaxMaxIndexBytes + 1
			}
			if cfg.Search != nil && name != "limits in shared mode" {
				cfg.Search.Indexes["published"] = idx
			}
			if _, err := startConfiguredRouter(t, app, cfg); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}

// Configuration shapes used by existing applications keep loading unchanged.
func TestExistingConfigurationShapesLoad(t *testing.T) {
	tracing := `"tracing":{"enabled":true,"service":"synthetic","delivery":"buffer","captureSql":true,"maxBytes":16777216}`
	for name, text := range map[string]string{
		"shared": `{"authCollection":"users","tables":{"deals":["id","title"]},"timeoutMs":2000,"maxRows":500,"maxBytes":1048576}`,
		"shared scoped search": `{"authCollection":"users","tables":{"page_revisions":["id","title","body"],"publications":["id","manifest"],"search_state":["id","generation"]},` + tracing + `,
			"search":{"indexes":{"pages":{"table":"published_pages_fts","collection":"page_revisions","columns":["title","body"],"snippetColumn":"body","weights":[8,1],"scope":{"collection":"publications","field":"manifest"},"generation":{"collection":"search_state","record":"pagesindexstate","field":"generation"}}}}}`,
		"snapshot with policy": `{"authCollection":"users","tables":{"claims":["id","owner","amount"],"user_directory":["id","name"]},"timeoutMs":2000,"maxRows":500,"maxBytes":1048576,` + tracing + `,
			"snapshot":{"filters":{"claims":"owner = :requester OR EXISTS (SELECT 1 FROM finance_members f WHERE f.account = :requester)","user_directory":"id = :requester"},"policyTables":{"finance_members":["account"]},"timeoutMs":5000,"maxRows":100000,"maxBytes":16777216,"maxConcurrent":4}}`,
		"snapshot without policy": `{"authCollection":"users","tables":{"user_directory":["id","name"]},"snapshot":{"filters":{"user_directory":"1 = 1"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pocketcontext.json")
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Search != nil {
				if cfg.Search.MaxIndexBytes != 0 || cfg.Search.CacheBytes != 0 {
					t.Fatalf("shared search gained private limits: %+v", cfg.Search)
				}
				// Shared index JSON feeds the generation token; it must not change.
				encoded, _ := json.Marshal(cfg.Search.Indexes["pages"])
				for _, key := range []string{"isolation", "members", "where"} {
					if strings.Contains(string(encoded), key) {
						t.Fatalf("shared index encoding changed: %s", encoded)
					}
				}
				if discovery, _ := json.Marshal(searchDiscovery(*cfg.Search, cfg.MaxRows, false)); strings.Contains(string(discovery), "isolation") || strings.Contains(string(discovery), "maxIndexBytes") {
					t.Fatalf("shared discovery changed: %s", discovery)
				}
			}
		})
	}
}
