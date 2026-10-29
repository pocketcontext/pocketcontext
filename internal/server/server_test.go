package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ganigeorgiev.com/pocketbase/pocketbase/apis"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"github.com/pocketcontext/pocketcontext/internal/database"
)

func fixture(t *testing.T) (*tests.TestApp, string, string) {
	t.Helper()
	app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DataDir: t.TempDir(), DBConnect: database.Connect})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)
	tokens := []string{}
	for _, name := range []string{"agents", "outsiders"} {
		c := core.NewAuthCollection(name)
		if err := app.Save(c); err != nil {
			t.Fatal(err)
		}
		r := core.NewRecord(c)
		r.SetEmail(name + "@example.com")
		r.SetPassword("test-password-12345")
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		token, err := r.NewAuthToken()
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	c := core.NewBaseCollection("deals")
	rule := "@request.auth.collectionName = 'agents'"
	c.CreateRule = &rule
	c.UpdateRule = &rule
	c.ViewRule = &rule
	c.ListRule = &rule
	c.Fields.Add(&core.TextField{Name: "title", Required: true}, &core.TextField{Name: "secret", Hidden: true})
	if err := app.Save(c); err != nil {
		t.Fatal(err)
	}
	return app, tokens[0], tokens[1]
}
func startRouter(t *testing.T, app *tests.TestApp, tables map[string][]string) (http.Handler, error) {
	t.Helper()
	cfg := Config{AuthCollection: "agents", Tables: tables, TimeoutMS: 1000, MaxRows: 100, MaxBytes: 4096}
	return startConfiguredRouter(t, app, cfg)
}

func startConfiguredRouter(t *testing.T, app *tests.TestApp, cfg Config) (http.Handler, error) {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "context.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	Register(app, path)
	router, err := apis.NewRouter(app)
	if err != nil {
		return nil, err
	}
	event := &core.ServeEvent{App: app, Router: router}
	if err := app.OnServe().Trigger(event); err != nil {
		return nil, err
	}
	return router.BuildMux()
}
func request(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
func TestBaaSWritesAndSQLReads(t *testing.T) {
	app, token, other := fixture(t)
	h, err := startRouter(t, app, map[string][]string{"deals": {"id", "title"}})
	if err != nil {
		t.Fatal(err)
	}
	write := request(h, "POST", "/api/collections/deals/records", token, `{"title":"Acme renewal","secret":"private"}`)
	if write.Code != 200 {
		t.Fatalf("BaaS create: %d %s", write.Code, write.Body)
	}
	for _, test := range []struct{ name, token string }{{"anonymous", ""}, {"wrong collection", other}} {
		t.Run(test.name, func(t *testing.T) {
			for _, path := range []string{"/api/context/query", "/api/context/schema"} {
				method := "POST"
				if strings.HasSuffix(path, "schema") {
					method = "GET"
				}
				r := request(h, method, path, test.token, `{"sql":"SELECT title FROM deals"}`)
				if r.Code != 401 && r.Code != 403 {
					t.Fatalf("expected refusal: %d %s", r.Code, r.Body)
				}
			}
		})
	}
	schema := request(h, "GET", "/api/context/schema", token, "")
	if schema.Code != 200 || !strings.Contains(schema.Body.String(), `"title"`) || strings.Contains(schema.Body.String(), "secret") {
		t.Fatalf("schema: %d %s", schema.Code, schema.Body)
	}
	if !strings.Contains(schema.Body.String(), `"permissionModel":"shared"`) || strings.Contains(schema.Body.String(), "snapshotLimits") || schema.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("shared schema metadata: %s %v", schema.Body, schema.Header())
	}
	query := request(h, "POST", "/api/context/query", token, `{"sql":"SELECT title FROM deals"}`)
	if query.Code != 200 || !strings.Contains(query.Body.String(), "Acme renewal") {
		t.Fatalf("SQL: %d %s", query.Code, query.Body)
	}
	if query.Header().Get("X-Context-Scope") != "" || query.Header().Get("X-Context-Snapshot-At") != "" {
		t.Fatal("shared response incorrectly marked as snapshot")
	}
	csv := request(h, "POST", "/api/context/query", token, `{"sql":"SELECT title FROM deals","format":"csv"}`)
	if csv.Code != 200 || csv.Body.String() != "title\nAcme renewal\n" || csv.Header().Get("X-Context-Truncated") != "false" {
		t.Fatalf("CSV: %d %s", csv.Code, csv.Body)
	}
	for _, sql := range []string{"SELECT secret FROM deals", "SELECT * FROM agents", "UPDATE deals SET title='tampered'", "SELECT 1; SELECT 2"} {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		r := request(h, "POST", "/api/context/query", token, string(body))
		if r.Code != 400 {
			t.Fatalf("%q accepted: %d %s", sql, r.Code, r.Body)
		}
	}
	query = request(h, "POST", "/api/context/query", token, `{"sql":"SELECT title FROM deals"}`)
	if !strings.Contains(query.Body.String(), "Acme renewal") {
		t.Fatal("rejected mutation altered data")
	}
}
func TestRejectUnsafeTableConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tables map[string][]string
	}{
		{"auth", map[string][]string{"agents": {"id"}}},
		{"system", map[string][]string{"_collections": {"id"}}},
		{"implicit hidden", map[string][]string{"deals": nil}},
		{"explicit hidden", map[string][]string{"deals": {"secret"}}},
		{"case variant hidden", map[string][]string{"deals": {"SECRET"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, _, _ := fixture(t)
			if _, err := startRouter(t, app, tc.tables); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}
func TestNonReadOnlyStatementRejectedOverHTTP(t *testing.T) {
	app, token, _ := fixture(t)
	h, err := startRouter(t, app, map[string][]string{"deals": {"id", "title"}})
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "copy.db")
	body, _ := json.Marshal(map[string]string{"sql": "VACUUM INTO '" + out + "'"})
	r := request(h, "POST", "/api/context/query", token, string(body))
	if r.Code != http.StatusBadRequest || !strings.Contains(r.Body.String(), `"message":"SQL query rejected: statement is not read-only."`) {
		t.Fatalf("VACUUM INTO: %d %s", r.Code, r.Body)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("VACUUM INTO wrote %s: %v", out, err)
	}
}
