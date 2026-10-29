package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func snapshotFixture(t *testing.T) (*tests.TestApp, Config, map[string]string, map[string]*core.Record) {
	t.Helper()
	app, _, outsider := fixture(t)
	auth, err := app.FindCollectionByNameOrId("agents")
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{"outsider": outsider}
	people := map[string]*core.Record{}
	for _, name := range []string{"hr", "alice", "bob", "carol", "david"} {
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
		tokens[name], people[name] = token, r
	}
	for _, spec := range []struct {
		name   string
		fields []core.Field
	}{
		{"compensation", []core.Field{&core.TextField{Name: "employee"}, &core.TextField{Name: "name"}, &core.NumberField{Name: "salary"}, &core.TextField{Name: "medical", Hidden: true}}},
		{"management_chain", []core.Field{&core.TextField{Name: "manager"}, &core.TextField{Name: "employee"}}},
		{"hr_members", []core.Field{&core.TextField{Name: "user"}}},
	} {
		c := core.NewBaseCollection(spec.name)
		c.Fields.Add(spec.fields...)
		if err := app.Save(c); err != nil {
			t.Fatal(err)
		}
	}
	save := func(table string, values map[string]any) *core.Record {
		c, err := app.FindCollectionByNameOrId(table)
		if err != nil {
			t.Fatal(err)
		}
		r := core.NewRecord(c)
		for k, v := range values {
			r.Set(k, v)
		}
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	for name, salary := range map[string]int{"alice": 150, "bob": 100, "carol": 80, "david": 900} {
		save("compensation", map[string]any{"employee": people[name].Id, "name": name, "salary": salary, "medical": "private medical"})
	}
	for _, pair := range [][2]string{{"alice", "bob"}, {"alice", "carol"}, {"bob", "carol"}} {
		r := save("management_chain", map[string]any{"manager": people[pair[0]].Id, "employee": people[pair[1]].Id})
		people[pair[0]+"_"+pair[1]] = r
	}
	people["hr_membership"] = save("hr_members", map[string]any{"user": people["hr"].Id})
	cfg := Config{AuthCollection: "agents", Tables: map[string][]string{"compensation": {"employee", "name", "salary"}}, TimeoutMS: 1000, MaxRows: 100, MaxBytes: 4096, Snapshot: &sqlread.SnapshotConfig{
		Filters:      map[string]string{"compensation": `EXISTS (SELECT 1 FROM hr_members h WHERE h.user = :requester) OR EXISTS (SELECT 1 FROM management_chain m WHERE m.manager = :requester AND m.employee = compensation.employee)`},
		PolicyTables: map[string][]string{"hr_members": {"user"}, "management_chain": {"manager", "employee"}},
	}}
	return app, cfg, tokens, people
}

func TestSnapshotHTTPIsolationAndRevocation(t *testing.T) {
	app, cfg, tokens, people := snapshotFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	query := func(name, sql, format string) *httpResponse {
		body, _ := json.Marshal(map[string]string{"sql": sql, "format": format})
		r := request(h, "POST", "/api/context/query", tokens[name], string(body))
		return &httpResponse{r.Code, r.Body.String(), r.Header()}
	}
	for _, tc := range []struct {
		name  string
		count int
		total float64
	}{{"hr", 4, 1230}, {"alice", 2, 180}, {"bob", 1, 80}, {"carol", 0, 0}, {"david", 0, 0}} {
		r := query(tc.name, "SELECT count(*) AS n, coalesce(sum(salary),0) AS total FROM compensation", "")
		var result sqlread.Result
		if err := json.Unmarshal([]byte(r.body), &result); err != nil {
			t.Fatal(err)
		}
		if r.code != 200 || len(result.Rows) != 1 || result.Rows[0][0] != float64(tc.count) || result.Rows[0][1] != tc.total {
			t.Fatalf("%s: %d %s", tc.name, r.code, r.body)
		}
		if r.header.Get("X-Context-Scope") != "authorized-snapshot" || r.header.Get("Cache-Control") != "no-store" || !strings.Contains(r.header.Get("Access-Control-Expose-Headers"), "X-Context-Snapshot-At") {
			t.Fatalf("headers: %v", r.header)
		}
		if _, err := time.Parse(time.RFC3339Nano, r.header.Get("X-Context-Snapshot-At")); err != nil {
			t.Fatal(err)
		}
	}
	csv := query("alice", "SELECT name,salary FROM compensation ORDER BY name", "csv")
	if csv.code != 200 || csv.body != "name,salary\nbob,100\ncarol,80\n" || csv.header.Get("X-Context-Scope") != "authorized-snapshot" {
		t.Fatalf("csv: %+v", csv)
	}
	schema := request(h, "GET", "/api/context/schema", tokens["alice"], "")
	if schema.Code != 200 || schema.Header().Get("Cache-Control") != "no-store" || !strings.Contains(schema.Body.String(), `"permissionModel":"filtered-snapshot"`) || !strings.Contains(schema.Body.String(), `"snapshotLimits"`) {
		t.Fatalf("schema: %d %s", schema.Code, schema.Body)
	}
	for _, secret := range []string{"hr_members", "management_chain", "medical", ":requester", "filters"} {
		if strings.Contains(schema.Body.String(), secret) {
			t.Fatalf("schema leaks %s: %s", secret, schema.Body)
		}
	}
	for _, sql := range []string{"SELECT * FROM hr_members", "SELECT * FROM management_chain", "SELECT medical FROM compensation", "SELECT * FROM agents", "SELECT * FROM sqlite_master", "ATTACH ':memory:' AS source", "UPDATE compensation SET salary=1", "SELECT load_extension('anything')"} {
		r := query("alice", sql, "")
		if r.code != 400 {
			t.Fatalf("%s: %d %s", sql, r.code, r.body)
		}
	}
	for _, name := range []string{"outsider", "anonymous"} {
		for _, path := range []string{"/api/context/query", "/api/context/schema"} {
			method := "POST"
			if strings.HasSuffix(path, "schema") {
				method = "GET"
			}
			r := request(h, method, path, tokens[name], `{"sql":"SELECT * FROM compensation"}`)
			if r.Code != 401 && r.Code != 403 {
				t.Fatalf("auth %s: %d %s", name, r.Code, r.Body)
			}
		}
	}
	spoof := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT * FROM compensation","requester":"`+people["hr"].Id+`"}`)
	if spoof.Code != 400 {
		t.Fatalf("spoof: %d %s", spoof.Code, spoof.Body)
	}
	if err := app.Delete(people["alice_bob"]); err != nil {
		t.Fatal(err)
	}
	if err := app.Delete(people["alice_carol"]); err != nil {
		t.Fatal(err)
	}
	r := query("alice", "SELECT name FROM compensation", "")
	if r.code != 200 || strings.Contains(r.body, "bob") || strings.Contains(r.body, "carol") {
		t.Fatalf("revocation: %+v", r)
	}
	if err := app.Delete(people["hr_membership"]); err != nil {
		t.Fatal(err)
	}
	r = query("hr", "SELECT name FROM compensation", "")
	if r.code != 200 || strings.Contains(r.body, "david") {
		t.Fatalf("HR revocation: %+v", r)
	}
}

type httpResponse struct {
	code   int
	body   string
	header http.Header
}

func TestSnapshotHTTPRejectsUnsafeConfiguration(t *testing.T) {
	for _, name := range []string{"implicit output", "missing filter", "extra filter", "implicit policy", "overlap", "hidden output", "hidden policy", "auth policy", "system policy", "bad limits", "invalid filter", "unlisted policy column"} {
		t.Run(name, func(t *testing.T) {
			app, cfg, _, _ := snapshotFixture(t)
			switch name {
			case "implicit output":
				cfg.Tables["compensation"] = nil
			case "missing filter":
				delete(cfg.Snapshot.Filters, "compensation")
			case "extra filter":
				cfg.Snapshot.Filters["deals"] = "1"
			case "implicit policy":
				cfg.Snapshot.PolicyTables["hr_members"] = nil
			case "overlap":
				cfg.Snapshot.PolicyTables["compensation"] = []string{"name"}
			case "hidden output":
				cfg.Tables["compensation"] = []string{"medical"}
			case "hidden policy":
				cfg.Snapshot.PolicyTables["deals"] = []string{"secret"}
			case "auth policy":
				cfg.Snapshot.PolicyTables["agents"] = []string{"id"}
			case "system policy":
				cfg.Snapshot.PolicyTables["_collections"] = []string{"id"}
			case "bad limits":
				cfg.Snapshot.MaxRows = -1
			case "invalid filter":
				cfg.Snapshot.Filters["compensation"] = "1; SELECT 1"
			case "unlisted policy column":
				cfg.Snapshot.Filters["compensation"] = "EXISTS (SELECT id FROM hr_members)"
			}
			if _, err := startConfiguredRouter(t, app, cfg); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}

func TestSnapshotHTTPLimitFailsWithoutPartialResults(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	cfg.Snapshot.MaxRows = 1
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name,salary FROM compensation LIMIT 1"}`)
	if r.Code != 413 || strings.Contains(r.Body.String(), "bob") || strings.Contains(r.Body.String(), "carol") || strings.Contains(r.Body.String(), `"rows"`) {
		t.Fatalf("partial export: %d %s", r.Code, r.Body)
	}
	r = request(h, "POST", "/api/context/query", tokens["bob"], `{"sql":"SELECT name FROM compensation"}`)
	if r.Code != 200 || !strings.Contains(r.Body.String(), "carol") {
		t.Fatalf("after limit failure: %d %s", r.Code, r.Body)
	}
}

func TestSnapshotHTTPBuildFailureIsGeneric(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// A later collection change invalidates the trusted export. It must never
	// return an internal source query or details as a client SQL rejection.
	collection, err := app.FindCollectionByNameOrId("hr_members")
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Delete(collection); err != nil {
		t.Fatal(err)
	}
	r := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation"}`)
	if r.Code != 500 || !strings.Contains(r.Body.String(), "Cannot build authorized snapshot") {
		t.Fatalf("build failure: %d %s", r.Code, r.Body)
	}
	for _, secret := range []string{"hr_members", "management_chain", ":requester", "EXISTS", "bob", "carol"} {
		if strings.Contains(r.Body.String(), secret) {
			t.Fatalf("build failure leaks %s: %s", secret, r.Body)
		}
	}
}

func TestSnapshotHTTPRequesterConcurrency(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	cfg.Snapshot.MaxConcurrent = 2
	cfg.Snapshot.MaxConcurrentPerRequester = 1
	cfg.TimeoutMS = 1500
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httpResponse, 4)
	for range 4 {
		go func() {
			r := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x) SELECT sum(n) FROM x"}`)
			done <- &httpResponse{r.Code, r.Body.String(), r.Header()}
		}()
	}
	// Three prompt rejections establish that one slow request owns admission;
	// polling with an ordinary same-account query could itself win admission.
	for range 3 {
		select {
		case r := <-done:
			if r.code != 429 || r.header.Get("Retry-After") != "1" {
				t.Fatalf("expected requester 429 with retry hint: %+v", r)
			}
		case <-time.After(time.Second):
			t.Fatal("excess requester work was not promptly rejected")
		}
	}
	r := request(h, "POST", "/api/context/query", tokens["bob"], `{"sql":"SELECT name FROM compensation"}`)
	if r.Code != 200 || !strings.Contains(r.Body.String(), "carol") || strings.Contains(r.Body.String(), "bob") {
		t.Fatalf("other account: %d %s", r.Code, r.Body)
	}
	if r := <-done; r.code != 408 {
		t.Fatalf("slow query: %+v", r)
	}
	r = request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation"}`)
	if r.Code != 200 {
		t.Fatalf("admission not released: %d %s", r.Code, r.Body)
	}
}
