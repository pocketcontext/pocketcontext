package server

import (
	"encoding/json"
	"math"
	"net/http"
	"testing"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func TestSQLJSONAndPercentilesHTTP(t *testing.T) {
	app, token, outsider := fixture(t)
	collection, err := app.FindCollectionByNameOrId("deals")
	if err != nil {
		t.Fatal(err)
	}
	collection.Fields.Add(&core.JSONField{Name: "metrics"})
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}
	h, err := startRouter(t, app, map[string][]string{"deals": {"id", "title", "metrics"}})
	if err != nil {
		t.Fatal(err)
	}
	written := request(h, "POST", "/api/collections/deals/records", token,
		`{"title":"Synthetic measurements","metrics":{"durations":[10,20,90]},"secret":"[999]"}`)
	if written.Code != http.StatusOK {
		t.Fatalf("REST write: %d %s", written.Code, written.Body)
	}
	query := `SELECT median(j.value), percentile_cont(j.value,0.95) FROM deals, json_each(deals.metrics,'$.durations') AS j`
	body, _ := json.Marshal(map[string]string{"sql": query})
	for _, identity := range []string{"", outsider} {
		response := request(h, "POST", "/api/context/query", identity, string(body))
		if response.Code != http.StatusUnauthorized && response.Code != http.StatusForbidden {
			t.Fatalf("unauthorized extension query: %d %s", response.Code, response.Body)
		}
	}
	response := request(h, "POST", "/api/context/query", token, string(body))
	var result sqlread.Result
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || result.Truncated || len(result.Rows) != 1 || len(result.Rows[0]) != 2 {
		t.Fatalf("extension result: %d %s", response.Code, response.Body)
	}
	if result.Rows[0][0] != float64(20) || math.Abs(result.Rows[0][1].(float64)-83) > 1e-9 {
		t.Fatalf("incorrect percentiles: %s", response.Body)
	}
	body, _ = json.Marshal(map[string]string{
		"sql":    `SELECT j.atom AS duration FROM deals, json_tree(deals.metrics,'$.durations') AS j WHERE j.type='integer' ORDER BY j.atom`,
		"format": "csv",
	})
	response = request(h, "POST", "/api/context/query", token, string(body))
	if response.Code != http.StatusOK || response.Body.String() != "duration\n10\n20\n90\n" {
		t.Fatalf("JSON traversal CSV: %d %s", response.Code, response.Body)
	}
	for _, denied := range []string{
		`SELECT median(j.value) FROM deals, json_each(deals.secret) AS j`,
		`SELECT j.value FROM json_tree((SELECT json_group_array(email) FROM agents)) AS j`,
		`SELECT j.value FROM json_each((SELECT json_group_array(sql) FROM sqlite_schema)) AS j`,
		`SELECT median(value) FROM pragma_table_info('deals')`,
	} {
		body, _ := json.Marshal(map[string]string{"sql": denied})
		response := request(h, "POST", "/api/context/query", token, string(body))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", denied, response.Code, response.Body)
		}
	}
}

func TestSnapshotJSONPercentilesHTTP(t *testing.T) {
	app, cfg, tokens, people := snapshotFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	query := `SELECT median(j.value) FROM compensation, json_each(json_array(salary)) AS j`
	body, _ := json.Marshal(map[string]string{"sql": query})
	check := func(name string, want any) {
		t.Helper()
		response := request(h, "POST", "/api/context/query", tokens[name], string(body))
		var result sqlread.Result
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || response.Header().Get("X-Context-Scope") != "authorized-snapshot" || result.Truncated || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != want {
			t.Fatalf("%s median: %d %s (want %v)", name, response.Code, response.Body, want)
		}
	}
	check("hr", float64(125))
	check("alice", float64(90))
	check("bob", float64(80))
	check("carol", nil)
	if err := app.Delete(people["alice_bob"]); err != nil {
		t.Fatal(err)
	}
	check("alice", float64(80))
	if err := app.Delete(people["alice_carol"]); err != nil {
		t.Fatal(err)
	}
	check("alice", nil)
}
