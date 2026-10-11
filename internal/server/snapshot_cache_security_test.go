package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

// Exercise writes through the application's record layer after repeated reads,
// including policy grants, revocations and auth-token revocation. A cache must
// preserve the same fresh-read contract as the uncached endpoint.
func TestSnapshotCacheHTTPCommittedChangesAndAuthentication(t *testing.T) {
	app, cfg, tokens, people := snapshotFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	query := func(token, sql string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		return request(h, "POST", "/api/context/query", token, string(body))
	}
	assertTotal := func(name string, total float64) {
		t.Helper()
		// Repetition warms any cache without depending on its internal structure.
		var snapshotAt string
		for range 3 {
			r := query(tokens[name], "SELECT coalesce(sum(salary),0) FROM compensation")
			var result sqlread.Result
			if err := json.Unmarshal(r.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if r.Code != http.StatusOK || len(result.Rows) != 1 || result.Rows[0][0] != total {
				t.Fatalf("%s: want total %v, got %d %s", name, total, r.Code, r.Body)
			}
			at := r.Header().Get("X-Context-Snapshot-At")
			if at == "" || (snapshotAt != "" && at != snapshotAt) {
				t.Fatalf("unchanged source did not reuse %s snapshot: %q -> %q", name, snapshotAt, at)
			}
			snapshotAt = at
		}
	}
	assertTotal("alice", 180)
	assertTotal("bob", 80)
	assertTotal("david", 0)
	rows, err := app.FindRecordsByFilter("compensation", "name = 'bob'", "", 1, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("fixture lookup: %v", err)
	}
	rows[0].Set("salary", 120)
	if err := app.Save(rows[0]); err != nil {
		t.Fatal(err)
	}
	assertTotal("alice", 200)
	assertTotal("bob", 80)
	if err := app.Delete(people["alice_bob"]); err != nil {
		t.Fatal(err)
	}
	assertTotal("alice", 80)
	collection, err := app.FindCollectionByNameOrId("management_chain")
	if err != nil {
		t.Fatal(err)
	}
	grant := core.NewRecord(collection)
	grant.Set("manager", people["david"].Id)
	grant.Set("employee", people["bob"].Id)
	if err := app.Save(grant); err != nil {
		t.Fatal(err)
	}
	assertTotal("david", 120)
	if err := app.Delete(grant); err != nil {
		t.Fatal(err)
	}
	assertTotal("david", 0)
	people["alice"].RefreshTokenKey()
	if err := app.Save(people["alice"]); err != nil {
		t.Fatal(err)
	}
	r := query(tokens["alice"], "SELECT name FROM compensation")
	if r.Code != http.StatusUnauthorized && r.Code != http.StatusForbidden {
		t.Fatalf("revoked auth reused a snapshot: %d %s", r.Code, r.Body)
	}
	tokens["alice"], err = people["alice"].NewAuthToken()
	if err != nil {
		t.Fatal(err)
	}
	assertTotal("alice", 80)
}

func TestSnapshotCacheHTTPClockPolicyExpiresWithoutWrite(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	// Quoting the function name catches lexical shortcuts in cache eligibility
	// detection. The wall clock changes authorization without any data commit.
	expires := time.Now().Add(3 * time.Second).Unix()
	cfg.Snapshot.Filters["compensation"] = fmt.Sprintf("(%s) AND \"unixepoch\"() < %d", cfg.Snapshot.Filters["compensation"], expires)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	count := func(want float64) {
		t.Helper()
		r := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT count(*) FROM compensation"}`)
		var result sqlread.Result
		if err := json.Unmarshal(r.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if r.Code != http.StatusOK || len(result.Rows) != 1 || result.Rows[0][0] != want {
			t.Fatalf("clock policy: want %v rows, got %d %s", want, r.Code, r.Body)
		}
	}
	count(2)
	count(2)
	time.Sleep(time.Until(time.Unix(expires, 0)) + 20*time.Millisecond)
	count(0)
}

func TestSnapshotCacheHTTPWarmReaderStillRestrictsSQL(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"PRAGMA data_version", "SELECT * FROM management_chain",
		"SELECT medical FROM compensation", "SELECT * FROM agents",
		"SELECT * FROM sqlite_master", "ATTACH ':memory:' AS source",
		"UPDATE compensation SET salary=0", "SELECT load_extension('anything')",
	} {
		warm := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation"}`)
		if warm.Code != http.StatusOK {
			t.Fatalf("warm: %d %s", warm.Code, warm.Body)
		}
		body, _ := json.Marshal(map[string]string{"sql": sql})
		r := request(h, "POST", "/api/context/query", tokens["alice"], string(body))
		if r.Code != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", sql, r.Code, r.Body)
		}
	}
}
