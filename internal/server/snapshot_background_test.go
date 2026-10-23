package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/searchread"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func TestBackgroundSnapshotHTTPMetadataFreshnessAndIsolation(t *testing.T) {
	app, cfg, tokens, people := snapshotFixture(t)
	cfg.Snapshot.Background = &sqlread.BackgroundSnapshotConfig{
		MaxDiskBytes: 64 << 20, MinFreeBytes: 1, BuildTimeoutMS: 10000,
		RefreshIntervalMS: 60000, RetryDelayMS: 10, MaxCachedUsers: 8,
	}
	// These still limit trusted filtered reads, but not background construction.
	cfg.Snapshot.MaxRows, cfg.Snapshot.MaxBytes = 1, 1024
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	type response struct {
		sqlread.Result
		Snapshot sqlread.SnapshotMetadata `json:"snapshot"`
	}
	read := func(name string, options map[string]string, total float64) response {
		t.Helper()
		body := map[string]string{"sql": "SELECT coalesce(sum(salary),0) FROM compensation"}
		for key, value := range options {
			body[key] = value
		}
		encoded, _ := json.Marshal(body)
		r := request(h, http.MethodPost, "/api/context/query", tokens[name], string(encoded))
		var result response
		if err := json.Unmarshal(r.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if r.Code != http.StatusOK || len(result.Rows) != 1 || result.Rows[0][0] != total {
			t.Fatalf("%s query: expected %v, got %d %s", name, total, r.Code, r.Body)
		}
		if result.Snapshot.Generation == "" || result.Snapshot.CapturedAt.IsZero() || result.Snapshot.DiskBytes <= 0 {
			t.Fatalf("missing completed snapshot metadata: %+v", result.Snapshot)
		}
		if r.Header().Get("X-Context-Snapshot-Generation") != result.Snapshot.Generation || r.Header().Get("X-Context-Scope") != "authorized-snapshot" || r.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("snapshot headers: %v", r.Header())
		}
		for _, header := range []string{"X-Context-Snapshot-Generation", "X-Context-Snapshot-At", "X-Context-Snapshot-Stale", "X-Context-Snapshot-Refreshing"} {
			if !strings.Contains(strings.Join(r.Header().Values("Access-Control-Expose-Headers"), ","), header) {
				t.Fatalf("snapshot header is not exposed to browser: %s", header)
			}
		}
		return result
	}
	first := read("alice", map[string]string{"consistency": "fresh"}, 180)
	if first.Snapshot.Rows != 2 {
		t.Fatalf("snapshot rows are not exported rows: %+v", first.Snapshot)
	}
	status := request(h, http.MethodGet, "/api/context/snapshot?requester="+people["hr"].Id, tokens["alice"], "")
	var current sqlread.SnapshotMetadata
	if err := json.Unmarshal(status.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if status.Code != http.StatusOK || current.Generation != first.Snapshot.Generation || current.Rows != 2 || status.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("requester status: %d %s", status.Code, status.Body)
	}
	bob := read("bob", map[string]string{"consistency": "fresh"}, 80)
	if bob.Snapshot.Generation == first.Snapshot.Generation {
		t.Fatal("different requesters share a generation token")
	}
	read("alice", map[string]string{"consistency": "available", "atLeast": first.Snapshot.Generation}, 180)
	csvBody, _ := json.Marshal(map[string]string{"sql": "SELECT name FROM compensation ORDER BY name", "format": "csv", "expectedGeneration": first.Snapshot.Generation})
	csv := request(h, http.MethodPost, "/api/context/query", tokens["alice"], string(csvBody))
	if csv.Code != http.StatusOK || csv.Body.String() != "name\nbob\ncarol\n" || csv.Header().Get("X-Context-Snapshot-Generation") != first.Snapshot.Generation {
		t.Fatalf("CSV snapshot metadata: %d %s %v", csv.Code, csv.Body, csv.Header())
	}
	schema := request(h, http.MethodGet, "/api/context/schema", tokens["alice"], "")
	if schema.Code != http.StatusOK || !strings.Contains(schema.Body.String(), `"mode":"background"`) || !strings.Contains(schema.Body.String(), `"defaultConsistency":"available"`) {
		t.Fatalf("background discovery: %d %s", schema.Code, schema.Body)
	}
	for _, secret := range []string{"management_chain", "hr_members", "medical", ":requester", "filters"} {
		if strings.Contains(schema.Body.String(), secret) || strings.Contains(status.Body.String(), secret) {
			t.Fatalf("snapshot discovery leaks %q", secret)
		}
	}
	rows, err := app.FindRecordsByFilter("compensation", "name = 'bob'", "", 1, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("fixture lookup: %v", err)
	}
	rows[0].Set("salary", 200)
	if err := app.Save(rows[0]); err != nil {
		t.Fatal(err)
	}
	next := read("alice", map[string]string{"consistency": "fresh"}, 280)
	if next.Snapshot.Generation == first.Snapshot.Generation {
		t.Fatal("fresh read after write did not advance generation")
	}
	for _, fields := range []map[string]string{{"expectedGeneration": first.Snapshot.Generation}, {"atLeast": bob.Snapshot.Generation}} {
		fields["sql"] = "SELECT name FROM compensation"
		body, _ := json.Marshal(fields)
		r := request(h, http.MethodPost, "/api/context/query", tokens["alice"], string(body))
		if r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), `"code":"snapshot_generation_conflict"`) {
			t.Fatalf("generation conflict: %d %s", r.Code, r.Body)
		}
	}
	for _, token := range []string{"", tokens["outsider"]} {
		r := request(h, http.MethodGet, "/api/context/snapshot", token, "")
		if r.Code != http.StatusUnauthorized && r.Code != http.StatusForbidden {
			t.Fatalf("status authentication: %d %s", r.Code, r.Body)
		}
	}
	people["alice"].RefreshTokenKey()
	if err := app.Save(people["alice"]); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/context/query", "/api/context/snapshot"} {
		method, body := http.MethodGet, ""
		if strings.HasSuffix(path, "/query") {
			method, body = http.MethodPost, `{"sql":"SELECT name FROM compensation"}`
		}
		r := request(h, method, path, tokens["alice"], body)
		if r.Code != http.StatusUnauthorized && r.Code != http.StatusForbidden {
			t.Fatalf("revoked token on %s: %d %s", path, r.Code, r.Body)
		}
	}
}

func TestSnapshotHTTPOptionsKeepLegacyContract(t *testing.T) {
	t.Run("shared", func(t *testing.T) {
		app, token, _ := fixture(t)
		h, err := startRouter(t, app, map[string][]string{"deals": {"title"}})
		if err != nil {
			t.Fatal(err)
		}
		r := request(h, http.MethodPost, "/api/context/query", token, `{"sql":"SELECT 1","consistency":"fresh"}`)
		if r.Code != http.StatusBadRequest {
			t.Fatalf("shared snapshot options: %d %s", r.Code, r.Body)
		}
		if r := request(h, http.MethodGet, "/api/context/snapshot", token, ""); r.Code != http.StatusNotFound {
			t.Fatalf("shared snapshot status: %d %s", r.Code, r.Body)
		}
	})
	t.Run("synchronous", func(t *testing.T) {
		app, cfg, tokens, _ := snapshotFixture(t)
		h, err := startConfiguredRouter(t, app, cfg)
		if err != nil {
			t.Fatal(err)
		}
		r := request(h, http.MethodPost, "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation","consistency":"fresh"}`)
		if r.Code != http.StatusOK || strings.Contains(r.Body.String(), `"snapshot":`) || r.Header().Get("X-Context-Snapshot-Generation") != "" {
			t.Fatalf("legacy response changed: %d %s %v", r.Code, r.Body, r.Header())
		}
		for _, option := range []string{`"consistency":"unknown"`, `"atLeast":"unknown"`, `"expectedGeneration":"unknown"`} {
			r := request(h, http.MethodPost, "/api/context/query", tokens["alice"], `{"sql":"SELECT 1",`+option+`}`)
			if r.Code != http.StatusBadRequest {
				t.Fatalf("legacy option %s: %d %s", option, r.Code, r.Body)
			}
		}
		if r := request(h, http.MethodGet, "/api/context/snapshot", tokens["alice"], ""); r.Code != http.StatusNotFound {
			t.Fatalf("legacy snapshot status: %d %s", r.Code, r.Body)
		}
	})
}

func TestBackgroundSnapshotHTTPStorageFailureKeepsAvailableGeneration(t *testing.T) {
	app, cfg, tokens, people := snapshotFixture(t)
	cfg.Snapshot.Background = &sqlread.BackgroundSnapshotConfig{
		MaxDiskBytes: 65536, MinFreeBytes: 1, BuildTimeoutMS: 5000,
		RefreshIntervalMS: 60000, RetryDelayMS: 60000,
	}
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	fresh := `{"sql":"SELECT count(*) FROM compensation","consistency":"fresh"}`
	first := request(h, http.MethodPost, "/api/context/query", tokens["alice"], fresh)
	if first.Code != http.StatusOK {
		t.Fatalf("initial generation: %d %s", first.Code, first.Body)
	}
	generation := first.Header().Get("X-Context-Snapshot-Generation")
	collection, err := app.FindCollectionByNameOrId("compensation")
	if err != nil {
		t.Fatal(err)
	}
	if err := app.RunInTransaction(func(tx core.App) error {
		for range 50 {
			record := core.NewRecord(collection)
			record.Set("employee", people["bob"].Id)
			record.Set("name", strings.Repeat("synthetic ", 200))
			record.Set("salary", 1)
			if err := tx.Save(record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	failed := request(h, http.MethodPost, "/api/context/query", tokens["alice"], fresh)
	if failed.Code != http.StatusServiceUnavailable || !strings.Contains(failed.Body.String(), `"code":"snapshot_resource_limit"`) || failed.Header().Get("Retry-After") != "1" {
		t.Fatalf("storage exhaustion classification: %d %s", failed.Code, failed.Body)
	}
	available := request(h, http.MethodPost, "/api/context/query", tokens["alice"], `{"sql":"SELECT count(*) FROM compensation","consistency":"available"}`)
	var result struct {
		sqlread.Result
		Snapshot sqlread.SnapshotMetadata `json:"snapshot"`
	}
	if err := json.Unmarshal(available.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if available.Code != http.StatusOK || len(result.Rows) != 1 || result.Rows[0][0] != float64(2) || result.Snapshot.Generation != generation || !result.Snapshot.Stale || result.Snapshot.LastError != "storage_budget" {
		t.Fatalf("refresh failure lost previous generation: %d %s", available.Code, available.Body)
	}
	status := request(h, http.MethodGet, "/api/context/snapshot", tokens["alice"], "")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"lastError":"storage_budget"`) || strings.Contains(status.Body.String(), "synthetic") {
		t.Fatalf("storage failure status: %d %s", status.Code, status.Body)
	}
}

func TestSnapshotHTTPErrorCodes(t *testing.T) {
	app, _, _ := fixture(t)
	router, err := apis.NewRouter(app)
	if err != nil {
		t.Fatal(err)
	}
	var failure error
	router.GET("/snapshot-error", func(re *core.RequestEvent) error {
		handled, err := snapshotHTTPError(re, fmt.Errorf("private source detail: %w", failure))
		if !handled {
			return re.InternalServerError("unhandled error", nil)
		}
		return err
	})
	h, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{sqlread.ErrSnapshotPending, 503, "snapshot_pending"},
		{sqlread.ErrSnapshotResource, 503, "snapshot_resource_limit"},
		{sqlread.ErrSnapshotGeneration, 409, "snapshot_generation_conflict"},
	} {
		failure = tc.err
		r := request(h, http.MethodGet, "/snapshot-error", "", "")
		if r.Code != tc.status || !strings.Contains(r.Body.String(), tc.code) || strings.Contains(r.Body.String(), "private source detail") {
			t.Fatalf("%s mapping: %d %s", tc.code, r.Code, r.Body)
		}
		if tc.status == 503 && (r.Header().Get("Retry-After") != "1" || !strings.Contains(r.Header().Get("Access-Control-Expose-Headers"), "Retry-After")) {
			t.Fatalf("retry metadata: %v", r.Header())
		}
	}
}

func TestSearchHTTPSnapshotOptionsAndMetadata(t *testing.T) {
	app, token, _ := fixture(t)
	router, err := apis.NewRouter(app)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{AuthCollection: "agents", MaxBytes: 4096, Snapshot: &sqlread.SnapshotConfig{Background: &sqlread.BackgroundSnapshotConfig{}}, Search: &searchread.Config{Indexes: map[string]searchread.Index{"notes": {}}}}
	metadata := sqlread.SnapshotMetadata{Generation: "synthetic-generation", CapturedAt: time.Now().UTC(), Rows: 5, DiskBytes: 8192}
	var received searchread.Request
	registerSearchRoute(app, &core.ServeEvent{App: app, Router: router}, cfg, func(_ context.Context, _ string, req searchread.Request) (searchread.Result, error) {
		received = req
		return searchread.Result{Index: "notes", Hits: []searchread.Hit{}, Snapshot: &metadata}, nil
	})
	h, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	r := request(h, http.MethodPost, "/api/context/search", token, `{"index":"notes","query":"test","consistency":"fresh","atLeast":"minimum","snapshotGeneration":"exact","expectedGeneration":"fts"}`)
	if r.Code != http.StatusOK || received.Consistency != "fresh" || received.AtLeast != "minimum" || received.SnapshotGeneration != "exact" || received.ExpectedGeneration != "fts" {
		t.Fatalf("search options: %+v, %d %s", received, r.Code, r.Body)
	}
	if !strings.Contains(r.Body.String(), `"snapshot":`) || r.Header().Get("X-Context-Snapshot-Generation") != metadata.Generation {
		t.Fatalf("search metadata: %d %s %v", r.Code, r.Body, r.Header())
	}
}
