package server

import (
	"bufio"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

func TestTracingAuthenticatedRequest(t *testing.T) {
	app, token, _ := fixture(t)
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	h, err := startConfiguredRouter(t, app, Config{AuthCollection: "agents", Tables: map[string][]string{"deals": {"id", "title"}}, TimeoutMS: 1000, MaxRows: 100, MaxBytes: 4096, Tracing: tracing.Config{Enabled: true, Path: path, Service: "test", CaptureSQL: true}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/api/context/query?secret=never-capture", strings.NewReader(`{"sql":"SELECT title FROM deals"}`))
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Context-Correlation-Id", "test-client-1")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != 200 || len(response.Header().Get("X-Context-Request-Id")) != 32 {
		t.Fatalf("response %d %v", response.Code, response.Header())
	}
	bad := request(h, "POST", "/api/context/query", token, `{"sql":"DELETE FROM deals"}`)
	if bad.Code != 400 {
		t.Fatal(bad.Code)
	}
	request(h, "GET", "/api/context/schema", "", "")
	head := request(h, "HEAD", "/api/context/schema", token, "")
	if head.Code != 200 {
		t.Fatal(head.Code)
	}
	var traces []tracing.Trace
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(path)
		traces = nil
		scan := bufio.NewScanner(strings.NewReader(string(data)))
		for scan.Scan() {
			var tr tracing.Trace
			if json.Unmarshal(scan.Bytes(), &tr) == nil {
				traces = append(traces, tr)
			}
		}
		if len(traces) >= 3 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if len(traces) != 3 {
		t.Fatalf("traces: %#v", traces)
	}
	tr := traces[0]
	if tr.Status != 200 || tr.RequestID != response.Header().Get("X-Context-Request-Id") || tr.CorrelationID != "test-client-1" || tr.UserID == "" || tr.SQL != "SELECT title FROM deals" || len(tr.Spans) < 4 || strings.Contains(tr.Route, "secret") {
		t.Fatalf("trace %#v", tr)
	}
	if traces[1].Status != 400 {
		t.Fatalf("failure: %#v", traces[1])
	}
	if traces[2].Method != "HEAD" || traces[2].Route != "/api/context/schema" || traces[2].Status != 200 {
		t.Fatalf("HEAD trace: %#v", traces[2])
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
}

func TestTracingSnapshotAndRESTPrivacy(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	path := filepath.Join(t.TempDir(), "traces.jsonl")
	cfg.Tracing = tracing.Config{Enabled: true, Path: path, Service: "private-app"}
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	response := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation"}`)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	rest := request(h, "POST", "/api/collections/deals/records", tokens["alice"], `{"title":"private-body-value"}`)
	if rest.Code != 200 {
		t.Fatal(rest.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	var data []byte
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(path)
		if strings.Count(string(data), "\n") >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Contains(string(data), "private-body-value") || strings.Contains(string(data), "SELECT name") || strings.Contains(string(data), tokens["alice"]) {
		t.Fatal("captured private payload")
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatal(string(data))
	}
	var tr tracing.Trace
	if err := json.Unmarshal([]byte(lines[0]), &tr); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, span := range tr.Spans {
		names[span.Name] = true
		if span.DurationMS < 0 || span.OffsetMS < 0 {
			t.Fatal(span)
		}
	}
	for _, name := range []string{"snapshot.wait", "snapshot.build", "snapshot.reader_init", "sql.prepare", "sql.execute", "sql.scan", "response.encode", "auth"} {
		if !names[name] {
			t.Fatalf("missing phase %s", name)
		}
	}
	if err := json.Unmarshal([]byte(lines[1]), &tr); err != nil {
		t.Fatal(err)
	}
	if tr.Route != "/api/collections/{collection}/records" {
		t.Fatal(tr.Route)
	}
}

func TestTraceRouteMethods(t *testing.T) {
	for _, pattern := range []string{"GET /api/context/schema", "HEAD /api/context/schema", "/api/context/schema"} {
		if got := traceRoute(pattern); got != "/api/context/schema" {
			t.Fatalf("pattern %q: %q", pattern, got)
		}
	}
}

func TestTracingRecoveredPanic(t *testing.T) {
	app, token, _ := fixture(t)
	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		e.Router.GET("/api/panic-test", func(re *core.RequestEvent) error { panic("private-panic-value") }).Bind(apis.RequireAuth("agents"))
		return e.Next()
	})
	path := filepath.Join(t.TempDir(), "traces.jsonl")
	cfg := Config{AuthCollection: "agents", Tables: map[string][]string{"deals": {"id", "title"}}, TimeoutMS: 1000, MaxRows: 100, MaxBytes: 4096, Tracing: tracing.Config{Enabled: true, Path: path, Service: "test"}}
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	response := request(h, "GET", "/api/panic-test", token, "")
	if response.Code != 500 {
		t.Fatalf("panic response %d", response.Code)
	}
	// The panic must still pass to PocketBase's normal recovery and leave the router usable.
	if again := request(h, "GET", "/api/context/schema", token, ""); again.Code != 200 {
		t.Fatal(again.Code)
	}
	var data []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(path)
		if strings.Count(string(data), "\n") >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatal(string(data))
	}
	var tr tracing.Trace
	if err := json.Unmarshal([]byte(lines[0]), &tr); err != nil {
		t.Fatal(err)
	}
	if tr.Status != 500 || tr.UserID == "" || tr.Route != "/api/panic-test" || tr.DurationMS <= 0 {
		t.Fatalf("panic trace %#v", tr)
	}
	if strings.Contains(string(data), "private-panic-value") {
		t.Fatal("panic contents leaked into trace")
	}
}
