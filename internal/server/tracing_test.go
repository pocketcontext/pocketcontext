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
		if len(traces) >= 2 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if len(traces) != 2 {
		t.Fatalf("traces: %#v", traces)
	}
	tr := traces[0]
	if tr.Status != 200 || tr.RequestID != response.Header().Get("X-Context-Request-Id") || tr.CorrelationID != "test-client-1" || tr.UserID == "" || tr.SQL != "SELECT title FROM deals" || len(tr.Spans) < 4 || strings.Contains(tr.Route, "secret") {
		t.Fatalf("trace %#v", tr)
	}
	if traces[1].Status != 400 {
		t.Fatalf("failure: %#v", traces[1])
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
