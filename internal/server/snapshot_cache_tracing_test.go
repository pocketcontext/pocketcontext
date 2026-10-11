package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

func TestSnapshotCacheTracingDistinguishesWarmAcquisition(t *testing.T) {
	app, cfg, tokens, _ := snapshotFixture(t)
	path := filepath.Join(t.TempDir(), "traces.jsonl")
	cfg.Tracing = tracing.Config{Enabled: true, Path: path, Service: "private-app"}
	h, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var at string
	for range 2 {
		r := request(h, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation ORDER BY name"}`)
		if r.Code != 200 || !strings.Contains(r.Body.String(), `"bob"`) || !strings.Contains(r.Body.String(), `"carol"`) {
			t.Fatalf("query: %d %s", r.Code, r.Body)
		}
		next := r.Header().Get("X-Context-Snapshot-At")
		if next == "" || (at != "" && at != next) {
			t.Fatal("second query did not reuse snapshot")
		}
		at = next
	}
	var data []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(path)
		if strings.Count(string(data), "\n") == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two traces, got %d", len(lines))
	}
	for i, line := range lines {
		var tr tracing.Trace
		if err := json.Unmarshal([]byte(line), &tr); err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, span := range tr.Spans {
			names[span.Name] = true
		}
		for _, name := range []string{"snapshot.wait", "snapshot.acquire", "sql.prepare", "sql.execute", "sql.scan", "response.encode", "auth"} {
			if !names[name] {
				t.Fatalf("request %d missing %s", i, name)
			}
		}
		for _, name := range []string{"snapshot.build", "snapshot.reader_init"} {
			if names[name] != (i == 0) {
				t.Fatalf("request %d unexpected build phase presence for %s", i, name)
			}
		}
	}
}
