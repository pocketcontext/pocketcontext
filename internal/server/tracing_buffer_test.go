package server

import (
	"encoding/json"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func bufferRequest(h http.Handler, token, opt, sql string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/api/context/query", strings.NewReader(`{"sql":"SELECT title FROM deals"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", token)
	req.Header.Set("X-Context-Trace", opt)
	req.Header.Set("X-Context-Capture-Sql", sql)
	req.Header.Set("X-Context-Correlation-Id", "client-chosen")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}
func TestBufferTraceAuthOptInAndSQL(t *testing.T) {
	for _, capture := range []bool{false, true} {
		t.Run(map[bool]string{false: "noSQL", true: "SQL"}[capture], func(t *testing.T) {
			app, token, outsider := fixture(t)
			collection, _ := app.FindCollectionByNameOrId("agents")
			second := core.NewRecord(collection)
			second.SetEmail("second@example.com")
			second.SetPassword("test-password-12345")
			if err := app.Save(second); err != nil {
				t.Fatal(err)
			}
			other, _ := second.NewAuthToken()
			ac, _ := app.FindCollectionByNameOrId("_superusers")
			admin := core.NewRecord(ac)
			admin.SetEmail("admin@example.com")
			admin.SetPassword("test-password-12345")
			if err := app.Save(admin); err != nil {
				t.Fatal(err)
			}
			adminToken, _ := admin.NewAuthToken()
			cfg := Config{AuthCollection: "agents", Tables: map[string][]string{"deals": {"id", "title"}}, TimeoutMS: 1000, MaxRows: 100, MaxBytes: 4096, Tracing: tracing.Config{Enabled: true, Delivery: "buffer", Service: "test", CaptureSQL: capture}}
			h, err := startConfiguredRouter(t, app, cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, opt := range []string{"", "true", "0"} {
				w := bufferRequest(h, token, opt, "1")
				if w.Code != 200 || w.Header().Get("X-Context-Request-Id") != "" {
					t.Fatal("opt-out", w.Code)
				}
			}
			for _, sqlOpt := range []string{"", "1"} {
				w := bufferRequest(h, token, "1", sqlOpt)
				id := w.Header().Get("X-Context-Request-Id")
				if w.Code != 200 || len(id) != 32 {
					t.Fatal("missing trace", w.Code, id)
				}
				path := "/api/context/traces/" + id
				own := request(h, "GET", path, token, "")
				if own.Code != 200 {
					t.Fatal(own.Code, own.Body.String())
				}
				recursive := httptest.NewRequest("GET", path, nil)
				recursive.Header.Set("Authorization", token)
				recursive.Header.Set("X-Context-Trace", "1")
				recursiveResponse := httptest.NewRecorder()
				h.ServeHTTP(recursiveResponse, recursive)
				if recursiveResponse.Code != 200 || recursiveResponse.Header().Get("X-Context-Request-Id") != "" {
					t.Fatal("opted-in retrieval recursively traced")
				}
				if own.Header().Get("Cache-Control") != "no-store" || own.Header().Get("X-Context-Request-Id") != "" {
					t.Fatal("cached or recursive")
				}
				var tr tracing.Trace
				if err := json.Unmarshal(own.Body.Bytes(), &tr); err != nil {
					t.Fatal(err)
				}
				if tr.RequestID != id || tr.CorrelationID != "client-chosen" || len(tr.Spans) < 4 {
					t.Fatal(tr)
				}
				if (tr.SQL != "") != (capture && sqlOpt == "1") {
					t.Fatal("SQL permission")
				}
				if cross := request(h, "GET", path, other, ""); cross.Code != 404 {
					t.Fatal("cross user", cross.Code)
				}
				for _, bad := range []string{"", outsider, adminToken} {
					if d := request(h, "GET", path, bad, ""); d.Code == 200 {
						t.Fatal("unauthorized")
					}
				}
				for _, id := range []string{"client-chosen", "00000000000000000000000000000000"} {
					if d := request(h, "GET", "/api/context/traces/"+id, token, ""); d.Code != 404 {
						t.Fatal("guessed ID", d.Code)
					}
				}
			}
			w := bufferRequest(h, other, "1", "1")
			path := "/api/context/traces/" + w.Header().Get("X-Context-Request-Id")
			second.RefreshTokenKey()
			if err := app.Save(second); err != nil {
				t.Fatal(err)
			}
			fresh, _ := second.NewAuthToken()
			if d := request(h, "GET", path, fresh, ""); d.Code != 404 {
				t.Fatal("new session retrieved prior trace", d.Code)
			}
			if d := request(h, "GET", path, other, ""); d.Code == 200 {
				t.Fatal("revoked token")
			}
		})
	}
}
