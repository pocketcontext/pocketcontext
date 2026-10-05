package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pocketbase/dbx"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketcontext/pocketcontext/internal/database"
)

func TestMaintenanceRequestDrain(t *testing.T) {
	d := &requestDrain{}
	if !d.enter() {
		t.Fatal("initial admission failed")
	}
	d.block(true)
	if d.enter() {
		t.Fatal("admitted mutation after block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if d.wait(ctx) == nil {
		t.Fatal("reported drained with accepted work active")
	}
	d.leave()
	if err := d.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.block(false)
	if !d.enter() {
		t.Fatal("unfreeze did not reopen admission")
	}
	d.leave()
}

func TestMaintenanceReadClassification(t *testing.T) {
	for _, p := range []string{"/api/context/query", "/api/context/search", "/api/files/token", "/api/realtime", "/api/collections/users/auth-refresh"} {
		if !maintenanceRead(httptest.NewRequest(http.MethodPost, p, nil)) {
			t.Fatalf("blocked reader %s", p)
		}
	}
	for _, p := range []string{"/api/batch", "/api/collections/users/auth-with-oauth2", "/api/wiki/search/rebuild", "/api/collections/sources/records"} {
		if maintenanceRead(httptest.NewRequest(http.MethodPost, p, nil)) {
			t.Fatalf("write bypass %s", p)
		}
	}
	for _, p := range []string{"/api/files/sources/record/original.png?thumb=100x100", "/api/collections/sources/records/rebuild/all", "/custom-write"} {
		if maintenanceRead(httptest.NewRequest(http.MethodGet, p, nil)) {
			t.Fatalf("side-effect route bypass %s", p)
		}
	}
}

func TestMaintenanceAPI(t *testing.T) {
	dir := t.TempDir()
	var c *database.Controller
	app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DataDir: dir, DBConnect: func(path string) (*dbx.DB, error) {
		if c == nil {
			var err error
			c, err = database.NewController(filepath.Dir(path))
			if err != nil {
				return nil, err
			}
		}
		return c.Connect(path)
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)
	collection := core.NewAuthCollection("agents")
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}
	user := core.NewRecord(collection)
	user.SetEmail("reader@example.com")
	user.SetPassword("test-password-12345")
	if err := app.Save(user); err != nil {
		t.Fatal(err)
	}
	userToken, err := user.NewAuthToken()
	if err != nil {
		t.Fatal(err)
	}
	superusers, err := app.FindCollectionByNameOrId("_superusers")
	if err != nil {
		t.Fatal(err)
	}
	admin := core.NewRecord(superusers)
	admin.SetEmail("operator@example.com")
	admin.SetPassword("test-password-12345")
	if err := app.Save(admin); err != nil {
		t.Fatal(err)
	}
	adminToken, err := admin.NewAuthToken()
	if err != nil {
		t.Fatal(err)
	}
	docs := core.NewBaseCollection("documents")
	rule := "@request.auth.id != ''"
	docs.CreateRule = &rule
	docs.ListRule = &rule
	docs.Fields.Add(&core.TextField{Name: "title"})
	if err := app.Save(docs); err != nil {
		t.Fatal(err)
	}
	started, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	t.Cleanup(func() { finishOnce.Do(func() { close(finish) }) })
	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		e.Router.POST("/api/test-slow-write", func(re *core.RequestEvent) error {
			close(started)
			<-finish
			if _, err := re.App.DB().NewQuery("INSERT INTO documents (id,title) VALUES ('slowabcdefghijk','accepted before freeze')").Execute(); err != nil {
				return err
			}
			return re.NoContent(204)
		})
		return e.Next()
	})
	RegisterMaintenance(app, c)
	h, err := startRouter(t, app, map[string][]string{"documents": {"id", "title"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"", userToken} {
		for _, method := range []string{"GET", "PUT"} {
			r := request(h, method, maintenancePath, token, `{"readOnly":true,"expectedGeneration":0}`)
			if r.Code != 401 && r.Code != 403 {
				t.Fatalf("unauthorized control: %d", r.Code)
			}
		}
	}
	status := func() map[string]any {
		r := request(h, "GET", maintenancePath, adminToken, "")
		if r.Code != 200 {
			t.Fatalf("status: %d %s", r.Code, r.Body)
		}
		var value map[string]any
		if err := json.Unmarshal(r.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	initial := status()
	slowResult := make(chan *httptest.ResponseRecorder, 1)
	go func() { slowResult <- request(h, "POST", "/api/test-slow-write", adminToken, "") }()
	<-started
	freezeResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		freezeResult <- request(h, "PUT", maintenancePath, adminToken, fmt.Sprintf(`{"readOnly":true,"expectedGeneration":%.0f}`, initial["generation"]))
	}()
	deadline := time.Now().Add(2 * time.Second)
	for c.Status().State != "draining" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Status().State != "draining" {
		t.Fatal("freeze did not enter draining")
	}
	select {
	case <-freezeResult:
		t.Fatal("freeze completed before accepted request")
	default:
	}
	finishOnce.Do(func() { close(finish) })
	if r := <-slowResult; r.Code != 204 {
		t.Fatalf("admitted mutation failed during drain: %d %s", r.Code, r.Body)
	}
	freeze := <-freezeResult
	if freeze.Code != 200 {
		t.Fatalf("freeze: %d %s", freeze.Code, freeze.Body)
	}
	if status()["state"] != "read_only" {
		t.Fatal("not frozen")
	}
	for _, token := range []string{userToken, adminToken} {
		r := request(h, "POST", "/api/collections/documents/records", token, `{"title":"blocked"}`)
		if r.Code != 503 {
			t.Fatalf("write while frozen: %d %s", r.Code, r.Body)
		}
	}
	if _, err := app.DB().NewQuery("INSERT INTO documents (id,title) VALUES ('abcdefghijklmno','bypass')").Execute(); err == nil {
		t.Fatal("direct SQL bypassed freeze")
	}
	read := request(h, "POST", "/api/context/query", userToken, `{"sql":"SELECT id,title FROM documents"}`)
	if read.Code != 200 {
		t.Fatalf("read while frozen: %d %s", read.Code, read.Body)
	}
	bad := request(h, "PUT", maintenancePath, adminToken, fmt.Sprintf(`{"readOnly":false,"expectedGeneration":%.0f}`, initial["generation"]))
	if bad.Code != 409 {
		t.Fatalf("stale unfreeze: %d", bad.Code)
	}
	frozen := status()
	unfreeze := request(h, "PUT", maintenancePath, adminToken, fmt.Sprintf(`{"readOnly":false,"expectedGeneration":%.0f}`, frozen["generation"]))
	if unfreeze.Code != 200 {
		t.Fatalf("unfreeze: %d %s", unfreeze.Code, unfreeze.Body)
	}
	write := request(h, "POST", "/api/collections/documents/records", userToken, `{"title":"resumed"}`)
	if write.Code != 200 {
		t.Fatalf("resumed write: %d %s", write.Code, write.Body)
	}
}

func TestMaintenanceDropsPersistentLogs(t *testing.T) {
	dir := t.TempDir()
	var controller *database.Controller
	app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DataDir: dir, DBConnect: func(path string) (*dbx.DB, error) {
		if controller == nil {
			var err error
			controller, err = database.NewController(filepath.Dir(path))
			if err != nil {
				return nil, err
			}
		}
		return controller.Connect(path)
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)
	RegisterMaintenance(app, controller)
	originalLogDays := app.Settings().Logs.MaxDays
	persist := func() {
		t.Helper()
		// Exercise the same auxiliary transaction/model-save path as PocketBase's
		// buffered logger. Skipping must return nil to avoid its full-model error log.
		err := app.AuxRunInTransaction(func(txApp core.App) error {
			entry := &core.Log{Message: "synthetic maintenance log regression"}
			entry.Id = core.GenerateDefaultRandomId()
			return txApp.AuxSave(entry)
		})
		if err != nil {
			t.Fatalf("log persistence path returned error: %v", err)
		}
	}
	count := func(want int) {
		t.Helper()
		var got int
		err := app.AuxDB().NewQuery("SELECT count(*) FROM _logs WHERE message = 'synthetic maintenance log regression'").Row(&got)
		if err != nil || got != want {
			t.Fatalf("persistent logs: got %d, want %d; error %v", got, want, err)
		}
		if app.Settings().Logs.MaxDays != originalLogDays {
			t.Fatal("maintenance mutated log retention settings")
		}
	}
	persist()
	count(1)
	if _, err := controller.BeginFreeze(controller.Status().Generation); err != nil {
		t.Fatal(err)
	}
	persist()
	count(1)
	if _, err := controller.CompleteFreeze(context.Background()); err != nil {
		t.Fatal(err)
	}
	persist()
	count(1)
	if _, err := controller.Unfreeze(context.Background(), controller.Status().Generation); err != nil {
		t.Fatal(err)
	}
	persist()
	count(2)
}
