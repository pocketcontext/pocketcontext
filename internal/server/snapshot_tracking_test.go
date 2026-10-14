package server

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketcontext/pocketcontext/internal/database"
	"github.com/pocketcontext/pocketcontext/internal/snapshotstate"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func TestSnapshotTrackingRestoredDatabaseLifecycle(t *testing.T) {
	app, cfg, tokens, people := snapshotFixture(t)
	cfg.Snapshot.TrackChanges = true
	handler, err := startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertTotal := func(handler http.Handler, total float64) {
		t.Helper()
		var at string
		for range 2 {
			response := request(handler, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT coalesce(sum(salary),0) FROM compensation"}`)
			var result sqlread.Result
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != 200 || len(result.Rows) != 1 || result.Rows[0][0] != total {
				t.Fatalf("restored snapshot: expected %v, got status %d (%v)", total, response.Code, err)
			}
			current := response.Header().Get("X-Context-Snapshot-At")
			if current == "" || (at != "" && at != current) {
				t.Fatal("unchanged restored database did not reuse its snapshot")
			}
			at = current
		}
	}
	type trackedVersion struct {
		Table   string `db:"table_name"`
		Version string `db:"version"`
	}
	versions := func(app core.App) []trackedVersion {
		t.Helper()
		var rows []trackedVersion
		if err := app.DB().NewQuery("SELECT table_name,version FROM _context_snapshot_versions ORDER BY table_name").All(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	assertTotal(handler, 180)
	before := versions(app)
	// Close every source reader and writer before the test harness copies the
	// entire isolated data directory. This exercises persistent metadata, not
	// a second writer sharing a live database or a retained snapshot file.
	if err := app.OnTerminate().Trigger(&core.TerminateEvent{App: app}, func(e *core.TerminateEvent) error {
		return app.ClearBootstrap()
	}); err != nil {
		t.Fatal(err)
	}
	var controller *database.Controller
	restored, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DataDir: app.DataDir(), DBConnect: func(path string) (*dbx.DB, error) {
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
	t.Cleanup(restored.Cleanup)
	if !reflect.DeepEqual(before, versions(restored)) {
		t.Fatal("restoration changed tracking metadata")
	}
	if _, err := controller.BeginFreeze(0); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.CompleteFreeze(context.Background()); err != nil {
		t.Fatal(err)
	}
	handler, err = startConfiguredRouter(t, restored, cfg)
	if err != nil {
		t.Fatal("restored tracking failed frozen startup", err)
	}
	assertTotal(handler, 180)
	if !reflect.DeepEqual(before, versions(restored)) {
		t.Fatal("frozen startup rewrote recovered tracking metadata")
	}
	if _, err := controller.Unfreeze(context.Background(), controller.Status().Generation); err != nil {
		t.Fatal(err)
	}
	rows, err := restored.FindRecordsByFilter("compensation", "name = 'bob'", "", 1, 0)
	if err != nil || len(rows) != 1 {
		t.Fatal("restored compensation missing", err)
	}
	rows[0].Set("salary", 120)
	if err := restored.Save(rows[0]); err != nil {
		t.Fatal(err)
	}
	assertTotal(handler, 200)
	grant, err := restored.FindRecordById("management_chain", people["alice_bob"].Id)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Delete(grant); err != nil {
		t.Fatal(err)
	}
	assertTotal(handler, 80)
	if reflect.DeepEqual(before, versions(restored)) {
		t.Fatal("restored triggers did not observe writes and revocation")
	}
	account, err := restored.FindRecordById("agents", people["alice"].Id)
	if err != nil {
		t.Fatal(err)
	}
	account.RefreshTokenKey()
	if err := restored.Save(account); err != nil {
		t.Fatal(err)
	}
	response := request(handler, "POST", "/api/context/query", tokens["alice"], `{"sql":"SELECT name FROM compensation"}`)
	if response.Code != http.StatusUnauthorized && response.Code != http.StatusForbidden {
		t.Fatal("restored snapshot accepted a revoked token")
	}
}

func TestSnapshotTrackingFrozenStartupIsReadOnly(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "installed"}[installed], func(t *testing.T) {
			var controller *database.Controller
			app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DataDir: t.TempDir(), DBConnect: func(path string) (*dbx.DB, error) {
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
			if err := app.Save(core.NewAuthCollection("readers")); err != nil {
				t.Fatal(err)
			}
			if err := app.Save(core.NewBaseCollection("documents")); err != nil {
				t.Fatal(err)
			}
			cfg := Config{AuthCollection: "readers", Tables: map[string][]string{"documents": {"id"}}, Snapshot: &sqlread.SnapshotConfig{TrackChanges: true, Filters: map[string]string{"documents": "1"}}}
			if installed {
				if err := ensureSnapshotTracking(app, cfg); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := controller.BeginFreeze(0); err != nil {
				t.Fatal(err)
			}
			if _, err := controller.CompleteFreeze(context.Background()); err != nil {
				t.Fatal(err)
			}
			err = ensureSnapshotTracking(app, cfg)
			if installed && err != nil {
				t.Fatal("installed tracking wrote during frozen startup", err)
			}
			if !installed && err == nil {
				t.Fatal("tracking installation bypassed maintenance fence")
			}
		})
	}
}

func TestSnapshotTrackingRejectsChangedDefinitions(t *testing.T) {
	app, cfg, _, _ := snapshotFixture(t)
	if err := ensureSnapshotTracking(app, cfg); err != nil {
		t.Fatal(err)
	}
	trigger := snapshotstate.Definitions([]string{"compensation"})[1]
	if _, err := app.DB().NewQuery("DROP TRIGGER " + snapshotstate.Quote(trigger.Name)).Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.DB().NewQuery("CREATE TRIGGER " + snapshotstate.Quote(trigger.Name) + " AFTER INSERT ON compensation BEGIN SELECT 1; END").Execute(); err != nil {
		t.Fatal(err)
	}
	if err := ensureSnapshotTracking(app, cfg); err == nil {
		t.Fatal("silently accepted altered tracking trigger")
	}
}

func TestSnapshotTrackingResolvesAuthenticationCollectionID(t *testing.T) {
	app, cfg, _, _ := snapshotFixture(t)
	auth, err := app.FindCollectionByNameOrId(cfg.AuthCollection)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AuthCollection = auth.Id
	cfg.Snapshot.TrackChanges = true
	_, err = startConfiguredRouter(t, app, cfg)
	if err != nil {
		t.Fatal("auth collection ID used as a SQLite table", err)
	}
	var rows []struct {
		Name string `db:"table_name"`
	}
	if err := app.DB().NewQuery("SELECT table_name FROM _context_snapshot_versions WHERE table_name={:name}").Bind(dbx.Params{"name": auth.Name}).All(&rows); err != nil || len(rows) != 1 {
		t.Fatalf("authentication table was not tracked by name: %v", err)
	}
}
