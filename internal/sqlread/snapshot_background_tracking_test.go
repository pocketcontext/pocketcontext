package sqlread

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

func backgroundTrackingFixture(t *testing.T, track bool) (*sql.DB, *SnapshotSource) {
	t.Helper()
	db, path, cfg, sc := snapshotFixture(t)
	if _, err := db.Exec("CREATE TABLE accounts(id TEXT); CREATE TABLE unrelated(id TEXT); CREATE TABLE unused_policy(id TEXT)"); err != nil {
		t.Fatal(err)
	}
	sc = backgroundConfig(sc)
	// Observe via requests so these tests do not depend on timer scheduling.
	sc.Background.RefreshIntervalMS = 60000
	sc.PolicyTables["unused_policy"] = []string{"id"}
	if track {
		sc.TrackChanges, sc.AuthorizationTable = true, "accounts"
		installTestTracking(t, db, "pay", "managers", "accounts", "unused_policy")
	}
	return db, newTestSnapshot(t, path, cfg, sc)
}

func TestTrackedBackgroundRelevantFreshness(t *testing.T) {
	db, source := backgroundTrackingFixture(t, true)
	read := func(sum int64) string {
		t.Helper()
		result, meta, err := source.QueryWithOptions(context.Background(), "alice", "SELECT coalesce(sum(salary),0) FROM pay", SnapshotReadOptions{Consistency: "fresh"})
		if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != sum || meta.Stale {
			t.Fatalf("fresh result: %#v %#v %v", result, meta, err)
		}
		return meta.Generation
	}
	prev := read(300)
	for _, step := range []struct {
		write   string
		sum     int64
		changed bool
	}{
		{"INSERT INTO unrelated VALUES ('irrelevant')", 300, false},
		{"INSERT INTO unused_policy VALUES ('also irrelevant')", 300, false},
		{"UPDATE pay SET salary=110 WHERE id='a'", 310, true},
		{"DELETE FROM managers WHERE requester='alice'", 0, true},
		{"INSERT INTO accounts VALUES ('account')", 0, true},
	} {
		if _, err := db.Exec(step.write); err != nil {
			t.Fatal(err)
		}
		next := read(step.sum)
		if (next != prev) != step.changed {
			t.Fatalf("wrong generation after %q: %q -> %q", step.write, prev, next)
		}
		prev = next
	}
	m := source.BackgroundMetrics()
	if m.ObservedChanges != 5 || m.UnrelatedChanges != 2 || m.CompletedBuilds != 4 || m.ColdBuilds != 1 {
		t.Fatalf("unexpected observation/build counters: %+v", m)
	}
}

func TestTrackedBackgroundObservationFailureDeniesNewLeases(t *testing.T) {
	for _, change := range []string{
		"CREATE TABLE changed_schema(id TEXT)",
		"DELETE FROM _context_snapshot_versions WHERE table_name='managers'",
	} {
		t.Run(change, func(t *testing.T) {
			db, source := backgroundTrackingFixture(t, true)
			ctx := context.Background()
			initial, err := source.AcquireSnapshot(ctx, "alice", SnapshotReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			gen := initial.Metadata().Generation
			initial.Close()

			// Hold a replacement after SQL export but before its publication.
			started, release := make(chan struct{}), make(chan struct{})
			finish := sync.OnceFunc(func() { close(release) })
			defer finish()
			source.SetSnapshotInitializer(func(ctx context.Context, _ *SnapshotLease) error {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			})
			if _, err := db.Exec("UPDATE pay SET salary=110 WHERE id='a'"); err != nil {
				t.Fatal(err)
			}
			if _, err := source.SnapshotStatus(ctx, "alice"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("replacement did not start")
			}
			if _, err := db.Exec(change); err != nil {
				t.Fatal(err)
			}
			for _, opts := range []SnapshotReadOptions{{}, {Consistency: "fresh"}, {AtLeast: gen}, {ExpectedGeneration: gen}} {
				lease, err := source.AcquireSnapshot(ctx, "alice", opts)
				if lease != nil {
					lease.Close()
				}
				if !errors.Is(err, ErrSnapshotBuild) {
					t.Fatalf("expected failed source validation for %+v, got %v", opts, err)
				}
			}
			// Finish the in-flight attempt and verify it was discarded.
			finish()
			awaitBackground(t, func() bool {
				meta, err := source.SnapshotStatus(ctx, "alice")
				return err == nil && !meta.Refreshing && meta.Stale && meta.LastError == "source_check_failed" && meta.Generation == gen
			})
		})
	}
}

// Six separately observed unrelated writes for two active requesters, then
// one relevant write. Count only completed generation files after warming.
func TestTrackedBackgroundWriteVolume(t *testing.T) {
	for _, scenario := range []struct {
		name            string
		track           bool
		unrelatedBuilds uint64
	}{{"untracked", false, 12}, {"tracked", true, 0}} {
		t.Run(scenario.name, func(t *testing.T) {
			db, source := backgroundTrackingFixture(t, scenario.track)
			_, err := db.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<256)
				INSERT INTO pay(id,salary) SELECT printf('bulk%d',i),printf('text-%02048d',i) FROM n;
				INSERT INTO managers SELECT 'alice',id FROM pay WHERE id LIKE 'bulk%';
				INSERT INTO managers SELECT 'bob',id FROM pay WHERE id LIKE 'bulk%'`)
			if err != nil {
				t.Fatal(err)
			}
			fresh := func() BackgroundSnapshotMetrics {
				t.Helper()
				for _, user := range []string{"alice", "bob"} {
					lease, err := source.AcquireSnapshot(context.Background(), user, SnapshotReadOptions{Consistency: "fresh"})
					if err != nil {
						t.Fatal(err)
					}
					lease.Close()
				}
				return source.BackgroundMetrics()
			}
			warm := fresh()
			var after BackgroundSnapshotMetrics
			for i := range 6 {
				if _, err := db.Exec("INSERT INTO unrelated VALUES (?)", i); err != nil {
					t.Fatal(err)
				}
				after = fresh()
			}
			builds, bytes := after.CompletedBuilds-warm.CompletedBuilds, after.GenerationBytes-warm.GenerationBytes
			if builds != scenario.unrelatedBuilds || (bytes == 0) != (builds == 0) {
				t.Fatalf("unrelated writes: %d builds, %d generation bytes", builds, bytes)
			}
			if _, err := db.Exec("UPDATE pay SET salary=110 WHERE id='a'"); err != nil {
				t.Fatal(err)
			}
			end := fresh()
			if end.CompletedBuilds-after.CompletedBuilds != 2 || end.GenerationBytes <= after.GenerationBytes {
				t.Fatal("relevant write did not rebuild both requesters")
			}
			t.Logf("unrelated: %d builds, %d generation bytes; relevant: 2 builds, %d generation bytes",
				builds, bytes, end.GenerationBytes-after.GenerationBytes)
		})
	}
}
