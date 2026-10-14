package server

import (
	"fmt"
	"sort"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/snapshotstate"
)

// Installation runs only for explicitly opted-in applications. An already
// installed configuration performs no writes, including during frozen startup.
// Unexpected definitions are never replaced silently.
func ensureSnapshotTracking(app core.App, cfg Config) error {
	tables := []string{cfg.AuthCollection}
	for name := range cfg.Tables {
		tables = append(tables, name)
	}
	for name := range cfg.Snapshot.PolicyTables {
		tables = append(tables, name)
	}
	sort.Strings(tables)
	return app.RunInTransaction(func(tx core.App) error {
		for _, expected := range snapshotstate.Definitions(tables) {
			var rows []struct {
				Type string `db:"type"`
				SQL  string `db:"sql"`
			}
			if err := tx.DB().NewQuery("SELECT type,sql FROM sqlite_schema WHERE name={:name}").Bind(dbx.Params{"name": expected.Name}).All(&rows); err != nil {
				return err
			}
			if len(rows) > 0 {
				if len(rows) != 1 || rows[0].Type != expected.Kind || rows[0].SQL != expected.SQL {
					return fmt.Errorf("snapshot tracking definition mismatch; review schema before startup")
				}
				continue
			}
			if _, err := tx.DB().NewQuery(expected.SQL).Execute(); err != nil {
				return fmt.Errorf("initialize snapshot tracking (requires writable maintenance state): %w", err)
			}
		}
		for _, table := range tables {
			var rows []struct {
				Version string `db:"version"`
			}
			if err := tx.DB().NewQuery("SELECT version FROM " + snapshotstate.Quote(snapshotstate.Table) + " WHERE table_name={:name}").Bind(dbx.Params{"name": table}).All(&rows); err != nil {
				return err
			}
			if len(rows) == 0 {
				if _, err := tx.DB().NewQuery(snapshotstate.SeedSQL(table)).Execute(); err != nil {
					return err
				}
			} else if len(rows) != 1 || len(rows[0].Version) != 32 {
				return fmt.Errorf("invalid snapshot tracking token")
			}
		}
		return nil
	})
}
