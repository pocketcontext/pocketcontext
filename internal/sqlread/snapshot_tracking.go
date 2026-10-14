package sqlread

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pocketcontext/pocketcontext/internal/snapshotstate"
)

type snapshotState struct {
	version int64
	epochs  map[string]string
}

func (a snapshotState) equal(b snapshotState) bool {
	if a.epochs == nil || b.epochs == nil {
		return a.epochs == nil && b.epochs == nil && a.version == b.version
	}
	if len(a.epochs) != len(b.epochs) {
		return false
	}
	for name, epoch := range a.epochs {
		if b.epochs[name] != epoch {
			return false
		}
	}
	return true
}

func (s *SnapshotSource) initTracking(ctx context.Context) error {
	sort.Strings(s.trackedTables)
	return s.trustedMonitor(func() error {
		tx, err := s.monitor.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, expected := range snapshotstate.Definitions(s.trackedTables) {
			var kind, ddl string
			if err := tx.QueryRowContext(ctx, "SELECT type,sql FROM sqlite_schema WHERE name=?", expected.Name).Scan(&kind, &ddl); err != nil {
				return err
			}
			if kind != expected.Kind || ddl != expected.SQL {
				return fmt.Errorf("snapshot tracking definition mismatch")
			}
		}
		if err := tx.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&s.trackedSchema); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *SnapshotSource) trustedMonitor(fn func() error) error {
	var trusted *bool
	if err := s.monitor.Raw(func(raw any) error { trusted = raw.(roConn).trusted; *trusted = true; return nil }); err != nil {
		return err
	}
	defer func() { *trusted = false }()
	return fn()
}

// cacheMu owns the monitor and lastState. data_version is only a fast path;
// the transactional tokens, not commit counts, determine which exports changed.
func (s *SnapshotSource) state(ctx context.Context) (snapshotState, error) {
	version, err := s.version(ctx)
	if err != nil {
		return snapshotState{}, err
	}
	if !s.snapshot.TrackChanges {
		return snapshotState{version: version}, nil
	}
	if s.lastState != nil && s.lastState.version == version {
		return *s.lastState, nil
	}
	state := snapshotState{version: version, epochs: make(map[string]string)}
	err = s.trustedMonitor(func() error {
		tx, err := s.monitor.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var schema int64
		if err := tx.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&schema); err != nil {
			return err
		}
		if schema != s.trackedSchema {
			return fmt.Errorf("source schema changed; restart to validate snapshot tracking")
		}
		marks, args := make([]string, len(s.trackedTables)), make([]any, len(s.trackedTables))
		for i, name := range s.trackedTables {
			marks[i], args[i] = "?", name
		}
		rows, err := tx.QueryContext(ctx, "SELECT table_name,version FROM "+quote(snapshotstate.Table)+" WHERE table_name IN ("+strings.Join(marks, ",")+")", args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var name, epoch string
			if err := rows.Scan(&name, &epoch); err != nil {
				rows.Close()
				return err
			}
			if len(epoch) != 32 {
				rows.Close()
				return fmt.Errorf("invalid snapshot tracking token")
			}
			state.epochs[strings.ToLower(name)] = epoch
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(state.epochs) != len(s.trackedTables) {
			return fmt.Errorf("missing snapshot tracking token")
		}
		return tx.Commit()
	})
	if err != nil {
		return snapshotState{}, err
	}
	s.lastState = &state
	return state, nil
}

func (s *SnapshotSource) changedExports(before, after snapshotState) map[string]bool {
	changed := make(map[string]bool)
	auth := strings.ToLower(s.snapshot.AuthorizationTable)
	for table, dependencies := range s.dependencies {
		if auth != "" && before.epochs[auth] != after.epochs[auth] {
			changed[table] = true
			continue
		}
		for dependency := range dependencies {
			if before.epochs[dependency] != after.epochs[dependency] {
				changed[table] = true
				break
			}
		}
	}
	return changed
}
