package sqlread

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// SnapshotRead is a trusted, startup-validated projection over an already
// authorized generation. It never opens or attaches the source database.
type SnapshotRead struct {
	source        *SnapshotSource
	all, selected string
	columns       []string
}

func (s *SnapshotSource) PrepareSnapshotRead(spec FilteredReadSpec) (*SnapshotRead, error) {
	if !s.BackgroundEnabled() {
		return nil, errors.New("snapshot reads require background mode")
	}
	configured, ok := s.cfg.Tables[spec.Table]
	if !ok {
		return nil, errors.New("snapshot read table is not exported")
	}
	allowed := map[string]bool{}
	for _, column := range configured {
		allowed[column] = true
	}
	if !allowed["id"] || len(spec.Columns) == 0 || len(spec.Columns) > 64 {
		return nil, errors.New("snapshot read requires id and 1..64 columns")
	}
	names := make([]string, len(spec.Columns))
	for i, column := range spec.Columns {
		if !allowed[column] {
			return nil, errors.New("snapshot read column is not exported")
		}
		names[i] = "+" + quote(column)
	}
	query := "SELECT " + strings.Join(names, ",") + " FROM " + quote(spec.Table) + " WHERE (:requester <> '')"
	if spec.Where != "" {
		query += " AND (\n" + spec.Where + "\n)"
	}
	selected := query + " AND " + quote(spec.Table) + `."id" IN (SELECT value FROM json_each(:ids))`
	query += " ORDER BY " + quote(spec.Table) + `."id"`
	selected += " ORDER BY " + quote(spec.Table) + `."id"`
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrSnapshotBuild
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.snapshot.TimeoutMS)*time.Millisecond)
	defer cancel()
	conn, err := s.source.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	err = conn.Raw(func(raw any) error {
		c := raw.(roConn)
		*c.jsonEach = true
		defer func() { *c.jsonEach = false; *c.readTables = nil }()
		for q, inputs := range map[string]int{query: 1, selected: 2} {
			if err := singleStatement(q); err != nil {
				return err
			}
			*c.readTables = map[string]bool{}
			stmt, err := c.PrepareContext(ctx, q)
			if err != nil {
				return err
			}
			n := stmt.NumInput()
			stmt.Close()
			if n != inputs {
				return errors.New("snapshot predicate may bind only :requester")
			}
			for table := range *c.readTables {
				if table == "json_each" {
					continue
				}
				found := false
				for output := range s.cfg.Tables {
					if strings.EqualFold(table, output) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("snapshot predicate depends on unexported table %q", table)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &SnapshotRead{source: s, all: query, selected: selected, columns: append([]string(nil), spec.Columns...)}, nil
}

func (l *SnapshotLease) Read(ctx context.Context, read *SnapshotRead, ids []string, fn func([]any) error) error {
	if read == nil || read.source != l.source {
		return errors.New("snapshot read belongs to another source")
	}
	query, args := read.all, []any{sql.Named("requester", l.entry.requester)}
	if ids != nil {
		encoded, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		query = read.selected
		args = append(args, sql.Named("ids", string(encoded)))
	}
	// Hold a dedicated connection while raising its scalar limit for this
	// trusted projection. Restore before returning it to ordinary SQL callers.
	conn, err := l.entry.engine.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var oldLimit int
	if err := conn.Raw(func(raw any) error {
		oldLimit = raw.(roConn).c.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, l.source.background.config.MaxValueBytes)
		return nil
	}); err != nil {
		return err
	}
	defer conn.Raw(func(raw any) error {
		raw.(roConn).c.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, oldLimit)
		return nil
	})
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		values := make([]any, len(read.columns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		if err := fn(values); err != nil {
			return err
		}
	}
	return rows.Err()
}
