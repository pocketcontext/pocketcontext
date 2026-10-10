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
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

// FilteredReadSpec describes a trusted, startup-validated read of selected
// configured columns of one snapshot output table. Rows always pass the
// table's snapshot filter for the requester. Where optionally narrows rows
// further; like a filter it is trusted configuration, may bind only
// :requester and may reference only configured columns and policy tables.
type FilteredReadSpec struct {
	Table   string
	Columns []string
	Where   string
}

// FilteredRead is a validated FilteredReadSpec. Create it with
// SnapshotSource.PrepareFilteredRead before serving requests.
type FilteredRead struct {
	source        *SnapshotSource
	table         string
	columns       []string
	all, selected string
}

// Columns returns the selected column names in result order.
func (r *FilteredRead) Columns() []string { return append([]string(nil), r.columns...) }

// PrepareFilteredRead validates a filtered read against the source database.
// Selecting by ID requires the table's configured "id" column.
func (s *SnapshotSource) PrepareFilteredRead(spec FilteredReadSpec) (*FilteredRead, error) {
	configured, ok := s.cfg.Tables[spec.Table]
	if !ok {
		return nil, fmt.Errorf("filtered read table %q is not a snapshot output table", spec.Table)
	}
	allowed := map[string]bool{}
	for _, col := range configured {
		allowed[col] = true
	}
	if !allowed["id"] {
		return nil, fmt.Errorf("filtered read table %q requires a configured id column", spec.Table)
	}
	if len(spec.Columns) == 0 || len(spec.Columns) > 64 {
		return nil, errors.New("filtered read requires 1..64 columns")
	}
	names := make([]string, len(spec.Columns))
	for i, col := range spec.Columns {
		if !allowed[col] {
			return nil, fmt.Errorf("filtered read column %s.%s is not a configured output column", spec.Table, col)
		}
		names[i] = "+" + quote(col)
	}
	query := "SELECT " + strings.Join(names, ",") + " FROM " + quote(spec.Table) + " WHERE (:requester <> '') AND (\n" + s.snapshot.Filters[spec.Table] + "\n)"
	if spec.Where != "" {
		if strings.TrimSpace(spec.Where) == "" {
			return nil, errors.New("filtered read predicate is empty")
		}
		query += " AND (\n" + spec.Where + "\n)"
	}
	selected := query + " AND " + quote(spec.Table) + `."id" IN (SELECT value FROM json_each(:ids))`
	for _, q := range []string{query, selected} {
		if err := singleStatement(q); err != nil {
			return nil, fmt.Errorf("invalid filtered read for %q: %w", spec.Table, err)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("snapshot source is closed")
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
		defer func() { *c.jsonEach = false }()
		for q, inputs := range map[string]int{query: 1, selected: 2} {
			stmt, err := c.PrepareContext(ctx, q)
			if err != nil {
				return fmt.Errorf("invalid filtered read for %q: %w", spec.Table, err)
			}
			n := stmt.NumInput()
			stmt.Close()
			if n != inputs {
				return fmt.Errorf("filtered read predicate for %q may bind only :requester", spec.Table)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &FilteredRead{source: s, table: spec.Table, columns: append([]string(nil), spec.Columns...), all: query, selected: selected}, nil
}

// FilteredTx reads authorized rows inside one source read transaction.
type FilteredTx struct {
	ctx         context.Context
	tx          *sql.Tx
	source      *SnapshotSource
	requester   string
	rows, bytes int
}

// errFilteredSource marks source database failures, distinguishing them from
// errors returned by the caller's callbacks.
type filteredSourceError struct{ err error }

func (e filteredSourceError) Error() string { return e.err.Error() }
func (e filteredSourceError) Unwrap() error { return e.err }

// Read calls fn for each row of r visible to the requester. A nil ids slice
// reads every visible row; otherwise only visible rows whose id is listed.
// Values are SQLite storage values: nil, int64, float64, string or []byte.
// Rows and encoded bytes count against the snapshot maxRows/maxBytes limits
// across the whole transaction.
func (t *FilteredTx) Read(r *FilteredRead, ids []string, fn func(values []any) error) error {
	if r == nil || r.source != t.source {
		return errors.New("filtered read belongs to another snapshot source")
	}
	query, args := r.all, []any{sql.Named("requester", t.requester)}
	if ids != nil {
		encoded, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		query = r.selected
		args = append(args, sql.Named("ids", string(encoded)))
	}
	rows, err := t.tx.QueryContext(t.ctx, query, args...)
	if err != nil {
		return filteredSourceError{err}
	}
	defer rows.Close()
	for rows.Next() {
		if t.rows >= t.source.snapshot.MaxRows {
			return ErrSnapshotLimit
		}
		row := make([]any, len(r.columns))
		ptr := make([]any, len(row))
		for i := range row {
			ptr[i] = &row[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			return filteredSourceError{err}
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return err
		}
		t.bytes += len(encoded)
		if t.bytes > t.source.snapshot.MaxBytes {
			return ErrSnapshotLimit
		}
		t.rows++
		if err := fn(row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return filteredSourceError{err}
	}
	return nil
}

// ReadFiltered runs fn inside one source read transaction for requester. It
// shares the snapshot concurrency slots, timeout and row/byte limits. Errors
// returned by fn are returned unchanged; source failures become ErrBusy,
// ErrSnapshotLimit, a context error or ErrSnapshotBuild.
func (s *SnapshotSource) ReadFiltered(ctx context.Context, requester string, fn func(*FilteredTx) error) error {
	if requester == "" {
		return ErrSnapshotBuild
	}
	readCtx, cancel := context.WithTimeout(ctx, time.Duration(s.snapshot.TimeoutMS)*time.Millisecond)
	defer cancel()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrSnapshotBuild
	}
	waitDone := tracing.Start(ctx, "snapshot.wait")
	select {
	case s.slots <- struct{}{}:
	case <-readCtx.Done():
		waitDone()
		return readCtx.Err()
	}
	waitDone()
	defer func() { <-s.slots }()
	readDone := tracing.Start(ctx, "snapshot.filtered_read")
	defer readDone()
	err := s.readFiltered(readCtx, requester, fn)
	if err == nil {
		return nil
	}
	var source filteredSourceError
	if !errors.As(err, &source) {
		if errors.Is(err, ErrSnapshotLimit) || errors.Is(err, ErrSnapshotBuild) {
			return err
		}
		if readCtx.Err() != nil {
			return readCtx.Err()
		}
		return err
	}
	if readCtx.Err() != nil {
		return readCtx.Err()
	}
	err = classify(source.err)
	var se sqlite3.Error
	if errors.As(err, &se) && (se.Code == sqlite3.ErrTooBig || se.Code == sqlite3.ErrFull) {
		return ErrSnapshotLimit
	}
	if errors.Is(err, ErrBusy) {
		return err
	}
	return ErrSnapshotBuild
}

func (s *SnapshotSource) readFiltered(ctx context.Context, requester string, fn func(*FilteredTx) error) error {
	conn, err := s.source.db.Conn(ctx)
	if err != nil {
		return filteredSourceError{err}
	}
	defer conn.Close()
	// This goroutine holds the connection exclusively until Close, so its
	// authorizer flags can be toggled directly. Reset them before the
	// connection returns to the pool that also serves snapshot exports.
	var trusted, jsonEach *bool
	if err = conn.Raw(func(raw any) error {
		c := raw.(roConn)
		trusted, jsonEach = c.trusted, c.jsonEach
		return nil
	}); err != nil {
		return filteredSourceError{err}
	}
	defer func() { *trusted, *jsonEach = false, false }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return filteredSourceError{err}
	}
	defer tx.Rollback()
	// The read pins schema and data for the transaction; a table named
	// json_each must not shadow the built-in ID-list module.
	*trusted = true
	var reserved int
	err = tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE lower(name)='json_each'").Scan(&reserved)
	*trusted = false
	if err != nil {
		return filteredSourceError{err}
	}
	if reserved != 0 {
		return ErrSnapshotBuild
	}
	*jsonEach = true
	return fn(&FilteredTx{ctx: ctx, tx: tx, source: s, requester: requester})
}
