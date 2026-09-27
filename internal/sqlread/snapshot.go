package sqlread

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

// SnapshotConfig is a trusted application policy, never request-supplied SQL.
// Filters are WHERE expressions. :requester is the authenticated record ID.
type SnapshotConfig struct {
	Filters       map[string]string   `json:"filters"`
	PolicyTables  map[string][]string `json:"policyTables"`
	TimeoutMS     int                 `json:"timeoutMs"`
	MaxRows       int                 `json:"maxRows"`
	MaxBytes      int                 `json:"maxBytes"`
	MaxConcurrent int                 `json:"maxConcurrent"`
}

func NormalizeSnapshotConfig(c *SnapshotConfig) error {
	if c == nil {
		return errors.New("snapshot configuration is nil")
	}
	for _, v := range []struct {
		p             *int
		def, min, max int
	}{
		{&c.TimeoutMS, 5000, 1, 30000}, {&c.MaxRows, 100000, 1, 1000000},
		{&c.MaxBytes, 16777216, 1024, 67108864}, {&c.MaxConcurrent, 4, 1, 16},
	} {
		if *v.p == 0 {
			*v.p = v.def
		}
		if *v.p < v.min || *v.p > v.max {
			return errors.New("snapshot limit outside permitted range")
		}
	}
	return nil
}

var ErrSnapshotLimit = errors.New("snapshot exceeds build limit")
var ErrSnapshotBuild = errors.New("snapshot build failed")

type SnapshotSource struct {
	source   *Engine
	cfg      Config
	snapshot SnapshotConfig
	tables   []Table
	exports  map[string]string
	root     string
	slots    chan struct{}
	mu       sync.RWMutex
	closed   bool
}

func NewSnapshotSource(path string, cfg Config, sc SnapshotConfig) (*SnapshotSource, error) {
	if err := NormalizeSnapshotConfig(&sc); err != nil {
		return nil, err
	}
	all := map[string][]string{}
	seen := map[string]bool{}
	for _, entries := range []map[string][]string{cfg.Tables, sc.PolicyTables} {
		for name, cols := range entries {
			key := strings.ToLower(name)
			if len(cols) == 0 || seen[key] {
				return nil, fmt.Errorf("snapshot tables require distinct names and explicit columns: %q", name)
			}
			seen[key] = true
			colSeen := map[string]bool{}
			for _, col := range cols {
				k := strings.ToLower(col)
				if colSeen[k] {
					return nil, fmt.Errorf("duplicate snapshot column %q", col)
				}
				colSeen[k] = true
			}
			all[name] = append([]string(nil), cols...)
		}
	}
	if len(cfg.Tables) == 0 || len(sc.Filters) != len(cfg.Tables) {
		return nil, errors.New("snapshot requires exactly one filter per exported table")
	}
	exports := map[string]string{}
	copied := map[string][]string{}
	for name, cols := range cfg.Tables {
		filter, ok := sc.Filters[name]
		if !ok || strings.TrimSpace(filter) == "" {
			return nil, fmt.Errorf("missing snapshot filter for %q", name)
		}
		// Unary + leaves the SQLite value unchanged but removes declared-type
		// metadata, preventing the driver from coercing DATE/BOOLEAN values.
		names := make([]string, len(cols))
		for i, col := range cols {
			names[i] = "+" + quote(col)
		}
		query := "SELECT " + strings.Join(names, ",") + " FROM " + quote(name) + " WHERE (:requester <> '') AND (\n" + filter + "\n)"
		if err := singleStatement(query); err != nil {
			return nil, fmt.Errorf("invalid snapshot filter for %q: %w", name, err)
		}
		exports[name] = query
		copied[name] = append([]string(nil), cols...)
	}
	cfg.Tables = copied
	sourceCfg := Config{Tables: all, Timeout: time.Duration(sc.TimeoutMS) * time.Millisecond, MaxRows: sc.MaxRows, MaxBytes: sc.MaxBytes}
	source, err := newEngine(path, sourceCfg, true)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			source.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), sourceCfg.Timeout)
	defer cancel()
	conn, err := source.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	err = conn.Raw(func(raw any) error {
		c := raw.(roConn)
		for name, query := range exports {
			stmt, e := c.PrepareContext(ctx, query)
			if e != nil {
				return fmt.Errorf("invalid snapshot filter for %q: %w", name, e)
			}
			inputs := stmt.NumInput()
			stmt.Close()
			if inputs != 1 {
				return fmt.Errorf("snapshot filter for %q may bind only :requester", name)
			}
		}
		return nil
	})
	conn.Close()
	if err != nil {
		return nil, err
	}
	tables := []Table{}
	for _, t := range source.tables {
		if _, ok := cfg.Tables[t.Name]; ok {
			tables = append(tables, t)
		}
	}
	root, err := os.MkdirTemp("", "pocketcontext-snapshot-")
	if err != nil {
		return nil, err
	}
	success = true
	return &SnapshotSource{source: source, cfg: cfg, snapshot: sc, tables: tables, exports: exports, root: root, slots: make(chan struct{}, sc.MaxConcurrent)}, nil
}

func (s *SnapshotSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.source.Close(), os.RemoveAll(s.root))
}
func (s *SnapshotSource) Schema(ctx context.Context) ([]Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("snapshot source is closed")
	}
	return (&Engine{tables: s.tables}).Schema(ctx)
}

func (s *SnapshotSource) Query(ctx context.Context, requester, query string) (Result, time.Time, error) {
	if len(query) > 65536 {
		return Result{}, time.Time{}, errors.New("SQL exceeds 65536 bytes")
	}
	if err := singleStatement(query); err != nil {
		return Result{}, time.Time{}, err
	}
	if requester == "" {
		return Result{}, time.Time{}, ErrSnapshotBuild
	}
	// Export timeout includes waiting for capacity; query execution has its own
	// normal engine deadline. Keep the capacity slot until files are removed.
	buildCtx, cancel := context.WithTimeout(ctx, time.Duration(s.snapshot.TimeoutMS)*time.Millisecond)
	defer cancel()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Result{}, time.Time{}, ErrSnapshotBuild
	}
	waitDone := tracing.Start(ctx, "snapshot.wait")
	select {
	case s.slots <- struct{}{}:
	case <-buildCtx.Done():
		waitDone()
		return Result{}, time.Time{}, buildCtx.Err()
	}
	waitDone()
	defer func() { <-s.slots }()
	dir, err := os.MkdirTemp(s.root, "request-")
	if err != nil {
		return Result{}, time.Time{}, ErrSnapshotBuild
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "snapshot.db")
	buildDone := tracing.Start(ctx, "snapshot.build")
	at, err := s.build(buildCtx, path, requester)
	buildDone()
	if err != nil {
		if buildCtx.Err() != nil {
			return Result{}, time.Time{}, buildCtx.Err()
		}
		err = classify(err)
		var se sqlite3.Error
		if errors.As(err, &se) && (se.Code == sqlite3.ErrTooBig || se.Code == sqlite3.ErrFull) {
			err = ErrSnapshotLimit
		}
		if errors.Is(err, ErrBusy) || errors.Is(err, ErrSnapshotLimit) {
			return Result{}, time.Time{}, err
		}
		return Result{}, time.Time{}, ErrSnapshotBuild
	}
	cancel()
	if err := ctx.Err(); err != nil {
		return Result{}, time.Time{}, err
	}
	initDone := tracing.Start(ctx, "snapshot.reader_init")
	engine, err := New(path, s.cfg)
	initDone()
	if err != nil {
		return Result{}, time.Time{}, ErrSnapshotBuild
	}
	defer engine.Close()
	result, err := engine.Query(ctx, query)
	return result, at, err
}

func (s *SnapshotSource) build(ctx context.Context, path, requester string) (time.Time, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return time.Time{}, err
	}
	file.Close()
	dst, err := sql.Open("sqlite3", path)
	if err != nil {
		return time.Time{}, err
	}
	defer dst.Close()
	dst.SetMaxOpenConns(1)
	// Bound physical storage separately from encoded rows, allowing page/schema overhead.
	pages := s.snapshot.MaxBytes*2/4096 + len(s.tables)*4 + 32
	if _, err = dst.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=4096; PRAGMA max_page_count=%d; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF", pages)); err != nil {
		return time.Time{}, err
	}
	writer, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer writer.Rollback()
	reader, err := s.source.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer reader.Rollback()
	// Force acquisition of the source read snapshot before recording its time.
	// Every export, including policy subqueries, uses this same transaction.
	first := s.tables[0]
	if _, err = reader.ExecContext(ctx, "SELECT "+quote(first.Columns[0].Name)+" FROM "+quote(first.Name)+" LIMIT 1"); err != nil {
		return time.Time{}, err
	}
	at := time.Now().UTC()
	count, size := 0, 0
	for _, table := range s.tables {
		defs := []string{}
		names := []string{}
		marks := []string{}
		// Preserve configured projection order; types come only from inspected schema.
		types := map[string]string{}
		for _, col := range table.Columns {
			types[strings.ToLower(col.Name)] = col.Type
		}
		for _, col := range s.cfg.Tables[table.Name] {
			defs = append(defs, quote(col)+" "+quote(types[strings.ToLower(col)]))
			names = append(names, quote(col))
			marks = append(marks, "?")
		}
		if _, err = writer.ExecContext(ctx, "CREATE TABLE "+quote(table.Name)+" ("+strings.Join(defs, ",")+")"); err != nil {
			return time.Time{}, err
		}
		insert, err := writer.PrepareContext(ctx, "INSERT INTO "+quote(table.Name)+" ("+strings.Join(names, ",")+") VALUES ("+strings.Join(marks, ",")+")")
		if err != nil {
			return time.Time{}, err
		}
		rows, err := reader.QueryContext(ctx, s.exports[table.Name], sql.Named("requester", requester))
		if err != nil {
			insert.Close()
			return time.Time{}, err
		}
		err = func() error {
			defer rows.Close()
			defer insert.Close()
			for rows.Next() {
				if count >= s.snapshot.MaxRows {
					return ErrSnapshotLimit
				}
				row := make([]any, len(names))
				ptr := make([]any, len(names))
				for i := range row {
					ptr[i] = &row[i]
				}
				if err := rows.Scan(ptr...); err != nil {
					return err
				}
				encoded, err := json.Marshal(row)
				if err != nil {
					return err
				}
				size += len(encoded)
				if size > s.snapshot.MaxBytes {
					return ErrSnapshotLimit
				}
				count++
				if _, err = insert.ExecContext(ctx, row...); err != nil {
					return err
				}
			}
			return rows.Err()
		}()
		if err != nil {
			return time.Time{}, err
		}
	}
	if err = reader.Commit(); err != nil {
		return time.Time{}, err
	}
	if err = writer.Commit(); err != nil {
		return time.Time{}, err
	}
	return at, ctx.Err()
}
