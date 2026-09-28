// Package sqlread provides bounded, explicitly authorized SQL reads of a SQLite database.
package sqlread

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

type Config struct {
	Tables   map[string][]string
	Timeout  time.Duration
	MaxRows  int
	MaxBytes int
}
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}
type Result struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	Truncated bool     `json:"truncated"`
}

// ErrBusy wraps transient SQLite busy or locked errors. Callers should retry.
var ErrBusy = errors.New("database busy")

// ErrNotReadOnly rejects statements that sqlite3_stmt_readonly does not report as read-only.
var ErrNotReadOnly = errors.New("statement is not read-only")

type Engine struct {
	db     *sql.DB
	cfg    Config
	tables []Table
}

type connector struct {
	driver       *sqlite3.SQLiteDriver
	dsn          string
	transactions bool
	authorize    func(int, string, string, string) int
}

func (c connector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn := conn.(*sqlite3.SQLiteConn)
	// database/sql serializes each connection, including Conn.Raw. Keep the
	// callback registered once: re-registering would retain driver handles.
	trusted := new(bool)
	sqliteConn.RegisterAuthorizer(func(op int, a, b, db string) int {
		if *trusted {
			return sqlite3.SQLITE_OK
		}
		return c.authorize(op, a, b, db)
	})
	return roConn{c: sqliteConn, transactions: c.transactions, trusted: trusted}, nil
}
func (c connector) Driver() driver.Driver { return c.driver }

// roConn exposes only prepare, so database/sql cannot bypass the read-only
// check. The check runs on the prepared statement itself, before any step.
type roConn struct {
	c            *sqlite3.SQLiteConn
	transactions bool
	trusted      *bool
}

func (r roConn) Close() error                   { return r.c.Close() }
func (r roConn) Ping(ctx context.Context) error { return r.c.Ping(ctx) }
func (r roConn) Begin() (driver.Tx, error) {
	if r.transactions {
		return r.c.Begin()
	}
	return nil, errors.New("transactions are not supported")
}
func (r roConn) Prepare(query string) (driver.Stmt, error) {
	return r.PrepareContext(context.Background(), query)
}
func (r roConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := r.c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if s, ok := stmt.(*sqlite3.SQLiteStmt); !ok || !s.Readonly() {
		stmt.Close()
		return nil, ErrNotReadOnly
	}
	return stmt, nil
}

// New opens an independent read-only connection. Tables must be ordinary tables;
// views, virtual tables and internal names are rejected. An empty column list
// exposes all current columns. Restart after changing the exposed schema.
func New(path string, cfg Config) (*Engine, error) { return newEngine(path, cfg, false) }

func newEngine(path string, cfg Config, transactions bool) (*Engine, error) {
	if len(cfg.Tables) == 0 {
		return nil, errors.New("SQL read table allowlist is empty")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 1000
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 20
	}
	if cfg.MaxBytes < 128 {
		return nil, errors.New("MaxBytes must be at least 128")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_busy_timeout", "2000")
	u.RawQuery = q.Encode()
	setup, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}
	defer setup.Close()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	e := &Engine{cfg: cfg}
	allowed := map[string]map[string]bool{}
	for name, cols := range cfg.Tables {
		if strings.HasPrefix(name, "_") || strings.HasPrefix(strings.ToLower(name), "sqlite_") {
			return nil, fmt.Errorf("internal table %q cannot be exposed", name)
		}
		var ddl string
		err = setup.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", name).Scan(&ddl)
		if err != nil {
			return nil, fmt.Errorf("inspect table %q: %w", name, err)
		}
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddl)), "CREATE TABLE") {
			return nil, fmt.Errorf("%q must be an ordinary table", name)
		}
		rows, err := setup.QueryContext(ctx, "PRAGMA table_info("+quote(name)+")")
		if err != nil {
			return nil, err
		}
		table := Table{Name: name, Columns: []Column{}}
		found := map[string]bool{}
		want := map[string]bool{}
		for _, col := range cols {
			want[strings.ToLower(col)] = true
		}
		for rows.Next() {
			var cid, notnull, pk int
			var col, typ string
			var def any
			if err = rows.Scan(&cid, &col, &typ, &notnull, &def, &pk); err != nil {
				rows.Close()
				return nil, err
			}
			if len(cols) == 0 || want[strings.ToLower(col)] {
				table.Columns = append(table.Columns, Column{Name: col, Type: typ})
				found[strings.ToLower(col)] = true
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		for col := range want {
			if !found[col] {
				return nil, fmt.Errorf("unknown column %s.%s", name, col)
			}
		}
		allowed[strings.ToLower(name)] = found
		e.tables = append(e.tables, table)
	}
	sort.Slice(e.tables, func(i, j int) bool { return e.tables[i].Name < e.tables[j].Name })
	authorize := func(op int, a, b, db string) int {
		switch op {
		case sqlite3.SQLITE_TRANSACTION:
			if transactions {
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_SELECT, 33: // SQLITE_RECURSIVE
			return sqlite3.SQLITE_OK
		case sqlite3.SQLITE_READ:
			if !transactions && jsonTableColumn(a, b) && (db == "main" || (db == "" && b == "")) {
				return sqlite3.SQLITE_OK
			}
			// SQLite may omit database identity for optimized COUNT(*) reads.
			columns, ok := allowed[strings.ToLower(a)]
			if ok && ((db == "main" && columns[strings.ToLower(b)]) || (b == "" && (db == "main" || db == ""))) {
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_FUNCTION:
			if safeFunctions[strings.ToLower(b)] {
				return sqlite3.SQLITE_OK
			}
		}
		return sqlite3.SQLITE_DENY
	}
	d := &sqlite3.SQLiteDriver{ConnectHook: func(c *sqlite3.SQLiteConn) error {
		if _, err := c.Exec("PRAGMA query_only = 1; PRAGMA hard_heap_limit = 268435456;", nil); err != nil {
			return err
		}
		c.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, cfg.MaxBytes)
		c.SetLimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 65536)
		c.SetLimit(sqlite3.SQLITE_LIMIT_COLUMN, 256)
		c.SetLimit(sqlite3.SQLITE_LIMIT_EXPR_DEPTH, 100)
		c.SetLimit(sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 50)
		c.SetLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
		return nil
	}}
	e.db = sql.OpenDB(connector{driver: d, dsn: u.String(), transactions: transactions, authorize: authorize})
	e.db.SetMaxOpenConns(4)
	e.db.SetMaxIdleConns(4)
	if err = e.db.PingContext(ctx); err != nil {
		e.db.Close()
		return nil, err
	}
	return e, nil
}
func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// classify wraps busy and locked SQLite errors with ErrBusy.
func classify(err error) error {
	var se sqlite3.Error
	if errors.As(err, &se) && (se.Code == sqlite3.ErrBusy || se.Code == sqlite3.ErrLocked) {
		return fmt.Errorf("%w: %v", ErrBusy, err)
	}
	return err
}
func (e *Engine) Close() error { return e.db.Close() }
func (e *Engine) Schema(ctx context.Context) ([]Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]Table, len(e.tables))
	for i, t := range e.tables {
		out[i] = Table{Name: t.Name, Columns: append([]Column(nil), t.Columns...)}
	}
	return out, nil
}
func (e *Engine) Query(ctx context.Context, query string) (Result, error) {
	result := Result{Columns: []string{}, Rows: [][]any{}}
	if len(query) > 65536 {
		return result, errors.New("SQL exceeds 65536 bytes")
	}
	if err := singleStatement(query); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()
	prepareDone := tracing.Start(ctx, "sql.prepare")
	conn, err := e.db.Conn(ctx)
	if err != nil {
		prepareDone()
		return result, classify(err)
	}
	defer conn.Close()
	var tx driver.Tx
	defer func() {
		if tx != nil {
			conn.Raw(func(raw any) error {
				r := raw.(roConn)
				*r.trusted = true
				defer func() { *r.trusted = false }()
				if err := tx.Rollback(); err != nil {
					return driver.ErrBadConn
				}
				return nil
			})
		}
	}()
	err = conn.Raw(func(raw any) error {
		r := raw.(roConn)
		*r.trusted = true
		defer func() { *r.trusted = false }()
		var err error
		tx, err = r.c.Begin()
		if err != nil {
			return err
		}
		// This read pins the schema and data until user rows are closed. A later
		// schema change cannot turn a built-in JSON module into a private table.
		rows, err := r.c.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE lower(name) IN ('json_each','json_tree') LIMIT 1", nil)
		if err != nil {
			return err
		}
		defer rows.Close()
		values := make([]driver.Value, 1)
		if err = rows.Next(values); err == nil {
			return errors.New("json_each and json_tree are reserved SQL module names")
		}
		if err != io.EOF {
			return err
		}
		return nil
	})

	if err != nil {
		prepareDone()
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, classify(err)
	}
	stmt, err := conn.PrepareContext(ctx, query)
	prepareDone()
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, classify(err)
	}
	defer stmt.Close()
	executeDone := tracing.Start(ctx, "sql.execute")
	rows, err := stmt.QueryContext(ctx)
	executeDone()
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, classify(err)
	}
	defer rows.Close()
	scanDone := tracing.Start(ctx, "sql.scan")
	defer scanDone()
	result.Columns, err = rows.Columns()
	if err != nil {
		return result, err
	}
	if len(result.Columns) == 0 {
		return result, errors.New("statement must return columns")
	}
	header, _ := json.Marshal(result)
	size := len(header)
	if size > e.cfg.MaxBytes {
		return Result{}, errors.New("result columns exceed byte limit")
	}
	for rows.Next() {
		if len(result.Rows) >= e.cfg.MaxRows {
			result.Truncated = true
			break
		}
		row := make([]any, len(result.Columns))
		ptr := make([]any, len(row))
		for i := range row {
			ptr[i] = &row[i]
		}
		if err = rows.Scan(ptr...); err != nil {
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			return Result{}, classify(err)
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return Result{}, err
		}
		extra := len(encoded)
		if len(result.Rows) > 0 {
			extra++
		}
		if size+extra > e.cfg.MaxBytes {
			result.Truncated = true
			break
		}
		size += extra
		result.Rows = append(result.Rows, row)
	}
	if err = ctx.Err(); err != nil {
		return Result{}, err
	}
	if err = rows.Err(); err != nil {
		return Result{}, classify(err)
	}
	return result, nil
}
func (r Result) CSV() ([]byte, error) {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	if err := w.Write(r.Columns); err != nil {
		return nil, err
	}
	for _, row := range r.Rows {
		values := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				continue
			}
			switch x := v.(type) {
			case string:
				values[i] = x
			case []byte:
				values[i] = string(x)
			default:
				values[i] = fmt.Sprint(v)
			}
		}
		if err := w.Write(values); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return b.Bytes(), w.Error()
}

var safeFunctions = func() map[string]bool {
	m := map[string]bool{}
	for _, s := range strings.Fields(`abs avg ceiling ceil char coalesce concat concat_ws count date datetime exp floor format glob group_concat hex ifnull iif instr json json_array json_array_length json_extract json_group_array json_group_object json_object json_quote json_type json_valid julianday length like ln log log10 log2 lower ltrim max median min mod percentile percentile_cont percentile_disc nullif octet_length pi pow power printf quote replace round rtrim sign sqrt strftime string_agg substr substring sum time timediff total trim typeof unicode unixepoch upper row_number rank dense_rank percent_rank cume_dist ntile lag lead first_value last_value nth_value -> ->>`) {
		m[s] = true
	}
	return m
}()

// This lexer only finds statement boundaries; SQLite parses and authorizes SQL.
// A single trailing semicolon and trailing comments are accepted.
func singleStatement(s string) error {
	if strings.IndexByte(s, 0) >= 0 {
		return errors.New("NUL is not allowed in SQL")
	}
	ended, content := false, false
	for i := 0; i < len(s); {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' {
			i++
			continue
		}
		if c == '-' && i+1 < len(s) && s[i+1] == '-' {
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(s) && s[i+1] == '*' {
			j := strings.Index(s[i+2:], "*/")
			if j < 0 {
				return errors.New("unterminated SQL comment")
			}
			i += j + 4
			continue
		}
		if ended {
			return errors.New("exactly one SQL statement is allowed")
		}
		if c == ';' {
			ended = true
			i++
			continue
		}
		content = true
		if c == '\'' || c == '"' || c == '`' || c == '[' {
			end := c
			if c == '[' {
				end = ']'
			}
			i++
			closed := false
			for i < len(s) {
				if s[i] == end {
					if end != ']' && i+1 < len(s) && s[i+1] == end {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return errors.New("unterminated SQL quote")
			}
			continue
		}
		i++
	}
	if !content {
		return errors.New("SQL statement is empty")
	}
	return nil
}

// Only the fixed columns of SQLite's two built-in JSON traversal modules are
// exposed. Inputs still pass ordinary source table/column authorization.
func jsonTableColumn(table, column string) bool {
	switch strings.ToLower(table) {
	case "json_each", "json_tree":
	default:
		return false
	}
	switch strings.ToLower(column) {
	case "", "key", "value", "type", "atom", "id", "parent", "fullkey", "path", "json", "root":
		return true
	}
	return false
}
