package database

import (
	"context"
	"database/sql/driver"
	"net/url"
	"reflect"
	"strings"

	sqlite3 "github.com/mattn/go-sqlite3"
)

type guardedConnector struct {
	controller *Controller
	dsn        string
}

func (c *guardedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Driver().Open(c.dsn)
}
func (c *guardedConnector) Driver() driver.Driver { return &guardedDriver{controller: c.controller} }

type guardedDriver struct{ controller *Controller }

func (d *guardedDriver) Open(name string) (driver.Conn, error) {
	frozen, release, err := d.controller.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	if frozen {
		u, err := url.Parse(name)
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("mode", "rw")
		u.RawQuery = q.Encode()
		name = u.String()
	}
	raw, err := (&sqlite3.SQLiteDriver{}).Open(name)
	if err != nil {
		return nil, err
	}
	conn := &guardedConn{raw: raw.(*sqlite3.SQLiteConn), controller: d.controller}
	conn.raw.RegisterAuthorizer(conn.authorize)
	conn.internal = true
	setup := `PRAGMA busy_timeout=10000; PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-32000;`
	if !frozen {
		setup += `PRAGMA journal_mode=WAL; PRAGMA journal_size_limit=200000000; PRAGMA synchronous=NORMAL;`
	}
	_, err = conn.raw.Exec(setup, nil)
	conn.internal = false
	if err == nil {
		err = conn.setFrozen(frozen)
	}
	if err != nil {
		raw.Close()
		return nil, err
	}
	return conn, nil
}

type guardedConn struct {
	raw                *sqlite3.SQLiteConn
	controller         *Controller
	frozen             bool
	internal           bool
	transactionControl bool
	preparing          *bool
	txRelease          func()
}

func (c *guardedConn) authorize(op int, a, b, database string) int {
	if c.internal {
		return sqlite3.SQLITE_OK
	}
	if (op == sqlite3.SQLITE_TRANSACTION || op == sqlite3.SQLITE_SAVEPOINT) && !c.transactionControl {
		return sqlite3.SQLITE_DENY
	}
	control := op == sqlite3.SQLITE_ATTACH || op == sqlite3.SQLITE_DETACH
	if op == sqlite3.SQLITE_PRAGMA {
		name := strings.ToLower(a)
		control = control || b != "" || name == "wal_checkpoint" || name == "incremental_vacuum" || name == "optimize" || name == "shrink_memory"
		// Connection enforcement is exclusively owned by this wrapper. Disallow
		// cached statements that could turn it off during a later frozen period.
		if name == "query_only" && b != "" {
			return sqlite3.SQLITE_DENY
		}
	}
	if control && c.preparing != nil {
		*c.preparing = true
	}
	if control && c.frozen {
		return sqlite3.SQLITE_DENY
	}
	return sqlite3.SQLITE_OK
}
func (c *guardedConn) setFrozen(frozen bool) error {
	c.internal = true
	defer func() { c.internal = false }()
	value := "OFF"
	if frozen {
		value = "ON"
	}
	if _, err := c.raw.Exec("PRAGMA query_only="+value, nil); err != nil {
		return err
	}
	c.frozen = frozen
	return nil
}
func (c *guardedConn) enter() (func(), error) {
	if c.txRelease != nil {
		return func() {}, nil
	}
	frozen, release, err := c.controller.acquire()
	if err != nil {
		return nil, err
	}
	if err = c.setFrozen(frozen); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
func (c *guardedConn) Close() error {
	err := c.raw.Close()
	if c.txRelease != nil {
		c.txRelease()
		c.txRelease = nil
	}
	return err
}
func (c *guardedConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}
func (c *guardedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	release, err := c.enter()
	if err != nil {
		return nil, err
	}
	defer release()
	control := false
	c.preparing = &control
	raw, err := c.raw.PrepareContext(ctx, query)
	c.preparing = nil
	if err != nil {
		return nil, err
	}
	return &guardedStmt{raw: raw.(*sqlite3.SQLiteStmt), conn: c, control: control}, nil
}
func (c *guardedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *guardedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	release, err := c.enter()
	if err != nil {
		return nil, err
	}
	c.transactionControl = true
	raw, err := c.raw.BeginTx(ctx, opts)
	c.transactionControl = false
	if err != nil {
		release()
		return nil, err
	}
	c.txRelease = release
	return &guardedTx{raw: raw, conn: c}, nil
}
func (c *guardedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	release, err := c.enter()
	if err != nil {
		return nil, err
	}
	defer release()
	return c.raw.ExecContext(ctx, query, args)
}
func (c *guardedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	release, err := c.enter()
	if err != nil {
		return nil, err
	}
	raw, err := c.raw.QueryContext(ctx, query, args)
	if err != nil {
		release()
		return nil, err
	}
	return &guardedRows{raw: raw, release: release}, nil
}
func (c *guardedConn) Ping(ctx context.Context) error { return c.raw.Ping(ctx) }

type guardedTx struct {
	raw  driver.Tx
	conn *guardedConn
}

func (t *guardedTx) finish(commit bool) error {
	t.conn.transactionControl = true
	defer func() { t.conn.transactionControl = false }()
	defer func() {
		if t.conn.txRelease != nil {
			t.conn.txRelease()
			t.conn.txRelease = nil
		}
	}()
	if commit {
		return t.raw.Commit()
	}
	return t.raw.Rollback()
}
func (t *guardedTx) Commit() error   { return t.finish(true) }
func (t *guardedTx) Rollback() error { return t.finish(false) }

type guardedStmt struct {
	raw     *sqlite3.SQLiteStmt
	conn    *guardedConn
	control bool
}

func (s *guardedStmt) Close() error  { return s.raw.Close() }
func (s *guardedStmt) NumInput() int { return s.raw.NumInput() }
func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
func (s *guardedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), named(args))
}
func (s *guardedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), named(args))
}
func (s *guardedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	release, err := s.conn.enter()
	if err != nil {
		return nil, err
	}
	defer release()
	if s.control && s.conn.frozen {
		return nil, ErrReadOnly
	}
	return s.raw.ExecContext(ctx, args)
}
func (s *guardedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	release, err := s.conn.enter()
	if err != nil {
		return nil, err
	}
	if s.control && s.conn.frozen {
		release()
		return nil, ErrReadOnly
	}
	raw, err := s.raw.QueryContext(ctx, args)
	if err != nil {
		release()
		return nil, err
	}
	return &guardedRows{raw: raw, release: release}, nil
}

type guardedRows struct {
	raw     driver.Rows
	release func()
}

func (r *guardedRows) Columns() []string { return r.raw.Columns() }
func (r *guardedRows) Close() error      { defer r.release(); return r.raw.Close() }
func (r *guardedRows) Next(dest []driver.Value) error {
	return r.raw.Next(dest)
}
func (r *guardedRows) ColumnTypeDatabaseTypeName(i int) string {
	if v, ok := r.raw.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return v.ColumnTypeDatabaseTypeName(i)
	}
	return ""
}
func (r *guardedRows) ColumnTypeLength(i int) (int64, bool) {
	if v, ok := r.raw.(driver.RowsColumnTypeLength); ok {
		return v.ColumnTypeLength(i)
	}
	return 0, false
}
func (r *guardedRows) ColumnTypeNullable(i int) (bool, bool) {
	if v, ok := r.raw.(driver.RowsColumnTypeNullable); ok {
		return v.ColumnTypeNullable(i)
	}
	return false, false
}
func (r *guardedRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	if v, ok := r.raw.(driver.RowsColumnTypePrecisionScale); ok {
		return v.ColumnTypePrecisionScale(i)
	}
	return 0, 0, false
}
func (r *guardedRows) ColumnTypeScanType(i int) reflect.Type {
	if v, ok := r.raw.(driver.RowsColumnTypeScanType); ok {
		return v.ColumnTypeScanType(i)
	}
	return reflect.TypeOf(new(any)).Elem()
}
