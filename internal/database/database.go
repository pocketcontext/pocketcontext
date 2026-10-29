// Package database configures PocketBase's writer with the same SQLite library
// used by the read-only query engine, so both share SQLite's process-local locks.
package database

import (
	"database/sql"
	"net/url"
	"path/filepath"

	"ganigeorgiev.com/pocketbase/dbx"
	sqlite3 "github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("pocketcontext_writer", &sqlite3.SQLiteDriver{ConnectHook: func(conn *sqlite3.SQLiteConn) error {
		_, err := conn.Exec(`PRAGMA busy_timeout = 10000;
PRAGMA journal_mode = WAL;
PRAGMA journal_size_limit = 200000000;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -32000;`, nil)
		return err
	}})
	dbx.BuilderFuncMap["pocketcontext_writer"] = dbx.BuilderFuncMap["sqlite3"]
}

func Connect(path string) (*dbx.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	return dbx.Open("pocketcontext_writer", u.String())
}
