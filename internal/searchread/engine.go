// Package searchread exposes bounded full-text search using fixed SQL only.
package searchread

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	MaxQueryBytes   = 4096
	MaxTerms        = 16
	MaxLimit        = 100
	DefaultLimit    = 20
	MaxScopeMembers = 10000
	MaxOffset       = 10000
	MaxScopeBytes   = 4 << 20
)

var (
	ErrGenerationConflict = errors.New("search generation changed")
	ErrInvalidQuery       = errors.New("invalid search request")
	ErrIndexInvalid       = errors.New("search index is invalid")
	ErrBusy               = errors.New("search database busy")
	ErrResponseLimit      = errors.New("search response exceeds byte limit")
)

type Config struct {
	Indexes map[string]Index `json:"indexes"`
}
type Scope struct {
	Collection string `json:"collection"`
	Field      string `json:"field"`
}
type Generation struct {
	Collection string `json:"collection"`
	Record     string `json:"record"`
	Field      string `json:"field"`
}
type Request struct {
	Index              string `json:"index"`
	Query              string `json:"query"`
	Limit              int    `json:"limit"`
	Scope              string `json:"scope,omitempty"`
	Offset             int    `json:"offset,omitempty"`
	ExpectedGeneration string `json:"expectedGeneration,omitempty"`
}
type Index struct {
	Scope         *Scope      `json:"scope,omitempty"`
	Generation    *Generation `json:"generation,omitempty"`
	Table         string      `json:"table"`
	Collection    string      `json:"collection"`
	Columns       []string    `json:"columns"`
	SnippetColumn string      `json:"snippetColumn"`
	Weights       []float64   `json:"weights,omitempty"`
}
type Limits struct {
	Timeout           time.Duration
	MaxRows, MaxBytes int
}
type Hit struct {
	ID      string  `json:"id"`
	Score   float64 `json:"score"`
	Excerpt string  `json:"excerpt"`
}
type Result struct {
	Scope      string `json:"scope,omitempty"`
	Generation string `json:"generation,omitempty"`
	HasMore    bool   `json:"hasMore"`
	Index      string `json:"index"`
	Hits       []Hit  `json:"hits"`
	Truncated  bool   `json:"truncated"`
}
type definition struct {
	index            Index
	schema           map[string]string
	search, validIDs string
}
type Engine struct {
	db      *sql.DB
	indexes map[string]definition
	limits  Limits
}

var identifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

func validName(s string) bool {
	return identifier.MatchString(s) && !strings.HasPrefix(strings.ToLower(s), "sqlite_")
}
func quote(s string) string { return `"` + s + `"` }

// CanonicalDDL returns the only accepted index shape. Applications execute this
// DDL through trusted migrations, then maintain index rows transactionally.
func CanonicalDDL(i Index) (string, error) {
	if (i.Scope == nil) != (i.Generation == nil) {
		return "", ErrIndexInvalid
	}
	if i.Scope != nil && (!validName(i.Scope.Collection) || !validName(i.Scope.Field) || !validName(i.Generation.Collection) || !validName(i.Generation.Field) || !validToken(i.Generation.Record)) {
		return "", ErrIndexInvalid
	}
	if !validName(i.Table) || !validName(i.Collection) || strings.EqualFold(i.Table, i.Collection) || len(i.Columns) == 0 || len(i.Columns) > 16 {
		return "", fmt.Errorf("%w: index identifiers or column count", ErrIndexInvalid)
	}
	seen := map[string]bool{"record_id": true}
	cols := []string{`"record_id" UNINDEXED`}
	snippet := false
	for _, c := range i.Columns {
		if !validName(c) || seen[strings.ToLower(c)] {
			return "", fmt.Errorf("%w: column names", ErrIndexInvalid)
		}
		seen[strings.ToLower(c)] = true
		cols = append(cols, quote(c))
		if c == i.SnippetColumn {
			snippet = true
		}
	}
	if !snippet {
		return "", fmt.Errorf("%w: snippet column", ErrIndexInvalid)
	}
	if len(i.Weights) != 0 && len(i.Weights) != len(i.Columns) {
		return "", fmt.Errorf("%w: weights", ErrIndexInvalid)
	}
	for _, w := range i.Weights {
		if math.IsNaN(w) || math.IsInf(w, 0) || w < 0 || w > 100 {
			return "", fmt.Errorf("%w: weight range", ErrIndexInvalid)
		}
	}
	return "CREATE VIRTUAL TABLE " + quote(i.Table) + " USING fts5(" + strings.Join(cols, ", ") + ", tokenize='unicode61')", nil
}

// connector only exposes read-only prepared statements to database/sql.
type connector struct {
	dsn      string
	allow    func(int, string, string, string) int
	maxBytes int
}

func (c connector) Driver() driver.Driver { return &sqlite3.SQLiteDriver{} }
func (c connector) Connect(ctx context.Context) (driver.Conn, error) {
	d := sqlite3.SQLiteDriver{}
	raw, err := d.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	db := raw.(*sqlite3.SQLiteConn)
	fail := func(err error) (driver.Conn, error) { db.Close(); return nil, err }
	if _, err = db.Exec("PRAGMA query_only=1; PRAGMA hard_heap_limit=268435456", nil); err != nil {
		return fail(err)
	}
	// Scope documents have a separate bounded allowance even with tiny response caps.
	db.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, max(c.maxBytes, MaxScopeBytes))
	db.SetLimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 65536)
	db.SetLimit(sqlite3.SQLITE_LIMIT_COLUMN, 256)
	db.SetLimit(sqlite3.SQLITE_LIMIT_EXPR_DEPTH, 100)
	db.SetLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
	if err = db.RegisterFunc("pocketcontext_utf8_valid", utf8.ValidString, true); err != nil {
		return fail(err)
	}
	db.RegisterAuthorizer(c.allow)
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	return readConn{db}, nil
}

type readConn struct{ c *sqlite3.SQLiteConn }

func (r readConn) Close() error { return r.c.Close() }
func (r readConn) Begin() (driver.Tx, error) {
	tx, err := r.c.Begin()
	if err != nil {
		return nil, err
	}
	return readTx{tx}, nil
}

type readTx struct{ driver.Tx }

func (t readTx) Rollback() error {
	if err := t.Tx.Rollback(); err != nil {
		return driver.ErrBadConn
	}
	return nil
}
func (t readTx) Commit() error {
	if err := t.Tx.Commit(); err != nil {
		return driver.ErrBadConn
	}
	return nil
}
func (r readConn) Prepare(s string) (driver.Stmt, error) {
	return r.PrepareContext(context.Background(), s)
}
func (r readConn) PrepareContext(ctx context.Context, s string) (driver.Stmt, error) {
	stmt, err := r.c.PrepareContext(ctx, s)
	if err != nil {
		return nil, err
	}
	if x, ok := stmt.(*sqlite3.SQLiteStmt); !ok || !x.Readonly() {
		stmt.Close()
		return nil, errors.New("search statement is not read-only")
	}
	return stmt, nil
}
func classify(err error) error {
	var se sqlite3.Error
	if errors.As(err, &se) && se.Code == sqlite3.ErrTooBig {
		return fmt.Errorf("%w: SQLite value limit", ErrResponseLimit)
	}
	if errors.As(err, &se) && (se.Code == sqlite3.ErrBusy || se.Code == sqlite3.ErrLocked) {
		return fmt.Errorf("%w: %w", ErrBusy, err)
	}
	return err
}

func New(path string, cfg Config, limits Limits) (*Engine, error) {
	if len(cfg.Indexes) == 0 || len(cfg.Indexes) > 16 {
		return nil, fmt.Errorf("%w: index count", ErrIndexInvalid)
	}
	if limits.Timeout <= 0 {
		limits.Timeout = 2 * time.Second
	}
	if limits.MaxRows <= 0 || limits.MaxRows > MaxLimit {
		limits.MaxRows = MaxLimit
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = 1 << 20
	}
	if limits.MaxBytes < 128 {
		return nil, ErrResponseLimit
	}
	e := &Engine{indexes: map[string]definition{}, limits: limits}
	// Generate expected shadow DDL with the pinned SQLite implementation rather
	// than trusting metadata supplied by the application's database.
	expected, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	defer expected.Close()
	expected.SetMaxOpenConns(1)
	allowed := map[string]map[string]bool{"sqlite_master": {"type": true, "name": true, "sql": true}, "sqlite_schema": {"type": true, "name": true, "sql": true}}
	allowed["json_each"] = map[string]bool{"value": true, "json": true}
	tableNames := map[string]bool{}
	aliases := make([]string, 0, len(cfg.Indexes))
	for alias := range cfg.Indexes {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		idx := cfg.Indexes[alias]
		if idx.Scope != nil {
			copied := *idx.Scope
			idx.Scope = &copied
		}
		if idx.Generation != nil {
			copied := *idx.Generation
			idx.Generation = &copied
		}
		idx.Columns = append([]string(nil), idx.Columns...)
		idx.Weights = append([]float64(nil), idx.Weights...)
		ddl, err := CanonicalDDL(idx)
		if err != nil {
			return nil, err
		}
		if !validName(alias) || tableNames[strings.ToLower(idx.Table)] {
			return nil, fmt.Errorf("%w: duplicate index or alias", ErrIndexInvalid)
		}
		tableNames[strings.ToLower(idx.Table)] = true
		if _, err = expected.Exec(ddl); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrIndexInvalid, err)
		}
		names := []string{idx.Table, idx.Table + "_data", idx.Table + "_idx", idx.Table + "_content", idx.Table + "_docsize", idx.Table + "_config"}
		schema := map[string]string{}
		for _, name := range names {
			var create string
			if err = expected.QueryRow("SELECT sql FROM sqlite_schema WHERE name=? AND type='table'", name).Scan(&create); err != nil {
				return nil, err
			}
			schema[name] = create
			cols, err := expected.Query("PRAGMA table_info(" + quote(name) + ")")
			if err != nil {
				return nil, err
			}
			colset := map[string]bool{"": true}
			for cols.Next() {
				var cid, nn, pk int
				var col, typ string
				var def any
				if err = cols.Scan(&cid, &col, &typ, &nn, &def, &pk); err != nil {
					cols.Close()
					return nil, err
				}
				colset[strings.ToLower(col)] = true
			}
			err = cols.Err()
			cols.Close()
			if err != nil {
				return nil, err
			}
			allowed[strings.ToLower(name)] = colset
		}
		allowed[strings.ToLower(idx.Table)][strings.ToLower(idx.Table)] = true // FTS hidden table argument.
		allowed[strings.ToLower(idx.Table)]["rowid"] = true
		source := strings.ToLower(idx.Collection)
		if allowed[source] == nil {
			allowed[source] = map[string]bool{}
		}
		allowed[source]["id"] = true
		if idx.Scope != nil {
			for _, pair := range [][2]string{{idx.Scope.Collection, idx.Scope.Field}, {idx.Generation.Collection, idx.Generation.Field}} {
				name := strings.ToLower(pair[0])
				if allowed[name] == nil {
					allowed[name] = map[string]bool{}
				}
				allowed[name]["id"] = true
				allowed[name][strings.ToLower(pair[1])] = true
			}
		}
		weights := []string{"0"}
		snip := 0
		for n, c := range idx.Columns {
			w := 1.0
			if len(idx.Weights) > 0 {
				w = idx.Weights[n]
			}
			weights = append(weights, fmt.Sprintf("%g", w))
			if c == idx.SnippetColumn {
				snip = n + 1
			}
		}
		table := quote(idx.Table)
		search := "SELECT record_id, bm25(" + table + "," + strings.Join(weights, ",") + "), snippet(" + table + fmt.Sprintf(",%d,'','',' … ',32) FROM ", snip) + table + " WHERE " + table + " MATCH ? ORDER BY 2, record_id LIMIT ?"
		if idx.Scope != nil {
			search = strings.Replace(search, " ORDER BY", " AND record_id IN (SELECT value FROM json_each(?)) ORDER BY", 1) + " OFFSET ?"
		}
		// Canonical FTS DDL and exact shadow DDL establish that _content.c0
		// stores record_id. Validate the same corpus directly, avoiding FTS's
		// per-row content lookup during an otherwise unindexed virtual-table scan.
		// These fixed internal reads never enter the arbitrary SQL reader.
		content := quote(idx.Table + "_content")
		validIDs := "SELECT 1 FROM " + content + " WHERE typeof(c0) != 'text' OR length(trim(c0))=0 OR NOT pocketcontext_utf8_valid(c0) OR length(CAST(c0 AS BLOB))>128 OR NOT EXISTS (SELECT 1 FROM " + quote(idx.Collection) + " WHERE id=" + content + ".c0) UNION ALL SELECT 1 FROM " + content + " GROUP BY c0 HAVING count(*)>1 LIMIT 1"
		e.indexes[alias] = definition{index: idx, schema: schema, search: search, validIDs: validIDs}
	}
	// Prevent one configured index/source name from impersonating another shadow.
	for _, def := range e.indexes {
		for _, other := range e.indexes {
			for name := range def.schema {
				if strings.EqualFold(name, other.index.Collection) || (other.index.Scope != nil && (strings.EqualFold(name, other.index.Scope.Collection) || strings.EqualFold(name, other.index.Generation.Collection))) {
					return nil, fmt.Errorf("%w: source/index name collision", ErrIndexInvalid)
				}
			}
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_busy_timeout", "100")
	u.RawQuery = q.Encode()
	allow := func(op int, a, b, db string) int {
		switch op {
		case sqlite3.SQLITE_SELECT, sqlite3.SQLITE_TRANSACTION:
			return sqlite3.SQLITE_OK
		case sqlite3.SQLITE_READ:
			if (db == "main" || (db == "" && (b == "" || strings.EqualFold(a, "json_each")))) && allowed[strings.ToLower(a)][strings.ToLower(b)] {
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_FUNCTION:
			switch strings.ToLower(b) {
			case "match", "bm25", "snippet", "typeof", "length", "trim", "count", "pocketcontext_utf8_valid", "lower":
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_PRAGMA:
			if strings.EqualFold(a, "data_version") && b == "" && db == "main" {
				return sqlite3.SQLITE_OK
			}
		}
		return sqlite3.SQLITE_DENY
	}
	e.db = sql.OpenDB(connector{dsn: u.String(), allow: allow, maxBytes: limits.MaxBytes})
	e.db.SetMaxOpenConns(4)
	e.db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), limits.Timeout)
	defer cancel()
	for _, alias := range aliases {
		tx, err := e.db.BeginTx(ctx, nil)
		if err == nil {
			err = e.validate(ctx, tx, e.indexes[alias])
			if rollback := tx.Rollback(); err == nil {
				err = rollback
			}
		}
		if err != nil {
			e.Close()
			return nil, fmt.Errorf("%w: %v", ErrIndexInvalid, err)
		}
	}
	return e, nil
}
func (e *Engine) Close() error { return e.db.Close() }
func (e *Engine) validate(ctx context.Context, tx *sql.Tx, d definition) error {
	var reserved int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE lower(name)='json_each'").Scan(&reserved); err != nil {
		return fmt.Errorf("%w: schema unavailable: %w", ErrIndexInvalid, err)
	}
	if reserved != 0 {
		return ErrIndexInvalid
	}
	sources := []string{d.index.Collection}
	if d.index.Scope != nil {
		sources = append(sources, d.index.Scope.Collection, d.index.Generation.Collection)
	}
	for _, source := range sources {
		var sourceDDL string
		if err := tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", source).Scan(&sourceDDL); err != nil {
			return fmt.Errorf("%w: source schema unavailable: %w", ErrIndexInvalid, err)
		}
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sourceDDL)), "CREATE TABLE") {
			return fmt.Errorf("%w: source must be an ordinary table", ErrIndexInvalid)
		}
	}
	names := make([]string, 0, len(d.schema))
	for name := range d.schema {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var ddl string
		if err := tx.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", name).Scan(&ddl); err != nil {
			return fmt.Errorf("%w: schema unavailable: %w", ErrIndexInvalid, err)
		}
		if ddl != d.schema[name] {
			return fmt.Errorf("%w: schema changed", ErrIndexInvalid)
		}
	}
	var found int
	err := tx.QueryRowContext(ctx, d.validIDs).Scan(&found)
	if err == nil {
		return fmt.Errorf("%w: record IDs must be unique nonempty text", ErrIndexInvalid)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return nil
}
func (e *Engine) Search(ctx context.Context, index, query string, limit int) (Result, error) {
	return e.SearchRequest(ctx, Request{Index: index, Query: query, Limit: limit})
}
func (e *Engine) SearchRequest(ctx context.Context, req Request) (Result, error) {
	index, query, limit := req.Index, req.Query, req.Limit
	result := Result{Index: index, Hits: []Hit{}}
	d, ok := e.indexes[index]
	if !ok {
		return result, fmt.Errorf("%w: unknown index", ErrInvalidQuery)
	}
	if d.index.Scope == nil {
		if req.Scope != "" || req.Offset != 0 || req.ExpectedGeneration != "" {
			return result, ErrInvalidQuery
		}
	} else if !validToken(req.Scope) || req.Offset < 0 || req.Offset > MaxOffset || (req.Offset > 0 && req.ExpectedGeneration == "") || (req.ExpectedGeneration != "" && !validToken(req.ExpectedGeneration)) {
		return result, ErrInvalidQuery
	}
	if len(query) > MaxQueryBytes || !utf8.ValidString(query) || strings.IndexByte(query, 0) >= 0 {
		return result, ErrInvalidQuery
	}
	terms := strings.Fields(query)
	if len(terms) == 0 || len(terms) > MaxTerms {
		return result, ErrInvalidQuery
	}
	for n, term := range terms {
		terms[n] = `"` + strings.ReplaceAll(term, `"`, `""`) + `"`
	}
	if limit == 0 {
		limit = min(DefaultLimit, e.limits.MaxRows)
	}
	if limit < 1 || limit > e.limits.MaxRows {
		return result, ErrInvalidQuery
	}
	ctx, cancel := context.WithTimeout(ctx, e.limits.Timeout)
	defer cancel()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return result, classify(err)
	}
	defer tx.Rollback()
	if err = e.validate(ctx, tx, d); err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, classify(err)
	}
	args := []any{strings.Join(terms, " AND ")}
	if d.index.Scope != nil {
		membership, generation, err := readScope(ctx, tx, d.index, req.Scope)
		if err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			return result, classify(err)
		}
		generation = exposedGeneration(d.index, generation, req.Scope, membership)
		if req.ExpectedGeneration != "" && req.ExpectedGeneration != generation {
			return result, ErrGenerationConflict
		}
		result.Scope, result.Generation = req.Scope, generation
		args = append(args, membership, limit+1, req.Offset)
	} else {
		args = append(args, limit+1)
	}
	rows, err := tx.QueryContext(ctx, d.search, args...)
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, classify(err)
	}
	defer rows.Close()
	for rows.Next() {
		if len(result.Hits) == limit {
			result.Truncated = true
			result.HasMore = true
			break
		}
		var hit Hit
		var excerpt sql.NullString
		if err = rows.Scan(&hit.ID, &hit.Score, &excerpt); err != nil {
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			return Result{}, classify(err)
		}
		hit.Excerpt = excerpt.String
		if !utf8.ValidString(hit.ID) || math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) {
			return Result{}, ErrIndexInvalid
		}
		result.Hits = append(result.Hits, hit)
		encoded, err := json.Marshal(result)
		if err != nil {
			return Result{}, err
		}
		if len(encoded) > e.limits.MaxBytes {
			return Result{}, ErrResponseLimit
		}
	}
	if ctx.Err() != nil {
		return Result{}, ctx.Err()
	}
	if err = rows.Err(); err != nil {
		return Result{}, classify(err)
	}
	encoded, _ := json.Marshal(result)
	if len(encoded) > e.limits.MaxBytes {
		return Result{}, ErrResponseLimit
	}
	return result, nil
}

func validToken(s string) bool {
	return len(s) > 0 && len(s) <= 128 && utf8.ValidString(s) && strings.TrimSpace(s) != "" && !strings.ContainsRune(s, 0)
}

// Decode token-by-token so duplicate JSON object keys cannot disappear silently.
func scopeMembers(raw string) ([]string, error) {
	if len(raw) > MaxScopeBytes || !utf8.ValidString(raw) || !validJSONEscapes(raw) {
		return nil, ErrIndexInvalid
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	start, err := dec.Token()
	if err != nil || start != json.Delim('{') {
		return nil, ErrIndexInvalid
	}
	keys := map[string]bool{}
	values := []string{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, ErrIndexInvalid
		}
		k, ok := key.(string)
		if !ok || !validToken(k) || keys[k] || len(keys) >= MaxScopeMembers {
			return nil, ErrIndexInvalid
		}
		keys[k] = true
		var v string
		if err := dec.Decode(&v); err != nil || !validToken(v) {
			return nil, ErrIndexInvalid
		}
		values = append(values, v)
	}
	if end, err := dec.Token(); err != nil || end != json.Delim('}') {
		return nil, ErrIndexInvalid
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, ErrIndexInvalid
	}
	return values, nil
}
func readScope(ctx context.Context, tx *sql.Tx, idx Index, scope string) (string, string, error) {
	var raw, generation, generationType string
	if err := tx.QueryRowContext(ctx, "SELECT "+quote(idx.Scope.Field)+" FROM "+quote(idx.Scope.Collection)+" WHERE id=?", scope).Scan(&raw); err != nil {
		return "", "", fmt.Errorf("%w: scope unavailable: %w", ErrIndexInvalid, err)
	}
	values, err := scopeMembers(raw)
	if err != nil {
		return "", "", err
	}
	if err = tx.QueryRowContext(ctx, "SELECT "+quote(idx.Generation.Field)+", typeof("+quote(idx.Generation.Field)+") FROM "+quote(idx.Generation.Collection)+" WHERE id=?", idx.Generation.Record).Scan(&generation, &generationType); err != nil {
		return "", "", fmt.Errorf("%w: generation unavailable: %w", ErrIndexInvalid, err)
	}
	if generationType != "text" || !validToken(generation) {
		return "", "", ErrIndexInvalid
	}
	sort.Strings(values)
	data, _ := json.Marshal(values)
	var missing int
	err = tx.QueryRowContext(ctx, "SELECT count(*) FROM json_each(?) WHERE NOT EXISTS (SELECT 1 FROM "+quote(idx.Collection)+" WHERE id=json_each.value)", string(data)).Scan(&missing)
	if err != nil {
		return "", "", err
	}
	if missing != 0 {
		return "", "", ErrIndexInvalid
	}
	return string(data), generation, nil
}

// encoding/json replaces unpaired UTF-16 escapes with U+FFFD. Reject them
// instead: corrupted membership must not select an actual replacement-character ID.
func validJSONEscapes(raw string) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		code, err := strconv.ParseUint(raw[i+1:i+5], 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if code >= 0xd800 && code <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1:i+3] != `\u` {
				return false
			}
			low, err := strconv.ParseUint(raw[i+3:i+7], 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		} else if code >= 0xdc00 && code <= 0xdfff {
			return false
		}
	}
	return true
}

// Bump this contract version whenever ranking or query semantics change. The
// token is a consistency fingerprint, not an authorization credential or MAC.
const rankingContractVersion = "literal-and-bm25-id-v1"

func exposedGeneration(idx Index, raw, scope, membership string) string {
	version, number, source := sqlite3.Version()
	data, _ := json.Marshal([]any{rankingContractVersion, version, number, source, idx, raw, scope, membership})
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}
