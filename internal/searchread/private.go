package searchread

import (
	"container/list"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

const (
	IsolationPrivate = "private"
	MembersAll       = "all"
	MembersVisible   = "visible"

	DefaultMaxIndexBytes = 8 << 20
	MaxMaxIndexBytes     = 64 << 20
	MinMaxIndexBytes     = 1024
	DefaultCacheBytes    = 64 << 20
	MaxCacheBytes        = 1 << 30
	MaxCacheEntries      = 64

	// privateTable is the fixed internal FTS5 table of every per-request index.
	privateTable = "private_search"
	// Concurrent in-memory index builds; queries on cached indexes do not wait.
	privateBuilds = 2
	// Bound retained authorized rows and evicted-but-active indexes too.
	privateRequests = 4
)

// Bump whenever private ranking, tokenization, query or fingerprint semantics
// change. Independent of the shared rankingContractVersion.
const privateRankingContractVersion = "private-literal-and-bm25-id-v1"

// IsPrivate reports whether an index uses per-requester isolation.
func IsPrivate(i Index) bool { return i.Isolation == IsolationPrivate }

// NormalizePrivateConfig applies private-search defaults and validates that
// every index is a well-formed private index.
func NormalizePrivateConfig(c *Config) error {
	if c == nil || len(c.Indexes) == 0 || len(c.Indexes) > 16 {
		return fmt.Errorf("%w: index count", ErrIndexInvalid)
	}
	if c.MaxIndexBytes == 0 {
		c.MaxIndexBytes = DefaultMaxIndexBytes
	}
	if c.CacheBytes == 0 {
		c.CacheBytes = DefaultCacheBytes
	}
	if c.MaxIndexBytes < MinMaxIndexBytes || c.MaxIndexBytes > MaxMaxIndexBytes {
		return fmt.Errorf("%w: maxIndexBytes must be %d..%d", ErrIndexInvalid, MinMaxIndexBytes, MaxMaxIndexBytes)
	}
	if c.CacheBytes < c.MaxIndexBytes || c.CacheBytes > MaxCacheBytes {
		return fmt.Errorf("%w: cacheBytes must be maxIndexBytes..%d", ErrIndexInvalid, MaxCacheBytes)
	}
	for alias, idx := range c.Indexes {
		if !validName(alias) {
			return fmt.Errorf("%w: index alias %q", ErrIndexInvalid, alias)
		}
		if err := validatePrivateIndex(idx); err != nil {
			return fmt.Errorf("search index %q: %w", alias, err)
		}
	}
	return nil
}

func validatePrivateIndex(i Index) error {
	if !IsPrivate(i) {
		return fmt.Errorf("%w: filtered snapshot search requires isolation \"private\"", ErrIndexInvalid)
	}
	if i.Table != "" || i.Generation != nil {
		return fmt.Errorf("%w: private index must not set table or generation", ErrIndexInvalid)
	}
	if i.Scope == nil {
		if i.Members != "" {
			return fmt.Errorf("%w: members requires scope", ErrIndexInvalid)
		}
	} else {
		if !validName(i.Scope.Collection) || !validName(i.Scope.Field) {
			return fmt.Errorf("%w: scope identifiers", ErrIndexInvalid)
		}
		if i.Members != "" && i.Members != MembersAll && i.Members != MembersVisible {
			return fmt.Errorf("%w: members must be \"all\" or \"visible\"", ErrIndexInvalid)
		}
	}
	if i.Where != "" && strings.TrimSpace(i.Where) == "" {
		return fmt.Errorf("%w: empty where predicate", ErrIndexInvalid)
	}
	for _, c := range i.Columns {
		if strings.EqualFold(c, privateTable) {
			return fmt.Errorf("%w: column names", ErrIndexInvalid)
		}
	}
	_, err := CanonicalDDL(privateShape(i))
	return err
}

// privateShape is the canonical FTS shape used for a private index.
func privateShape(i Index) Index {
	return Index{Table: privateTable, Collection: i.Collection, Columns: i.Columns, SnippetColumn: i.SnippetColumn, Weights: i.Weights}
}

type privateDefinition struct {
	index               Index
	rows, scope         *sqlread.FilteredRead
	ddl, insert, search string
}

// PrivateEngine searches per-request in-memory indexes built only from rows
// that a filtered snapshot source authorizes for the requester.
type PrivateEngine struct {
	source        *sqlread.SnapshotSource
	indexes       map[string]privateDefinition
	limits        Limits
	maxIndexBytes int
	cache         *ftsCache
	builds        chan struct{}
	requests      chan struct{}
}

// NewPrivate validates private indexes against source. Every source column
// and scope field must be a configured snapshot output column.
func NewPrivate(source *sqlread.SnapshotSource, cfg Config, limits Limits) (*PrivateEngine, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: private search requires a filtered snapshot source", ErrIndexInvalid)
	}
	if err := NormalizePrivateConfig(&cfg); err != nil {
		return nil, err
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
	scratch, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	defer scratch.Close()
	scratch.SetMaxOpenConns(1)
	e := &PrivateEngine{source: source, indexes: map[string]privateDefinition{}, limits: limits, maxIndexBytes: cfg.MaxIndexBytes, cache: newFTSCache(cfg.CacheBytes, MaxCacheEntries), builds: make(chan struct{}, privateBuilds), requests: make(chan struct{}, privateRequests)}
	for alias, idx := range cfg.Indexes {
		idx.Columns = append([]string(nil), idx.Columns...)
		idx.Weights = append([]float64(nil), idx.Weights...)
		if idx.Scope != nil {
			copied := *idx.Scope
			idx.Scope = &copied
			if idx.Members == "" {
				idx.Members = MembersAll
			}
		}
		shape := privateShape(idx)
		ddl, err := CanonicalDDL(shape)
		if err != nil {
			return nil, err
		}
		cols := []string{`"record_id"`}
		marks := []string{"?"}
		weights := []string{"0"}
		snip := 0
		for n, c := range idx.Columns {
			cols = append(cols, quote(c))
			marks = append(marks, "?")
			w := 1.0
			if len(idx.Weights) > 0 {
				w = idx.Weights[n]
			}
			weights = append(weights, fmt.Sprintf("%g", w))
			if c == idx.SnippetColumn {
				snip = n + 1
			}
		}
		table := quote(privateTable)
		def := privateDefinition{
			index:  idx,
			ddl:    ddl,
			insert: "INSERT INTO " + table + "(" + strings.Join(cols, ",") + ") VALUES(" + strings.Join(marks, ",") + ")",
			// Same fixed ranking and snippet query as shared scoped search.
			search: "SELECT record_id, bm25(" + table + "," + strings.Join(weights, ",") + "), snippet(" + table + fmt.Sprintf(",%d,'','',' … ',32) FROM ", snip) + table + " WHERE " + table + " MATCH ? ORDER BY 2, record_id LIMIT ? OFFSET ?",
		}
		if err := func() error {
			conn, err := scratch.Conn(context.Background())
			if err != nil {
				return err
			}
			defer conn.Close()
			if _, err = conn.ExecContext(context.Background(), ddl); err != nil {
				return err
			}
			defer conn.ExecContext(context.Background(), "DROP TABLE "+table)
			for _, q := range []string{def.insert, def.search} {
				stmt, err := conn.PrepareContext(context.Background(), q)
				if err != nil {
					return err
				}
				stmt.Close()
			}
			return nil
		}(); err != nil {
			return nil, fmt.Errorf("%w: search index %q: %v", ErrIndexInvalid, alias, err)
		}
		def.rows, err = source.PrepareFilteredRead(sqlread.FilteredReadSpec{Table: idx.Collection, Columns: append([]string{"id"}, idx.Columns...), Where: idx.Where})
		if err != nil {
			return nil, fmt.Errorf("search index %q: %w", alias, err)
		}
		if idx.Scope != nil {
			def.scope, err = source.PrepareFilteredRead(sqlread.FilteredReadSpec{Table: idx.Scope.Collection, Columns: []string{idx.Scope.Field}})
			if err != nil {
				return nil, fmt.Errorf("search index %q scope: %w", alias, err)
			}
		}
		e.indexes[alias] = def
	}
	return e, nil
}

func (e *PrivateEngine) Close() error {
	e.cache.close()
	return nil
}

type privateRow struct {
	id     string
	values []string
}

// SearchAs searches index content visible to requester. Authorization reads
// run on every request before any cache lookup.
func (e *PrivateEngine) SearchAs(ctx context.Context, requester string, req Request) (Result, error) {
	result := Result{Index: req.Index, Hits: []Hit{}}
	d, ok := e.indexes[req.Index]
	if !ok {
		return result, fmt.Errorf("%w: unknown index", ErrInvalidQuery)
	}
	if d.index.Scope == nil {
		if req.Scope != "" {
			return result, ErrInvalidQuery
		}
	} else if !validToken(req.Scope) {
		return result, ErrInvalidQuery
	}
	if req.Offset < 0 || req.Offset > MaxOffset || (req.Offset > 0 && req.ExpectedGeneration == "") || (req.ExpectedGeneration != "" && !validToken(req.ExpectedGeneration)) {
		return result, ErrInvalidQuery
	}
	match, err := matchExpression(req.Query)
	if err != nil {
		return result, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = min(DefaultLimit, e.limits.MaxRows)
	}
	if limit < 1 || limit > e.limits.MaxRows {
		return result, ErrInvalidQuery
	}
	if requester == "" {
		return result, ErrInvalidQuery
	}
	// Admission precedes reading content, so queued requests retain no corpus.
	waitCtx, waitCancel := context.WithTimeout(ctx, e.limits.Timeout)
	select {
	case e.requests <- struct{}{}:
		waitCancel()
	case <-waitCtx.Done():
		err := waitCtx.Err()
		waitCancel()
		return result, err
	}
	defer func() { <-e.requests }()
	rows, err := e.authorizedRows(ctx, requester, d, req.Scope)
	if err != nil {
		return result, err
	}
	generation := privateFingerprint(d.index, req.Scope, rows)
	if req.ExpectedGeneration != "" && req.ExpectedGeneration != generation {
		return result, ErrGenerationConflict
	}
	result.Scope, result.Generation = req.Scope, generation
	ctx, cancel := context.WithTimeout(ctx, e.limits.Timeout)
	defer cancel()
	entry := e.cache.acquire(generation)
	if entry == nil {
		buildDone := tracing.Start(ctx, "search.private_build")
		built, err := e.build(ctx, d, rows)
		buildDone()
		if err != nil {
			if ctx.Err() != nil {
				return Result{Index: req.Index, Hits: []Hit{}}, ctx.Err()
			}
			return Result{Index: req.Index, Hits: []Hit{}}, err
		}
		entry = e.cache.insert(generation, built)
	}
	defer e.cache.release(entry)
	queryDone := tracing.Start(ctx, "search.private_query")
	hits, more, err := e.query(ctx, entry, d, match, limit, req.Offset)
	queryDone()
	if err != nil {
		// A failed pinned connection is never reused or replaced silently.
		e.cache.remove(entry)
		if ctx.Err() != nil {
			return Result{Index: req.Index, Hits: []Hit{}}, ctx.Err()
		}
		return Result{Index: req.Index, Hits: []Hit{}}, err
	}
	for _, hit := range hits {
		result.Hits = append(result.Hits, hit)
		if encoded, _ := json.Marshal(result); len(encoded) > e.limits.MaxBytes {
			return Result{Index: req.Index, Hits: []Hit{}}, ErrResponseLimit
		}
	}
	result.HasMore, result.Truncated = more, more
	if encoded, _ := json.Marshal(result); len(encoded) > e.limits.MaxBytes {
		return Result{Index: req.Index, Hits: []Hit{}}, ErrResponseLimit
	}
	return result, nil
}

func matchExpression(query string) (string, error) {
	if len(query) > MaxQueryBytes || !utf8.ValidString(query) || strings.IndexByte(query, 0) >= 0 {
		return "", ErrInvalidQuery
	}
	terms := strings.Fields(query)
	if len(terms) == 0 || len(terms) > MaxTerms {
		return "", ErrInvalidQuery
	}
	for n, term := range terms {
		terms[n] = `"` + strings.ReplaceAll(term, `"`, `""`) + `"`
	}
	return strings.Join(terms, " AND "), nil
}

// authorizedRows reads the scope and source rows in one filtered snapshot
// read transaction. Missing and invisible scopes are indistinguishable.
func (e *PrivateEngine) authorizedRows(ctx context.Context, requester string, d privateDefinition, scope string) ([]privateRow, error) {
	var rows []privateRow
	err := e.source.ReadFiltered(ctx, requester, func(tx *sqlread.FilteredTx) error {
		rows = rows[:0]
		var ids []string
		if d.index.Scope != nil {
			found := 0
			var raw any
			if err := tx.Read(d.scope, []string{scope}, func(values []any) error {
				found++
				raw = values[0]
				return nil
			}); err != nil {
				return err
			}
			if found == 0 {
				return ErrScopeNotFound
			}
			text, ok := raw.(string)
			if found != 1 || !ok {
				return fmt.Errorf("%w: scope membership", ErrIndexInvalid)
			}
			members, err := scopeMembers(text)
			if err != nil {
				return err
			}
			unique := map[string]bool{}
			ids = make([]string, 0, len(members))
			for _, id := range members {
				if !unique[id] {
					unique[id] = true
					ids = append(ids, id)
				}
			}
			sort.Strings(ids)
		}
		seen := map[string]bool{}
		size := 0
		if err := tx.Read(d.rows, ids, func(values []any) error {
			id, ok := values[0].(string)
			if !ok || !validToken(id) || seen[id] {
				return fmt.Errorf("%w: record IDs must be unique nonempty text", ErrIndexInvalid)
			}
			seen[id] = true
			size += len(id)
			row := privateRow{id: id, values: make([]string, len(values)-1)}
			for i, v := range values[1:] {
				switch x := v.(type) {
				case nil:
				case string:
					if !utf8.ValidString(x) {
						return fmt.Errorf("%w: indexed values must be UTF-8 text", ErrIndexInvalid)
					}
					row.values[i] = x
					size += len(x)
				default:
					return fmt.Errorf("%w: indexed values must be text or null", ErrIndexInvalid)
				}
			}
			if size > e.maxIndexBytes {
				return fmt.Errorf("%w: maxIndexBytes", ErrIndexLimit)
			}
			rows = append(rows, row)
			return nil
		}); err != nil {
			return err
		}
		if ids != nil && d.index.Members == MembersAll && len(rows) != len(ids) {
			// Never return partial results for a scope that requires all members.
			return fmt.Errorf("%w: scope member unavailable", ErrIndexInvalid)
		}
		return nil
	})
	switch {
	case err == nil:
	case errors.Is(err, ErrScopeNotFound), errors.Is(err, ErrIndexInvalid), errors.Is(err, ErrIndexLimit), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return nil, err
	case errors.Is(err, sqlread.ErrSnapshotLimit):
		return nil, fmt.Errorf("%w: snapshot limits", ErrIndexLimit)
	case errors.Is(err, sqlread.ErrBusy):
		return nil, fmt.Errorf("%w: %w", ErrBusy, err)
	default:
		return nil, fmt.Errorf("%w: authorized read failed", ErrIndexInvalid)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	return rows, nil
}

// privateFingerprint identifies exactly the content and configuration from
// which a private index is built. It is the exposed generation token and the
// cache key, so neither depends on rows hidden from the requester.
func privateFingerprint(idx Index, scope string, rows []privateRow) string {
	version, number, source := sqlite3.Version()
	var scopeMarker any // JSON null marks a scope-less index.
	if idx.Scope != nil {
		scopeMarker = scope
	}
	h := sha256.New()
	header, _ := json.Marshal([]any{privateRankingContractVersion, version, number, source, idx, scopeMarker, len(rows)})
	h.Write(header)
	for _, row := range rows {
		values := make([]any, 0, len(row.values)+1)
		values = append(values, row.id)
		for _, v := range row.values {
			values = append(values, v)
		}
		encoded, _ := json.Marshal(values)
		h.Write(encoded)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (e *PrivateEngine) build(ctx context.Context, d privateDefinition, rows []privateRow) (*ftsEntry, error) {
	select {
	case e.builds <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-e.builds }()
	raw, err := (&sqlite3.SQLiteDriver{}).Open(":memory:")
	if err != nil {
		return nil, err
	}
	conn := raw.(*sqlite3.SQLiteConn)
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	conn.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, e.maxIndexBytes+e.limits.MaxBytes+65536)
	conn.SetLimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 65536)
	conn.SetLimit(sqlite3.SQLITE_LIMIT_COLUMN, 256)
	conn.SetLimit(sqlite3.SQLITE_LIMIT_EXPR_DEPTH, 100)
	conn.SetLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
	// Content and index together stay well below four times the indexed text.
	pages := e.maxIndexBytes*4/4096 + 256
	fail := func(err error) (*ftsEntry, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var se sqlite3.Error
		if errors.As(err, &se) && (se.Code == sqlite3.ErrFull || se.Code == sqlite3.ErrTooBig) {
			return nil, fmt.Errorf("%w: in-memory index size", ErrIndexLimit)
		}
		return nil, classify(err)
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=4096; PRAGMA max_page_count=%d; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY", pages), nil); err != nil {
		return fail(err)
	}
	if _, err = conn.ExecContext(ctx, d.ddl, nil); err != nil {
		return fail(err)
	}
	if _, err = conn.ExecContext(ctx, "BEGIN", nil); err != nil {
		return fail(err)
	}
	stmt, err := conn.Prepare(d.insert)
	if err != nil {
		return fail(err)
	}
	insert := stmt.(driver.StmtExecContext)
	args := make([]driver.NamedValue, len(d.index.Columns)+1)
	for _, row := range rows {
		args[0] = driver.NamedValue{Ordinal: 1, Value: row.id}
		for i, v := range row.values {
			args[i+1] = driver.NamedValue{Ordinal: i + 2, Value: v}
		}
		if _, err = insert.ExecContext(ctx, args); err != nil {
			stmt.Close()
			return fail(err)
		}
	}
	stmt.Close()
	if _, err = conn.ExecContext(ctx, "COMMIT", nil); err != nil {
		return fail(err)
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+quote(privateTable)+"("+quote(privateTable)+") VALUES('optimize')", nil); err != nil {
		return fail(err)
	}
	pageCount, err := queryInt(ctx, conn, "PRAGMA page_count")
	if err != nil {
		return fail(err)
	}
	if _, err = conn.ExecContext(ctx, "PRAGMA query_only=1", nil); err != nil {
		return fail(err)
	}
	conn.RegisterAuthorizer(func(op int, a, b, db string) int {
		switch op {
		case sqlite3.SQLITE_SELECT:
			return sqlite3.SQLITE_OK
		case sqlite3.SQLITE_READ:
			if db == "main" || db == "" {
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_FUNCTION:
			switch strings.ToLower(b) {
			case "match", "bm25", "snippet":
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_PRAGMA:
			// FTS5 checks for concurrent changes with data_version.
			if strings.EqualFold(a, "data_version") && b == "" && db == "main" {
				return sqlite3.SQLITE_OK
			}
		}
		return sqlite3.SQLITE_DENY
	})
	size := 0
	for _, row := range rows {
		size += len(row.id)
		for _, v := range row.values {
			size += len(v)
		}
	}
	success = true
	return &ftsEntry{conn: conn, size: max(size, int(pageCount)*4096), queries: make(chan struct{}, 1)}, nil
}

func queryInt(ctx context.Context, conn *sqlite3.SQLiteConn, query string) (int64, error) {
	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	dest := make([]driver.Value, 1)
	if err = rows.Next(dest); err != nil {
		return 0, err
	}
	n, ok := dest[0].(int64)
	if !ok {
		return 0, ErrIndexInvalid
	}
	return n, nil
}

func (e *PrivateEngine) query(ctx context.Context, entry *ftsEntry, d privateDefinition, match string, limit, offset int) ([]Hit, bool, error) {
	select {
	case entry.queries <- struct{}{}:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	defer func() { <-entry.queries }()
	if entry.conn == nil {
		return nil, false, fmt.Errorf("%w: private index connection lost", ErrIndexInvalid)
	}
	rows, err := entry.conn.QueryContext(ctx, d.search, []driver.NamedValue{{Ordinal: 1, Value: match}, {Ordinal: 2, Value: int64(limit + 1)}, {Ordinal: 3, Value: int64(offset)}})
	if err != nil {
		return nil, false, classify(err)
	}
	defer rows.Close()
	hits := []Hit{}
	dest := make([]driver.Value, 3)
	for {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false, classify(err)
		}
		if len(hits) == limit {
			return hits, true, nil
		}
		id, ok := dest[0].(string)
		score, scored := dest[1].(float64)
		if !ok || !scored || !utf8.ValidString(id) || math.IsNaN(score) || math.IsInf(score, 0) {
			return nil, false, ErrIndexInvalid
		}
		hit := Hit{ID: id, Score: score}
		switch x := dest[2].(type) {
		case nil:
		case string:
			hit.Excerpt = x
		default:
			return nil, false, ErrIndexInvalid
		}
		hits = append(hits, hit)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	return hits, false, nil
}

// ftsEntry is one pinned in-memory index connection. Queries serialize on queries.
// The connection closes only after eviction and the last release.
type ftsEntry struct {
	queries chan struct{}
	conn    *sqlite3.SQLiteConn
	key     string
	size    int
	refs    int
	evicted bool
	elem    *list.Element
}

// ftsCache is an LRU of private indexes bounded by bytes and entry count.
type ftsCache struct {
	mu                   sync.Mutex
	maxBytes, maxEntries int
	bytes                int
	lru                  *list.List
	entries              map[string]*ftsEntry
	closed               bool
}

func newFTSCache(maxBytes, maxEntries int) *ftsCache {
	return &ftsCache{maxBytes: maxBytes, maxEntries: maxEntries, lru: list.New(), entries: map[string]*ftsEntry{}}
}

func (c *ftsCache) acquire(key string) *ftsEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil
	}
	entry.refs++
	c.lru.MoveToFront(entry.elem)
	return entry
}

// insert caches a newly built entry and returns an acquired entry for key.
// Entries larger than the whole cache are used once, then closed.
func (c *ftsCache) insert(key string, built *ftsEntry) *ftsEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	built.key = key
	built.refs = 1
	if existing := c.entries[key]; existing != nil {
		existing.refs++
		c.lru.MoveToFront(existing.elem)
		built.evicted = true
		built.refs = 0
		closeEntry(built)
		return existing
	}
	if c.closed || built.size > c.maxBytes {
		built.evicted = true
		return built
	}
	built.elem = c.lru.PushFront(built)
	c.entries[key] = built
	c.bytes += built.size
	for c.bytes > c.maxBytes || len(c.entries) > c.maxEntries {
		c.evictLocked(c.lru.Back().Value.(*ftsEntry))
	}
	return built
}

func (c *ftsCache) evictLocked(entry *ftsEntry) {
	if entry.evicted {
		return
	}
	entry.evicted = true
	if entry.elem != nil {
		c.lru.Remove(entry.elem)
		entry.elem = nil
	}
	delete(c.entries, entry.key)
	c.bytes -= entry.size
	if entry.refs == 0 {
		closeEntry(entry)
	}
}

func (c *ftsCache) release(entry *ftsEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.refs--
	if entry.evicted && entry.refs == 0 {
		closeEntry(entry)
	}
}

func (c *ftsCache) remove(entry *ftsEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked(entry)
}

func (c *ftsCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, entry := range c.entries {
		c.evictLocked(entry)
	}
}

// stats reports cached entries and charged bytes.
func (c *ftsCache) stats() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.bytes
}

func closeEntry(entry *ftsEntry) {
	// The cache lock is held and no requests retain this entry.
	if entry.conn != nil {
		entry.conn.Close()
		entry.conn = nil
	}
}
