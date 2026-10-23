package searchread

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

// prepareSnapshotIndexes runs before the SQL generation becomes available.
// Scope selection belongs to application configuration; the generic server has
// no knowledge of publications, spaces, or what an application calls current.
func (e *PrivateEngine) prepareSnapshotIndexes(ctx context.Context, lease *sqlread.SnapshotLease) error {
	aliases := make([]string, 0, len(e.indexes))
	for alias := range e.indexes {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		d := e.indexes[alias]
		if d.index.Scope == nil {
			if _, err := e.ensureSnapshotIndex(ctx, lease, alias, "", d); err != nil {
				return diskIndexError(ctx, err)
			}
			continue
		}
		if d.warmScopes == nil {
			continue
		}
		// Read callbacks may retain the snapshot connection. Finish selecting
		// bounded scope identities before opening reads for their members.
		scopes := []string{}
		if err := lease.Read(ctx, d.warmScopes, nil, func(values []any) error {
			id, ok := values[0].(string)
			if !ok || !validToken(id) || len(scopes) >= MaxScopeMembers {
				return ErrIndexInvalid
			}
			scopes = append(scopes, id)
			return nil
		}); err != nil {
			return err
		}
		sort.Strings(scopes)
		for _, scope := range scopes {
			if _, err := e.ensureSnapshotIndex(ctx, lease, alias, scope, d); err != nil {
				return diskIndexError(ctx, err)
			}
		}
	}
	return nil
}

func (e *PrivateEngine) searchSnapshot(ctx context.Context, requester string, req Request, d privateDefinition, match string, limit int) (Result, error) {
	result := Result{Index: req.Index, Scope: req.Scope, Hits: []Hit{}}
	lease, err := e.source.AcquireSnapshot(ctx, requester, sqlread.SnapshotReadOptions{
		Consistency: req.Consistency, AtLeast: req.AtLeast, ExpectedGeneration: req.SnapshotGeneration,
	})
	if err != nil {
		return result, err
	}
	defer lease.Close()
	meta := lease.Metadata()
	result.Snapshot = &meta
	buildCtx, buildCancel := context.WithTimeout(ctx, lease.BuildTimeout())
	path, err := e.ensureSnapshotIndex(buildCtx, lease, req.Index, req.Scope, d)
	buildErr := diskIndexError(buildCtx, err)
	buildCancel()
	if err != nil {
		return result, buildErr
	}
	meta = lease.Metadata()
	ctx, cancel := context.WithTimeout(ctx, e.limits.Timeout)
	defer cancel()
	conn, err := openSnapshotIndex(path)
	if err != nil {
		return result, diskIndexError(ctx, err)
	}
	defer conn.Close()
	generation, err := diskGeneration(ctx, conn)
	if err != nil {
		return result, diskIndexError(ctx, err)
	}
	if req.ExpectedGeneration != "" && req.ExpectedGeneration != generation {
		return result, ErrGenerationConflict
	}
	result.Generation = generation
	restrictPrivateIndex(conn)
	entry := &ftsEntry{conn: conn, queries: make(chan struct{}, 1)}
	done := tracing.Start(ctx, "search.snapshot_query")
	hits, more, err := e.query(ctx, entry, d, match, limit, req.Offset)
	done()
	if err != nil {
		return Result{Index: req.Index, Hits: []Hit{}, Snapshot: &meta}, diskIndexError(ctx, err)
	}
	result.Hits, result.HasMore, result.Truncated = hits, more, more
	if encoded, _ := json.Marshal(result); len(encoded) > e.limits.MaxBytes {
		return Result{Index: req.Index, Hits: []Hit{}, Snapshot: &meta}, ErrResponseLimit
	}
	return result, nil
}

// Files live inside the leased generation. There are no cached open handles:
// releasing the last SQL/search lease can remove the entire obsolete generation.
func (e *PrivateEngine) ensureSnapshotIndex(ctx context.Context, lease *sqlread.SnapshotLease, alias, scope string, d privateDefinition) (string, error) {
	key := sha256.Sum256([]byte(alias + "\x00" + scope))
	dir := lease.Directory()
	path := filepath.Join(dir, fmt.Sprintf("search-%x.db", key))
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if info, err := os.Lstat(path); err == nil {
			if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
				return "", ErrIndexInvalid
			}
			return path, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		// Serialize additions to one generation so every builder sees the full
		// existing disk charge; other generations can build concurrently.
		e.diskMu.Lock()
		if pending := e.diskBuilding[dir]; pending != nil {
			e.diskMu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		pending := make(chan struct{})
		e.diskBuilding[dir] = pending
		e.diskMu.Unlock()
		err := lease.WithDiskReservation(ctx, func(budget int64) error {
			return e.buildSnapshotIndex(ctx, lease, path, d, scope, budget)
		})
		e.diskMu.Lock()
		delete(e.diskBuilding, dir)
		close(pending)
		e.diskMu.Unlock()
		if err != nil {
			return "", err
		}
		return path, nil
	}
}

func snapshotScopeIDs(ctx context.Context, lease *sqlread.SnapshotLease, d privateDefinition, scope string) ([]string, error) {
	if d.snapshotScope == nil {
		return nil, nil
	}
	var raw any
	found := 0
	if err := lease.Read(ctx, d.snapshotScope, []string{scope}, func(values []any) error {
		found++
		raw = values[0]
		return nil
	}); err != nil {
		return nil, err
	}
	if found == 0 {
		return nil, ErrScopeNotFound
	}
	text, ok := raw.(string)
	if found != 1 || !ok {
		return nil, ErrIndexInvalid
	}
	members, err := scopeMembers(text)
	if err != nil {
		return nil, ErrIndexInvalid
	}
	unique := make(map[string]bool, len(members))
	ids := make([]string, 0, len(members))
	for _, id := range members {
		if !unique[id] {
			unique[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (e *PrivateEngine) buildSnapshotIndex(ctx context.Context, lease *sqlread.SnapshotLease, path string, d privateDefinition, scope string, budget int64) error {
	select {
	case e.builds <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-e.builds }()
	done := tracing.Start(ctx, "search.snapshot_build")
	defer done()
	ids, err := snapshotScopeIDs(ctx, lease, d, scope)
	if err != nil {
		return err
	}
	var used int64
	err = filepath.WalkDir(lease.Directory(), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return ErrIndexInvalid
		}
		used += info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	pages := (budget - used) / 4096
	if pages < 16 {
		return sqlread.ErrSnapshotResource
	}
	file, err := os.CreateTemp(lease.Directory(), ".search-building-")
	if err != nil {
		return err
	}
	tmp := file.Name()
	if err = file.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	defer os.Remove(tmp)
	u := url.URL{Scheme: "file", Path: tmp}
	raw, err := (&sqlite3.SQLiteDriver{}).Open(u.String())
	if err != nil {
		return err
	}
	conn := raw.(*sqlite3.SQLiteConn)
	defer conn.Close()
	conn.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, MaxMaxIndexBytes+e.limits.MaxBytes+65536)
	conn.SetLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=4096; PRAGMA max_page_count=%d; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA cache_size=-2048; PRAGMA mmap_size=0", pages), nil); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, d.ddl, nil); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "CREATE TABLE index_ids(id TEXT PRIMARY KEY, fts_rowid INTEGER NOT NULL) WITHOUT ROWID; CREATE TABLE index_metadata(generation TEXT NOT NULL)", nil); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "BEGIN", nil); err != nil {
		return err
	}
	cols, marks := []string{"rowid", "record_id"}, []string{"?", "?"}
	for _, col := range d.index.Columns {
		cols = append(cols, col)
		marks = append(marks, "?")
	}
	for i := range cols {
		cols[i] = quote(cols[i])
	}
	stmt, err := conn.Prepare("INSERT INTO " + quote(privateTable) + "(" + strings.Join(cols, ",") + ") VALUES(" + strings.Join(marks, ",") + ")")
	if err != nil {
		return err
	}
	defer stmt.Close()
	idStmt, err := conn.Prepare("INSERT INTO index_ids(id,fts_rowid) VALUES(?,?)")
	if err != nil {
		return err
	}
	defer idStmt.Close()
	insert := stmt.(driver.StmtExecContext)
	insertID := idStmt.(driver.StmtExecContext)
	count := int64(0)
	err = lease.Read(ctx, d.snapshotRows, ids, func(values []any) error {
		id, ok := values[0].(string)
		if !ok || !validToken(id) {
			return ErrIndexInvalid
		}
		count++
		args := make([]driver.NamedValue, len(values)+1)
		args[0] = driver.NamedValue{Ordinal: 1, Value: count}
		for i, value := range values {
			text := ""
			if value != nil {
				var ok bool
				text, ok = value.(string)
				if !ok || !utf8.ValidString(text) {
					return ErrIndexInvalid
				}
			}
			args[i+1] = driver.NamedValue{Ordinal: i + 2, Value: text}
		}
		if _, err := insertID.ExecContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: id}, {Ordinal: 2, Value: count}}); err != nil {
			var se sqlite3.Error
			if errors.As(err, &se) && se.Code == sqlite3.ErrConstraint {
				return ErrIndexInvalid
			}
			return err
		}
		_, err := insert.ExecContext(ctx, args)
		return err
	})
	if err != nil {
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	if err = idStmt.Close(); err != nil {
		return err
	}
	if ids != nil && d.index.Members == MembersAll && count != int64(len(ids)) {
		return ErrIndexInvalid
	}
	generation, err := fingerprintDisk(ctx, conn, d.index, scope, count)
	if err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO index_metadata(generation) VALUES(?)", []driver.NamedValue{{Ordinal: 1, Value: generation}}); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "COMMIT", nil); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "INSERT INTO "+quote(privateTable)+"("+quote(privateTable)+") VALUES('optimize')", nil); err != nil {
		return err
	}
	// Close before publishing so there are no live writers to the completed file.
	if err = conn.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

func fingerprintDisk(ctx context.Context, conn *sqlite3.SQLiteConn, idx Index, scope string, count int64) (string, error) {
	version, number, source := sqlite3.Version()
	var scopeMarker any
	if idx.Scope != nil {
		scopeMarker = scope
	}
	h := sha256.New()
	header, _ := json.Marshal([]any{privateRankingContractVersion, version, number, source, idx, scopeMarker, count})
	h.Write(header)
	cols := []string{"f.record_id"}
	for _, col := range idx.Columns {
		cols = append(cols, "f."+quote(col))
	}
	rows, err := conn.QueryContext(ctx, "SELECT "+strings.Join(cols, ",")+" FROM index_ids ids CROSS JOIN "+quote(privateTable)+" f ON f.rowid=ids.fts_rowid ORDER BY ids.id", nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	values := make([]driver.Value, len(cols))
	for {
		if err := rows.Next(values); err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return "", err
		}
		h.Write(encoded)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func openSnapshotIndex(path string) (*sqlite3.SQLiteConn, error) {
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("immutable", "1")
	u.RawQuery = q.Encode()
	raw, err := (&sqlite3.SQLiteDriver{}).Open(u.String())
	if err != nil {
		return nil, err
	}
	conn := raw.(*sqlite3.SQLiteConn)
	if _, err = conn.ExecContext(context.Background(), "PRAGMA query_only=1; PRAGMA cache_size=-2048; PRAGMA mmap_size=0; PRAGMA temp_store=FILE", nil); err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
	conn.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, MaxMaxIndexBytes)
	return conn, nil
}

func diskGeneration(ctx context.Context, conn *sqlite3.SQLiteConn) (string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT generation FROM index_metadata", nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err = rows.Next(values); err != nil {
		return "", err
	}
	generation, ok := values[0].(string)
	if !ok || len(generation) != 64 {
		return "", ErrIndexInvalid
	}
	if err = rows.Next(values); err != io.EOF {
		return "", ErrIndexInvalid
	}
	return generation, nil
}

func restrictPrivateIndex(conn *sqlite3.SQLiteConn) {
	conn.RegisterAuthorizer(func(op int, a, b, db string) int {
		switch op {
		case sqlite3.SQLITE_SELECT:
			return sqlite3.SQLITE_OK
		case sqlite3.SQLITE_READ:
			if db == "main" || db == "" {
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_FUNCTION:
			if b == "match" || b == "bm25" || b == "snippet" {
				return sqlite3.SQLITE_OK
			}
		case sqlite3.SQLITE_PRAGMA:
			if strings.EqualFold(a, "data_version") && b == "" && db == "main" {
				return sqlite3.SQLITE_OK
			}
		}
		return sqlite3.SQLITE_DENY
	})
}

func diskIndexError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrScopeNotFound) || errors.Is(err, ErrIndexInvalid) || errors.Is(err, ErrIndexLimit) {
		return err
	}
	if errors.Is(err, sqlread.ErrSnapshotPending) || errors.Is(err, sqlread.ErrSnapshotGeneration) || errors.Is(err, sqlread.ErrSnapshotResource) {
		return err
	}
	var se sqlite3.Error
	if errors.As(err, &se) && se.Code == sqlite3.ErrFull {
		return sqlread.ErrSnapshotResource
	}
	if errors.As(err, &se) && se.Code == sqlite3.ErrTooBig {
		return ErrIndexLimit
	}
	if errors.Is(err, sqlread.ErrSnapshotLimit) {
		return ErrIndexLimit
	}
	if errors.Is(err, sqlread.ErrBusy) || (errors.As(err, &se) && (se.Code == sqlite3.ErrBusy || se.Code == sqlite3.ErrLocked)) {
		return ErrBusy
	}
	return fmt.Errorf("%w: disk search unavailable", ErrIndexInvalid)
}
