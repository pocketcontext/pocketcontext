package sqlread

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

// Time functions can change policy results without a database commit. Even
// constant-argument uses conservatively bypass reuse; SQL parsing, including
// quoted function names, is handled by SQLite's prepare-time authorizer.
func volatileSnapshotFunction(name string) bool {
	switch strings.ToLower(name) {
	case "date", "datetime", "julianday", "strftime", "time", "timediff", "unixepoch", "current_date", "current_time", "current_timestamp":
		return true
	}
	return false
}

type snapshotEntry struct {
	engine   *Engine
	dir      string
	at       time.Time
	version  int64
	refs     int
	retained bool
	used     time.Time
}

// version is always read on the same dedicated physical connection. Values
// from different SQLite connections are not comparable. No transaction is
// left open on the monitor and it never executes caller SQL or writes.
// cacheMu serializes its use and cache publication.
func (s *SnapshotSource) version(ctx context.Context) (int64, error) {
	var trusted *bool
	if err := s.monitor.Raw(func(raw any) error { trusted = raw.(roConn).trusted; *trusted = true; return nil }); err != nil {
		return 0, err
	}
	defer func() { *trusted = false }()
	var version int64
	err := s.monitor.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version)
	return version, err
}

func (s *SnapshotSource) discardSnapshot(entry *snapshotEntry) {
	entry.retained = false
	if entry.refs == 0 {
		entry.engine.Close()
		os.RemoveAll(entry.dir)
	}
}

func (s *SnapshotSource) releaseSnapshot(entry *snapshotEntry) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	entry.refs--
	if !entry.retained && entry.refs == 0 {
		entry.engine.Close()
		os.RemoveAll(entry.dir)
	}
}

func (s *SnapshotSource) acquireSnapshot(ctx context.Context, requester string) (*snapshotEntry, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.cacheMu.Lock()
		version, err := s.version(ctx)
		if err != nil {
			s.cacheMu.Unlock()
			return nil, err
		}
		// A commit invalidates all requesters, including changes to unexported
		// policy/auth tables. Existing readers keep their own immutable files.
		for user, entry := range s.cache {
			if entry.version != version {
				delete(s.cache, user)
				s.discardSnapshot(entry)
			}
		}
		if entry := s.cache[requester]; entry != nil {
			entry.refs++
			entry.used = time.Now()
			s.cacheMu.Unlock()
			return entry, nil
		}
		if pending := s.building[requester]; pending != nil {
			s.cacheMu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		pending := make(chan struct{})
		s.building[requester] = pending
		s.cacheMu.Unlock()

		entry, buildErr := s.buildSnapshot(ctx, requester)
		s.cacheMu.Lock()
		delete(s.building, requester)
		close(pending)
		if buildErr != nil {
			s.cacheMu.Unlock()
			return nil, buildErr
		}
		after, err := s.version(ctx)
		if err != nil {
			s.discardSnapshot(entry)
			s.cacheMu.Unlock()
			return nil, err
		}

		entry.refs = 1
		// A concurrent commit does not invalidate this request's consistent
		// source transaction, but its snapshot must never be reused.
		if s.cacheable && after == version {
			for len(s.cache) >= s.snapshot.MaxConcurrent {
				var oldest string
				var at time.Time
				for user, cached := range s.cache {
					if at.IsZero() || cached.used.Before(at) {
						oldest, at = user, cached.used
					}
				}
				old := s.cache[oldest]
				delete(s.cache, oldest)
				s.discardSnapshot(old)
			}
			entry.version, entry.retained, entry.used = after, true, time.Now()
			s.cache[requester] = entry
		}
		s.cacheMu.Unlock()
		return entry, nil
	}
}

func (s *SnapshotSource) buildSnapshot(ctx context.Context, requester string) (*snapshotEntry, error) {
	dir, err := os.MkdirTemp(s.root, "request-")
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			os.RemoveAll(dir)
		}
	}()
	path := filepath.Join(dir, "snapshot.db")
	done := tracing.Start(ctx, "snapshot.build")
	at, err := s.build(ctx, path, requester)
	done()
	if err != nil {
		return nil, err
	}
	done = tracing.Start(ctx, "snapshot.reader_init")
	engine, err := New(path, s.cfg)
	done()
	if err != nil {
		return nil, err
	}
	success = true
	return &snapshotEntry{engine: engine, dir: dir, at: at}, nil
}
