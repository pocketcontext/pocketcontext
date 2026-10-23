package sqlread

import (
	"context"
	"io"
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
	engine     *Engine
	dir        string
	at         time.Time
	state      snapshotState
	sizes      map[string]tableSize
	refs       int
	retained   bool
	used       time.Time
	requester  string
	generation string
	sequence   uint64
	epoch      uint64
	diskBytes  int64
	diskBudget int64
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
	s.releaseSnapshotLocked(entry)
}

func (s *SnapshotSource) releaseSnapshotLocked(entry *snapshotEntry) {
	entry.refs--
	if !entry.retained && entry.refs == 0 {
		entry.engine.Close()
		os.RemoveAll(entry.dir)
	}
}

func (s *SnapshotSource) acquireSnapshot(ctx context.Context, requester string) (*snapshotEntry, error) {
	fullRefresh := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.cacheMu.Lock()
		state, err := s.state(ctx)
		if err != nil {
			s.cacheMu.Unlock()
			return nil, err
		}
		// Default mode invalidates all requesters on every commit. Tracking mode
		// keeps old files only as refresh bases; their state must match to serve.
		for user, entry := range s.cache {
			if !s.snapshot.TrackChanges && !entry.state.equal(state) {
				delete(s.cache, user)
				s.discardSnapshot(entry)
			}
		}
		if entry := s.cache[requester]; entry != nil && entry.state.equal(state) {
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
		var base *snapshotEntry
		var changed map[string]bool
		if s.snapshot.TrackChanges && s.cacheable && !fullRefresh {
			base = s.cache[requester]
			if base != nil {
				base.refs++
				changed = s.changedExports(base.state, state)
			}
		}
		s.cacheMu.Unlock()

		entry, buildErr := s.buildSnapshot(ctx, requester, base, changed)
		s.cacheMu.Lock()
		if base != nil {
			s.releaseSnapshotLocked(base)
		}
		delete(s.building, requester)
		close(pending)
		if buildErr != nil {
			s.cacheMu.Unlock()
			return nil, buildErr
		}
		after, err := s.state(ctx)
		if err != nil {
			s.discardSnapshot(entry)
			s.cacheMu.Unlock()
			return nil, err
		}

		// A partial refresh combines unchanged old tables with one source read
		// transaction. Any intervening relevant commit makes that combination
		// unproven: discard it and retry, never return mixed authorization/data.
		if base != nil && !state.equal(after) {
			s.discardSnapshot(entry)
			s.cacheMu.Unlock()
			// A busy writer must not cause an endless sequence of incremental
			// retries. A full source transaction can safely finish even if a
			// later commit prevents retaining its result.
			fullRefresh = true
			continue
		}
		entry.refs = 1
		// A concurrent commit does not invalidate this request's consistent
		// source transaction, but its snapshot must never be reused.
		if s.cacheable && after.equal(state) {
			if old := s.cache[requester]; old != nil {
				delete(s.cache, requester)
				s.discardSnapshot(old)
			}
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
			entry.state, entry.retained, entry.used = after, true, time.Now()
			s.cache[requester] = entry
		}
		s.cacheMu.Unlock()
		return entry, nil
	}
}

func (s *SnapshotSource) buildSnapshot(ctx context.Context, requester string, base *snapshotEntry, changed map[string]bool) (*snapshotEntry, error) {
	dir, ok := ctx.Value(snapshotDirectoryKey{}).(string)
	var err error
	if !ok {
		dir, err = os.MkdirTemp(s.root, "request-")
		if err != nil {
			return nil, err
		}
	}
	success := false
	defer func() {
		if !success {
			os.RemoveAll(dir)
		}
	}()
	path := filepath.Join(dir, "snapshot.db")
	done := tracing.Start(ctx, "snapshot.build")
	if base != nil {
		copyDone := tracing.Start(ctx, "snapshot.copy")
		err := cloneSnapshot(ctx, filepath.Join(base.dir, "snapshot.db"), path)
		copyDone()
		if err != nil {
			done()
			return nil, err
		}
	}
	at, sizes, err := s.build(ctx, path, requester, base, changed)
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
	return &snapshotEntry{engine: engine, dir: dir, at: at, sizes: sizes}, nil
}

// Cached files are immutable and have no WAL. Only private generated snapshots
// are copied; the persistent application database is never copied this way.
func cloneSnapshot(ctx context.Context, source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := in.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return out.Close()
		}
		if readErr != nil {
			return readErr
		}
	}
}
