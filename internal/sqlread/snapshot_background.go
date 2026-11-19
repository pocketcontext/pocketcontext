package sqlread

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// BackgroundSnapshotConfig opts an application into eventual snapshot freshness,
// including eventual revocation. Ordinary authentication remains live.
type BackgroundSnapshotConfig struct {
	MaxDiskBytes      int64 `json:"maxDiskBytes"`
	MinFreeBytes      int64 `json:"minFreeBytes"`
	BuildTimeoutMS    int   `json:"buildTimeoutMs"`
	MaxBuilders       int   `json:"maxBuilders"`
	MaxCachedUsers    int   `json:"maxCachedUsers"`
	IdleTimeoutMS     int   `json:"idleTimeoutMs,omitempty"`
	RefreshIntervalMS int   `json:"refreshIntervalMs"`
	RetryDelayMS      int   `json:"retryDelayMs"`
	MaxValueBytes     int   `json:"maxValueBytes"`
}

func normalizeBackground(c *BackgroundSnapshotConfig) error {
	if c.MaxDiskBytes == 0 {
		c.MaxDiskBytes = 1 << 30
	}
	if c.MinFreeBytes == 0 {
		c.MinFreeBytes = 256 << 20
	}
	if c.MaxDiskBytes < 65536 || c.MaxDiskBytes > 1<<40 || c.MinFreeBytes < 0 || c.MinFreeBytes > 1<<40 {
		return errors.New("background snapshot disk budgets outside permitted range")
	}
	if c.IdleTimeoutMS != 0 && (c.IdleTimeoutMS < 60000 || c.IdleTimeoutMS > 86400000) {
		return errors.New("background snapshot idleTimeoutMs must be zero or between 60000 and 86400000")
	}
	for _, v := range []struct {
		p             *int
		def, min, max int
	}{
		{&c.BuildTimeoutMS, 120000, 1, 900000}, {&c.MaxBuilders, 1, 1, 4},
		{&c.MaxCachedUsers, 16, 1, 1024}, {&c.RefreshIntervalMS, 1000, 10, 60000},
		{&c.RetryDelayMS, 1000, 10, 60000}, {&c.MaxValueBytes, 16 << 20, 1024, 64 << 20},
	} {
		if *v.p == 0 {
			*v.p = v.def
		}
		if *v.p < v.min || *v.p > v.max {
			return errors.New("background snapshot setting outside permitted range")
		}
	}
	return nil
}

var (
	ErrSnapshotPending    = errors.New("snapshot is preparing")
	ErrSnapshotGeneration = errors.New("snapshot generation changed or is unavailable")
	ErrSnapshotResource   = errors.New("snapshot storage budget unavailable")
)

type SnapshotReadOptions struct {
	Consistency        string `json:"consistency,omitempty"`
	AtLeast            string `json:"atLeast,omitempty"`
	ExpectedGeneration string `json:"expectedGeneration,omitempty"`
}

type SnapshotMetadata struct {
	Generation string    `json:"generation,omitempty"`
	CapturedAt time.Time `json:"capturedAt"`
	Refreshing bool      `json:"refreshing"`
	Stale      bool      `json:"stale"`
	Rows       int64     `json:"rows"`
	DiskBytes  int64     `json:"diskBytes"`
	LastError  string    `json:"lastError,omitempty"`
}

type backgroundUser struct {
	current                 *snapshotEntry
	building, queued, dirty bool
	lastError               string
	retryAt                 time.Time
	failures                int
	used                    time.Time
	waiters                 int
	changed                 chan struct{}
}

type backgroundSnapshots struct {
	metrics           BackgroundSnapshotMetrics // protected by mu
	source            *SnapshotSource
	config            BackgroundSnapshotConfig
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	stopOnce          sync.Once
	mu                sync.Mutex
	users             map[string]*backgroundUser
	retired           map[*snapshotEntry]bool
	jobs              chan string
	observeMu         sync.Mutex
	state             snapshotState
	observed          bool
	sourceCheckFailed bool
	epoch, sequence   uint64
	instance          string
	storageMu         sync.Mutex
	reservations      map[string]int64
	storageChanged    chan struct{}
}

// BackgroundSnapshotMetrics summarizes this process's background work. GenerationBytes
// counts completed SQL and warm index files at publication, in logical bytes.
type BackgroundSnapshotMetrics struct {
	ObservedChanges  uint64 `json:"observedChanges"`
	UnrelatedChanges uint64 `json:"unrelatedChanges"`
	CompletedBuilds  uint64 `json:"completedBuilds"`
	FailedBuilds     uint64 `json:"failedBuilds"`
	ColdBuilds       uint64 `json:"coldBuilds"`
	IdleRetirements  uint64 `json:"idleRetirements"`
	GenerationBytes  uint64 `json:"generationBytes"`
	CachedUsers      int    `json:"cachedUsers"`
}

func (s *SnapshotSource) BackgroundMetrics() BackgroundSnapshotMetrics {
	b := s.background
	b.mu.Lock()
	defer b.mu.Unlock()
	metrics := b.metrics
	metrics.CachedUsers = len(b.users)
	return metrics
}

type snapshotDiskBudgetKey struct{}
type snapshotDirectoryKey struct{}

func newBackgroundSnapshots(s *SnapshotSource) *backgroundSnapshots {
	ctx, cancel := context.WithCancel(context.Background())
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic("cannot initialize snapshot identity")
	}
	b := &backgroundSnapshots{source: s, config: *s.snapshot.Background, ctx: ctx, cancel: cancel,
		users: map[string]*backgroundUser{}, retired: map[*snapshotEntry]bool{},
		jobs: make(chan string, s.snapshot.Background.MaxCachedUsers), instance: hex.EncodeToString(random[:]), reservations: map[string]int64{}, storageChanged: make(chan struct{})}
	b.wg.Add(1 + b.config.MaxBuilders)
	go b.watch()
	for i := 0; i < b.config.MaxBuilders; i++ {
		go b.worker()
	}
	return b
}

func (s *SnapshotSource) BackgroundEnabled() bool { return s.background != nil }

// SetSnapshotInitializer registers trusted derived-index construction before any
// requests are served. A failed initializer never publishes a partial generation.
func (s *SnapshotSource) SetSnapshotInitializer(fn func(context.Context, *SnapshotLease) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initializer = fn
}

func (b *backgroundSnapshots) stop() {
	b.stopOnce.Do(func() {
		b.cancel()
		b.wg.Wait()
	})
}

// observe serializes use of the connection-local data_version. Epochs are local
// monotonic barriers, never durable source commit tokens.
func (b *backgroundSnapshots) observe(ctx context.Context) (uint64, error) {
	b.observeMu.Lock()
	defer b.observeMu.Unlock()
	b.source.cacheMu.Lock()
	state, err := b.source.state(ctx)
	b.source.cacheMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.source.snapshot.TrackChanges {
		b.sourceCheckFailed = err != nil
	}
	if err != nil {
		return 0, err
	}
	changed := !b.observed
	if b.observed && state.version != b.state.version {
		b.metrics.ObservedChanges++
		changed = !b.source.snapshot.TrackChanges || len(b.source.changedExports(b.state, state)) > 0
		if !changed {
			b.metrics.UnrelatedChanges++
		}
	}
	b.observed, b.state = true, state
	if changed {
		b.epoch++
		for id, u := range b.users {
			u.dirty = true
			b.refreshCachedUserLocked(id, u)
		}
	}
	return b.epoch, nil
}

func (b *backgroundSnapshots) refreshCachedUserLocked(id string, u *backgroundUser) {
	if !b.retireIdleLocked(id, u) {
		b.scheduleLocked(id, u)
	}
}

func (b *backgroundSnapshots) scheduleLocked(id string, u *backgroundUser) {
	if !u.dirty || u.building || u.queued || time.Now().Before(u.retryAt) || b.ctx.Err() != nil {
		return
	}
	select {
	case b.jobs <- id:
		u.queued = true
	default:
	}
}

func (b *backgroundSnapshots) retireIdleLocked(id string, u *backgroundUser) bool {
	if b.config.IdleTimeoutMS == 0 || u.building || u.queued || u.waiters > 0 ||
		time.Since(u.used) < time.Duration(b.config.IdleTimeoutMS)*time.Millisecond {
		return false
	}
	b.evictUserLocked(id, u)
	b.metrics.IdleRetirements++
	return true
}

func (b *backgroundSnapshots) evictUserLocked(id string, u *backgroundUser) {
	delete(b.users, id)
	if u.current != nil {
		b.retireLocked(u.current)
	}
	close(u.changed)
}

func (b *backgroundSnapshots) watch() {
	defer b.wg.Done()
	ticker := time.NewTicker(time.Duration(b.config.RefreshIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(b.ctx, time.Duration(b.source.snapshot.TimeoutMS)*time.Millisecond)
		_, err := b.observe(ctx)
		cancel()
		b.mu.Lock()
		for id, u := range b.users {
			if err != nil {
				u.dirty = true
				u.lastError = "source_check_failed"
			}
			// Time-dependent policies need refresh even without a source commit.
			if !b.source.cacheable {
				u.dirty = true
			}
			b.refreshCachedUserLocked(id, u)
		}
		b.mu.Unlock()
	}
}

func (b *backgroundSnapshots) userLocked(id string) (*backgroundUser, error) {
	if u := b.users[id]; u != nil && !b.retireIdleLocked(id, u) {
		u.used = time.Now()
		return u, nil
	}
	if len(b.users) >= b.config.MaxCachedUsers {
		var oldest string
		var at time.Time
		for key, u := range b.users {
			if u.building || u.queued || u.waiters > 0 {
				continue
			}
			if at.IsZero() || u.used.Before(at) {
				oldest, at = key, u.used
			}
		}
		if oldest == "" {
			return nil, ErrSnapshotPending
		}
		b.evictUserLocked(oldest, b.users[oldest])
	}
	u := &backgroundUser{dirty: true, used: time.Now(), changed: make(chan struct{})}
	b.users[id] = u
	return u, nil
}

func (b *backgroundSnapshots) token(requester string, sequence uint64) string {
	digest := sha256.Sum256([]byte(b.instance + "/" + requester))
	return b.instance + "." + hex.EncodeToString(digest[:8]) + "." + strconv.FormatUint(sequence, 10)
}

func (b *backgroundSnapshots) tokenSequence(requester, token string) (uint64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0, ErrSnapshotGeneration
	}
	n, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil || n == 0 || b.token(requester, n) != token {
		return 0, ErrSnapshotGeneration
	}
	b.mu.Lock()
	issued := b.sequence
	b.mu.Unlock()
	if n > issued {
		return 0, ErrSnapshotGeneration
	}
	return n, nil
}

func (b *backgroundSnapshots) metadataLocked(u *backgroundUser) SnapshotMetadata {
	m := SnapshotMetadata{Refreshing: u.building || u.queued, Stale: u.dirty, LastError: u.lastError}
	if b.sourceCheckFailed {
		m.Stale, m.LastError = true, "source_check_failed"
	}
	if e := u.current; e != nil {
		m.Generation, m.CapturedAt, m.DiskBytes = e.generation, e.at, e.diskBytes
		for _, size := range e.sizes {
			m.Rows += int64(size.rows)
		}
	}
	return m
}

func (s *SnapshotSource) SnapshotStatus(ctx context.Context, requester string) (SnapshotMetadata, error) {
	if s.background == nil || requester == "" {
		return SnapshotMetadata{}, ErrSnapshotBuild
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return SnapshotMetadata{}, ErrSnapshotBuild
	}
	b := s.background
	_, observeErr := b.observe(ctx)
	b.mu.Lock()
	defer b.mu.Unlock()
	u, err := b.userLocked(requester)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if observeErr != nil {
		u.dirty = true
		u.lastError = "source_check_failed"
	}
	b.scheduleLocked(requester, u)
	return b.metadataLocked(u), nil
}

// SnapshotLease pins one immutable requester generation. Call Close after all
// associated SQL/search reads finish. It does not authorize another requester.
type SnapshotLease struct {
	source       *SnapshotSource
	entry        *snapshotEntry
	metadata     SnapshotMetadata
	owned        bool
	sourceLocked bool
	initializing bool
	budget       int64
	closeOnce    sync.Once
}

func (l *SnapshotLease) Metadata() SnapshotMetadata {
	m := l.metadata
	b := l.source.background
	b.mu.Lock()
	m.DiskBytes = l.entry.diskBytes
	b.mu.Unlock()
	return m
}
func (l *SnapshotLease) Directory() string { return l.entry.dir }
func (l *SnapshotLease) Requester() string { return l.entry.requester }
func (l *SnapshotLease) DiskBudget() int64 { return l.budget }
func (l *SnapshotLease) BuildTimeout() time.Duration {
	return time.Duration(l.source.background.config.BuildTimeoutMS) * time.Millisecond
}
func (l *SnapshotLease) Close() {
	l.closeOnce.Do(func() {
		if !l.owned {
			return
		}
		defer l.source.releaseRequester(l.entry.requester)
		if l.sourceLocked {
			defer l.source.mu.RUnlock()
		}
		b := l.source.background
		b.mu.Lock()
		defer b.mu.Unlock()
		l.entry.refs--
		if l.entry.refs == 0 && !l.entry.retained {
			b.removeLocked(l.entry)
		}
	})
}

func (s *SnapshotSource) AcquireSnapshot(ctx context.Context, requester string, opts SnapshotReadOptions) (*SnapshotLease, error) {
	if s.background == nil || requester == "" {
		return nil, ErrSnapshotBuild
	}
	s.mu.RLock()
	locked := true
	admitted := false
	defer func() {
		if locked {
			if admitted {
				s.releaseRequester(requester)
			}
			s.mu.RUnlock()
		}
	}()
	if s.closed {
		return nil, ErrSnapshotBuild
	}
	if opts.Consistency != "" && opts.Consistency != "available" && opts.Consistency != "fresh" {
		return nil, errors.New("invalid snapshot consistency")
	}
	if err := s.admitRequester(ctx, requester); err != nil {
		return nil, err
	}
	admitted = true
	b := s.background
	var minimum uint64
	var err error
	if opts.AtLeast != "" {
		minimum, err = b.tokenSequence(requester, opts.AtLeast)
		if err != nil {
			return nil, err
		}
	}
	if opts.ExpectedGeneration != "" {
		if _, err = b.tokenSequence(requester, opts.ExpectedGeneration); err != nil {
			return nil, err
		}
	}
	var target uint64
	if opts.Consistency == "fresh" || s.snapshot.TrackChanges {
		// Every new tracked lease validates source health, even when the
		// caller accepts a completed generation during an ordinary refresh.
		epoch, err := b.observe(ctx)
		if err != nil {
			return nil, ErrSnapshotBuild
		}
		if opts.Consistency == "fresh" {
			target = epoch
		}
		if opts.Consistency == "fresh" && !s.cacheable {
			b.mu.Lock()
			b.epoch++
			target = b.epoch
			for id, u := range b.users {
				u.dirty = true
				b.refreshCachedUserLocked(id, u)
			}
			b.mu.Unlock()
		}
	}
	waitMS := s.snapshot.TimeoutMS
	if opts.Consistency == "fresh" {
		waitMS = b.config.BuildTimeoutMS
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(waitMS)*time.Millisecond)
	defer cancel()
	for {
		b.mu.Lock()
		if b.sourceCheckFailed {
			b.mu.Unlock()
			return nil, ErrSnapshotBuild
		}
		u, err := b.userLocked(requester)
		if err != nil {
			b.mu.Unlock()
			return nil, err
		}
		if u.current != nil {
			e := u.current
			if opts.ExpectedGeneration != "" && e.generation != opts.ExpectedGeneration {
				b.mu.Unlock()
				return nil, ErrSnapshotGeneration
			}
			if e.epoch >= target && e.sequence >= minimum {
				b.scheduleLocked(requester, u)
				e.refs++
				lease := &SnapshotLease{source: s, entry: e, metadata: b.metadataLocked(u), owned: true, sourceLocked: true, budget: e.diskBudget}
				locked = false
				b.mu.Unlock()
				return lease, nil
			}
		}
		if u.lastError == "storage_budget" && !u.building && !u.queued {
			b.mu.Unlock()
			return nil, ErrSnapshotResource
		}
		u.dirty = true
		u.waiters++
		b.scheduleLocked(requester, u)
		changed := u.changed
		b.mu.Unlock()
		var waitErr error
		select {
		case <-changed:
		case <-waitCtx.Done():
			waitErr = ctx.Err()
			if waitErr == nil {
				waitErr = ErrSnapshotPending
			}
		case <-b.ctx.Done():
			waitErr = ErrSnapshotBuild
		}
		b.mu.Lock()
		u.waiters--
		u.used = time.Now()
		b.mu.Unlock()
		if waitErr != nil {
			return nil, waitErr
		}
	}
}

func (s *SnapshotSource) QueryWithOptions(ctx context.Context, requester, query string, opts SnapshotReadOptions) (Result, SnapshotMetadata, error) {
	if s.background == nil {
		if opts.AtLeast != "" || opts.ExpectedGeneration != "" {
			return Result{}, SnapshotMetadata{}, ErrSnapshotGeneration
		}
		if opts.Consistency != "" && opts.Consistency != "available" && opts.Consistency != "fresh" {
			return Result{}, SnapshotMetadata{}, errors.New("invalid snapshot consistency")
		}
		result, at, err := s.Query(ctx, requester, query)
		return result, SnapshotMetadata{CapturedAt: at}, err
	}
	if len(query) > 65536 {
		return Result{}, SnapshotMetadata{}, errors.New("SQL exceeds 65536 bytes")
	}
	if err := singleStatement(query); err != nil {
		return Result{}, SnapshotMetadata{}, err
	}
	lease, err := s.AcquireSnapshot(ctx, requester, opts)
	if err != nil {
		return Result{}, SnapshotMetadata{}, err
	}
	defer lease.Close()
	// A background build never occupies a query slot. Bound only concurrent
	// query execution here, separately from the background worker pool.
	waitCtx, cancel := context.WithTimeout(ctx, lease.entry.engine.cfg.Timeout)
	defer cancel()
	select {
	case s.slots <- struct{}{}:
	case <-waitCtx.Done():
		return Result{}, lease.Metadata(), waitCtx.Err()
	}
	defer func() { <-s.slots }()
	result, err := lease.entry.engine.Query(waitCtx, query)
	return result, lease.Metadata(), err
}

func (b *backgroundSnapshots) worker() {
	defer b.wg.Done()
	for {
		var id string
		select {
		case <-b.ctx.Done():
			return
		case id = <-b.jobs:
		}
		ctx, cancel := context.WithTimeout(b.ctx, time.Duration(b.config.BuildTimeoutMS)*time.Millisecond)
		epoch, err := b.observe(ctx)
		initialCheckFailed := err != nil
		b.mu.Lock()
		u := b.users[id]
		if u == nil {
			b.mu.Unlock()
			cancel()
			continue
		}
		u.queued = false
		if b.retireIdleLocked(id, u) || u.building || !u.dirty {
			b.mu.Unlock()
			cancel()
			continue
		}
		u.building = true
		b.sequence++
		sequence := b.sequence
		b.mu.Unlock()
		var entry *snapshotEntry
		if err == nil {
			entry, err = b.construct(ctx, id, sequence, epoch)
		}
		cancel()
		// A later data commit does not invalidate this complete generation.
		checkCtx, checkCancel := context.WithTimeout(b.ctx, time.Duration(b.source.snapshot.TimeoutMS)*time.Millisecond)
		_, checkErr := b.observe(checkCtx)
		checkCancel()
		b.mu.Lock()
		u = b.users[id]
		if u == nil {
			if entry != nil {
				b.removeLocked(entry)
			}
			b.mu.Unlock()
			continue
		}
		u.building = false
		failedCheck := b.source.snapshot.TrackChanges && (initialCheckFailed || checkErr != nil || b.sourceCheckFailed)
		if err != nil || failedCheck || b.ctx.Err() != nil {
			b.metrics.FailedBuilds++
			if entry != nil {
				b.removeLocked(entry)
			}
			u.dirty = true
			u.failures++
			u.lastError = backgroundError(err)
			if failedCheck {
				u.lastError = "source_check_failed"
			}
			delay := time.Duration(b.config.RetryDelayMS) * time.Millisecond
			for n := 1; n < u.failures && n < 7; n++ {
				delay *= 2
			}
			if delay > time.Minute {
				delay = time.Minute
			}
			u.retryAt = time.Now().Add(delay)
		} else {
			if u.current != nil {
				b.retireLocked(u.current)
			} else {
				b.metrics.ColdBuilds++
			}
			b.metrics.CompletedBuilds++
			b.metrics.GenerationBytes += uint64(entry.diskBytes)
			entry.retained = true
			u.current = entry
			u.dirty = checkErr != nil || epoch < b.epoch
			u.lastError = ""
			u.failures = 0
			u.retryAt = time.Time{}
			if checkErr != nil {
				u.lastError = "source_check_failed"
			}
		}
		close(u.changed)
		u.changed = make(chan struct{})
		b.refreshCachedUserLocked(id, u)
		b.mu.Unlock()
	}
}

func backgroundError(err error) string {
	if errors.Is(err, ErrSnapshotResource) {
		return "storage_budget"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "build_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "build_cancelled"
	}
	return "build_failed"
}

func (b *backgroundSnapshots) construct(ctx context.Context, id string, sequence, epoch uint64) (*snapshotEntry, error) {
	b.source.mu.RLock()
	defer b.source.mu.RUnlock()
	if b.source.closed {
		return nil, ErrSnapshotBuild
	}
	dir, err := os.MkdirTemp(b.source.root, "generation-")
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			os.RemoveAll(dir)
		}
	}()
	budget, release, err := b.reserveDisk(ctx, dir)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = context.WithValue(ctx, snapshotDiskBudgetKey{}, budget)
	ctx = context.WithValue(ctx, snapshotDirectoryKey{}, dir)
	entry, err := b.source.buildSnapshot(ctx, id, nil, nil)
	if err != nil {
		var se sqlite3.Error
		if errors.As(err, &se) && (se.Code == sqlite3.ErrFull || se.Code == sqlite3.ErrTooBig) {
			return nil, ErrSnapshotResource
		}
		return nil, err
	}
	entry.requester, entry.sequence, entry.epoch = id, sequence, epoch
	entry.generation = b.token(id, sequence)
	entry.diskBudget = budget
	lease := &SnapshotLease{source: b.source, entry: entry, initializing: true, budget: budget}
	lease.metadata = SnapshotMetadata{Generation: entry.generation, CapturedAt: entry.at}
	if b.source.initializer != nil {
		err = b.source.initializer(ctx, lease)
	}
	if err == nil {
		err = lease.RefreshDiskUsage()
	}
	if err != nil {
		entry.engine.Close()
		return nil, err
	}
	success = true
	return entry, nil
}

func (b *backgroundSnapshots) retireLocked(e *snapshotEntry) {
	e.retained = false
	if e.refs == 0 {
		b.removeLocked(e)
	} else {
		b.retired[e] = true
	}
}

func (b *backgroundSnapshots) removeLocked(e *snapshotEntry) {
	delete(b.retired, e)
	e.engine.Close()
	os.RemoveAll(e.dir)
}

func directoryBytes(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("unexpected snapshot symlink")
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// reserveDisk accounts for complete, retired and in-progress generations.
// Reserving the remaining space prevents concurrent builders from each spending
// the same disk budget. Contention waits for release rather than reporting a
// false capacity failure while another generation is growing.
func (b *backgroundSnapshots) reserveDisk(ctx context.Context, dir string) (int64, func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		b.storageMu.Lock()
		if len(b.reservations) == 0 {
			break
		}
		changed := b.storageChanged
		b.storageMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	defer b.storageMu.Unlock()
	entries, err := os.ReadDir(b.source.root)
	if err != nil {
		return 0, nil, err
	}
	var used int64
	for _, entry := range entries {
		path := filepath.Join(b.source.root, entry.Name())
		if path == dir {
			continue
		}
		if reserved, ok := b.reservations[path]; ok {
			used += reserved
			continue
		}
		n, err := directoryBytes(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, nil, err
		}
		used += n
	}
	current, err := directoryBytes(dir)
	if err != nil {
		return 0, nil, err
	}
	free, err := snapshotFreeBytes(b.source.root)
	if err != nil {
		return 0, nil, err
	}
	budget := b.config.MaxDiskBytes - used
	available := free - b.config.MinFreeBytes
	if available < budget-current {
		budget = current + available
	}
	if budget-current < 32768 {
		return 0, nil, ErrSnapshotResource
	}
	b.reservations[dir] = budget
	var once sync.Once
	release := func() {
		once.Do(func() {
			b.storageMu.Lock()
			delete(b.reservations, dir)
			close(b.storageChanged)
			b.storageChanged = make(chan struct{})
			b.storageMu.Unlock()
		})
	}
	return budget, release, nil
}

func (l *SnapshotLease) WithDiskReservation(ctx context.Context, fn func(int64) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.initializing {
		return fn(l.budget)
	}
	budget, release, err := l.source.background.reserveDisk(ctx, l.entry.dir)
	if err != nil {
		return err
	}
	defer release()
	err = fn(budget)
	if updateErr := l.RefreshDiskUsage(); err == nil {
		err = updateErr
	}
	return err
}

func (l *SnapshotLease) RefreshDiskUsage() error {
	size, err := directoryBytes(l.entry.dir)
	if err != nil {
		return err
	}
	b := l.source.background
	b.mu.Lock()
	l.entry.diskBytes = size
	b.mu.Unlock()
	if l.initializing && size > l.budget {
		return ErrSnapshotResource
	}
	return nil
}

// Close background engines only after foreground queries have left source.mu.
func (s *SnapshotSource) closeBackgroundEntries() {
	if s.background == nil {
		return
	}
	b := s.background
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, u := range b.users {
		if u.current != nil {
			u.current.engine.Close()
		}
	}
	for e := range b.retired {
		e.engine.Close()
	}
}
