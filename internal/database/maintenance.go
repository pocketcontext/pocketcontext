package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/pocketbase/dbx"
)

var (
	ErrReadOnly           = errors.New("persistent databases are read-only")
	ErrGenerationConflict = errors.New("maintenance generation conflict")
	ErrTransition         = errors.New("maintenance transition in progress")
)

const MarkerName = "maintenance.json"

type Status struct {
	State            string `json:"state"`
	Generation       uint64 `json:"generation"`
	ActiveOperations int    `json:"activeOperations"`
}

type marker struct {
	ReadOnly   bool   `json:"readOnly"`
	Generation uint64 `json:"generation"`
}

// Controller owns the persistent database pools for one application. It must not
// be shared with disposable filtered-query snapshot databases.
type Controller struct {
	mu         sync.Mutex
	transition sync.Mutex
	dir        string
	state      string
	generation uint64
	active     int
	blocked    bool
	changed    chan struct{}
}

func NewController(dataDir string) (*Controller, error) {
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	c := &Controller{dir: dir, state: "writable", changed: make(chan struct{})}
	path := filepath.Join(dir, MarkerName)
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
			return nil, errors.New("maintenance marker must be a private regular file")
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		var m struct {
			ReadOnly   *bool   `json:"readOnly"`
			Generation *uint64 `json:"generation"`
		}
		decoder := json.NewDecoder(io.LimitReader(f, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&m); err != nil {
			return nil, fmt.Errorf("invalid maintenance marker: %w", err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, errors.New("invalid maintenance marker trailing data")
		}
		if m.ReadOnly == nil || m.Generation == nil {
			return nil, errors.New("incomplete maintenance marker")
		}
		c.generation = *m.Generation
		if *m.ReadOnly {
			c.state = "read_only"
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return c, nil
}

func (c *Controller) Connect(path string) (*dbx.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if filepath.Dir(abs) != c.dir {
		return nil, errors.New("persistent database is outside maintenance directory")
	}
	u := url.URL{Scheme: "file", Path: abs}
	// A frozen application must never silently bootstrap missing databases.
	c.mu.Lock()
	frozen := c.state != "writable"
	c.mu.Unlock()
	if frozen {
		q := u.Query()
		q.Set("mode", "rw")
		u.RawQuery = q.Encode()
	}
	pool := sql.OpenDB(&guardedConnector{controller: c, dsn: u.String()})
	return dbx.NewFromDB(pool, "sqlite3"), nil
}

func (c *Controller) statusLocked() Status { return Status{c.state, c.generation, c.active} }
func (c *Controller) Status() Status       { c.mu.Lock(); defer c.mu.Unlock(); return c.statusLocked() }
func (c *Controller) notifyLocked()        { close(c.changed); c.changed = make(chan struct{}) }

func (c *Controller) persistLocked(readOnly bool, generation uint64) error {
	if err := os.MkdirAll(c.dir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(marker{readOnly, generation})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(c.dir, ".maintenance-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temp, filepath.Join(c.dir, MarkerName)); err != nil {
		return err
	}
	dir, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// BeginFreeze persists the intent before the HTTP admission gate drains. Already
// admitted HTTP operations can still use the database until CompleteFreeze.
func (c *Controller) BeginFreeze(expected uint64) (Status, error) {
	c.transition.Lock()
	defer c.transition.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != expected {
		return c.statusLocked(), ErrGenerationConflict
	}
	if c.state != "writable" {
		err := c.persistLocked(true, c.generation)
		if err != nil {
			c.blocked = true
			c.notifyLocked()
		}
		return c.statusLocked(), err
	}
	// Even an ambiguous fsync failure closes admission in memory; the operator
	// must resolve this transition rather than continuing to accept writes.
	c.state = "draining"
	c.generation++
	c.notifyLocked()
	err := c.persistLocked(true, c.generation)
	if err != nil {
		c.blocked = true
		c.notifyLocked()
	}
	return c.statusLocked(), err
}

func (c *Controller) waitLocked(ctx context.Context) error {
	for c.active != 0 {
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			c.mu.Lock()
			return ctx.Err()
		case <-changed:
		}
		c.mu.Lock()
	}
	return nil
}

// CompleteFreeze waits for transactions, statements and streaming rows. New
// operations run read-only during this final drain; existing transactions finish.
// A timeout keeps the database gate closed and is safe to retry.
func (c *Controller) CompleteFreeze(ctx context.Context) (Status, error) {
	c.transition.Lock()
	defer c.transition.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == "read_only" {
		return c.statusLocked(), nil
	}
	if c.state != "draining" {
		return c.statusLocked(), ErrTransition
	}
	c.blocked = true
	c.notifyLocked()
	if err := c.waitLocked(ctx); err != nil {
		return c.statusLocked(), err
	}
	c.state = "read_only"
	c.blocked = false
	c.notifyLocked()
	return c.statusLocked(), nil
}

func (c *Controller) Unfreeze(ctx context.Context, expected uint64) (Status, error) {
	c.transition.Lock()
	defer c.transition.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != expected {
		return c.statusLocked(), ErrGenerationConflict
	}
	if c.state == "writable" {
		return c.statusLocked(), ErrTransition
	}
	c.blocked = true
	c.notifyLocked()
	if err := c.waitLocked(ctx); err != nil {
		return c.statusLocked(), err
	}
	if err := c.persistLocked(false, c.generation+1); err != nil {
		return c.statusLocked(), err
	}
	c.generation++
	c.state = "writable"
	c.blocked = false
	c.notifyLocked()
	return c.statusLocked(), nil
}

func (c *Controller) acquire() (bool, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Read-only arrivals cannot prolong the writer drain, and authentication
	// reads must remain available after a timed-out transition. A read-only
	// transaction retains its connection enforcement until it finishes.
	frozen := c.blocked || c.state == "read_only"
	if frozen {
		return true, func() {}, nil
	}
	c.active++
	var once sync.Once
	return frozen, func() { once.Do(func() { c.mu.Lock(); c.active--; c.notifyLocked(); c.mu.Unlock() }) }, nil
}
