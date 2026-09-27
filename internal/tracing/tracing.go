// Package tracing records optional request timings without application dependencies.
package tracing

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var servicePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,100}$`)

type Config struct {
	Enabled    bool   `json:"enabled"`
	Path       string `json:"path"`
	Service    string `json:"service"`
	CaptureSQL bool   `json:"captureSql"`
	MaxBytes   int64  `json:"maxBytes"`
}

func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Path == "" || !servicePattern.MatchString(c.Service) {
		return errors.New("tracing requires path and service (at most 100 bytes)")
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 16 << 20
	}
	if c.MaxBytes < 1<<20 || c.MaxBytes > 1<<30 {
		return errors.New("tracing maxBytes must be 1048576..1073741824")
	}
	return nil
}

type Span struct {
	Name       string  `json:"name"`
	OffsetMS   float64 `json:"offset_ms"`
	DurationMS float64 `json:"duration_ms"`
}
type Trace struct {
	Version       int       `json:"version"`
	RequestID     string    `json:"request_id"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Service       string    `json:"service"`
	Method        string    `json:"method"`
	Route         string    `json:"route"`
	StartedAt     time.Time `json:"started_at"`
	DurationMS    float64   `json:"duration_ms"`
	Status        int       `json:"status"`
	UserID        string    `json:"user_id"`
	SQL           string    `json:"sql,omitempty"`
	Rows          int       `json:"rows"`
	Truncated     bool      `json:"truncated"`
	Spans         []Span    `json:"spans"`
}
type key struct{}

func With(ctx context.Context, t *Trace) context.Context { return context.WithValue(ctx, key{}, t) }
func From(ctx context.Context) *Trace                    { t, _ := ctx.Value(key{}).(*Trace); return t }
func Start(ctx context.Context, name string) func() {
	t := From(ctx)
	if t == nil {
		return func() {}
	}
	at := time.Now()
	return func() {
		if len(t.Spans) >= 128 {
			return
		}
		t.Spans = append(t.Spans, Span{name, float64(at.Sub(t.StartedAt)) / float64(time.Millisecond), float64(time.Since(at)) / float64(time.Millisecond)})
	}
}

// Sink keeps at most 256 pending records and two bounded files. Overflow and
// I/O failures drop telemetry, never block application requests.
type Sink struct {
	cfg     Config
	file    *os.File
	size    int64
	queue   chan []byte
	done    chan struct{}
	mu      sync.RWMutex
	closed  bool
	Dropped atomic.Uint64
}

func open(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("trace file must be a private regular file")
	}
	return f, nil
}
func NewSink(c Config) (*Sink, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	f, err := open(c.Path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &Sink{cfg: c, file: f, size: st.Size(), queue: make(chan []byte, 256), done: make(chan struct{})}
	go s.run()
	return s, nil
}
func (s *Sink) Submit(t *Trace) {
	data, err := json.Marshal(t)
	if err != nil {
		s.Dropped.Add(1)
		return
	}
	data = append(data, '\n')
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.queue <- data:
	default:
		s.Dropped.Add(1)
	}
}
func (s *Sink) run() {
	defer close(s.done)
	defer func() {
		if s.file != nil {
			s.file.Close()
		}
	}()
	for data := range s.queue {
		if s.file == nil {
			s.Dropped.Add(1)
			continue
		}
		if s.size+int64(len(data)) > s.cfg.MaxBytes {
			s.file.Close()
			s.file = nil
			if err := os.Rename(s.cfg.Path, s.cfg.Path+".1"); err != nil {
				s.Dropped.Add(1)
				continue
			}
			f, err := open(s.cfg.Path)
			if err != nil {
				s.Dropped.Add(1)
				continue
			}
			s.file = f
			s.size = 0
		}
		n, err := s.file.Write(data)
		s.size += int64(n)
		if err != nil {
			s.Dropped.Add(1)
		}
	}
}
func (s *Sink) Close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
	<-s.done
}
