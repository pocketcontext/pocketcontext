package server

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/pocketbase/pocketbase/tools/router"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func registerTracing(app core.App, e *core.ServeEvent, cfg Config) error {
	var sink *tracing.Sink
	var buffer *tracing.Buffer
	if cfg.Tracing.Delivery == "buffer" {
		buffer = tracing.NewBuffer(cfg.Tracing.MaxBytes)
		e.Router.GET("/api/context/traces/{request_id}", func(re *core.RequestEvent) error {
			re.Response.Header().Set("Cache-Control", "no-store")
			if re.Auth == nil || re.Auth.Collection().Name != cfg.AuthCollection {
				return re.NotFoundError("Trace not found", nil)
			}
			// Bind retrieval to the current token key too, so fresh login after revocation
			// cannot recover traces belonging to the previous authenticated session.
			current, err := re.App.FindRecordById(re.Auth.Collection().Id, re.Auth.Id)
			if err != nil || current.TokenKey() != re.Auth.TokenKey() {
				return re.NotFoundError("Trace not found", nil)
			}
			id := re.Request.PathValue("request_id")
			if !requestIDPattern.MatchString(id) {
				return re.NotFoundError("Trace not found", nil)
			}
			data, ok := buffer.Get(traceOwner(current), id)
			if !ok {
				return re.NotFoundError("Trace not found", nil)
			}
			return re.Blob(200, "application/json", data)
		}).Bind(apis.RequireAuth(cfg.AuthCollection))
	} else {
		var err error
		sink, err = tracing.NewSink(cfg.Tracing)
		if err != nil {
			return err
		}
		app.OnTerminate().BindFunc(func(te *core.TerminateEvent) error { defer sink.Close(); return te.Next() })
	}
	var reported atomic.Uint64
	e.Router.Bind(&hook.Handler[*core.RequestEvent]{Id: "contextTracing", Priority: apis.DefaultLoadAuthTokenMiddlewarePriority - 1, Func: func(re *core.RequestEvent) error {
		if !strings.HasPrefix(re.Request.URL.Path, "/api/") || re.Request.URL.Path == "/api/health" || strings.HasPrefix(re.Request.URL.Path, "/api/context/traces/") || (buffer != nil && re.Request.Header.Get("X-Context-Trace") != "1") {
			return re.Next()
		}
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return re.Next()
		}
		t := &tracing.Trace{Version: 1, RequestID: hex.EncodeToString(id), Service: cfg.Tracing.Service, Method: re.Request.Method, Route: traceRoute(re.Request.Pattern), StartedAt: time.Now(), Spans: []tracing.Span{}}
		correlation := re.Request.Header.Get("X-Context-Correlation-Id")
		if correlationPattern.MatchString(correlation) {
			t.CorrelationID = correlation
		}
		re.Request = re.Request.WithContext(tracing.With(re.Request.Context(), t))
		re.Response.Header().Set("X-Context-Request-Id", t.RequestID)
		re.Response.Header().Add("Access-Control-Expose-Headers", "X-Context-Request-Id")
		var requestErr error
		returned := false
		// Finalize during unwinding without recovering: PocketBase retains its
		// normal panic handling, including http.ErrAbortHandler propagation.
		defer func() {
			if re.Auth == nil || re.Auth.Collection().Name != cfg.AuthCollection {
				return
			}
			t.UserID = re.Auth.Id
			t.DurationMS = float64(time.Since(t.StartedAt)) / float64(time.Millisecond)
			t.Status = re.Status()
			if !re.Written() {
				if !returned {
					t.Status = 500
				} else if requestErr != nil {
					t.Status = router.ToApiError(requestErr).Status
				}
			}
			if t.Status == 0 {
				t.Status = 200
			}
			if buffer != nil {
				buffer.Submit(traceOwner(re.Auth), t)
				return
			}
			sink.Submit(t)
			if dropped := sink.Dropped.Load(); dropped > reported.Load() {
				previous := reported.Swap(dropped)
				if dropped > previous {
					app.Logger().Warn("context tracing records dropped", "count", dropped)
				}
			}
		}()
		requestErr = re.Next()
		returned = true
		return requestErr
	}})
	e.Router.Bind(&hook.Handler[*core.RequestEvent]{Id: "contextTraceAuth", Priority: apis.DefaultLoadAuthTokenMiddlewarePriority + 1, Func: func(re *core.RequestEvent) error {
		if t := tracing.From(re.Request.Context()); t != nil {
			t.Spans = append(t.Spans, tracing.Span{Name: "auth", DurationMS: float64(time.Since(t.StartedAt)) / float64(time.Millisecond)})
		}
		return re.Next()
	}})
	return nil
}

// A GET route also matches HEAD; the pattern method need not equal the request method.
func traceRoute(pattern string) string {
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

var requestIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

func traceOwner(record *core.Record) string {
	return record.Collection().Id + ":" + record.Id + ":" + record.TokenKey()
}
