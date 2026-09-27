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
	sink, err := tracing.NewSink(cfg.Tracing)
	if err != nil {
		return err
	}
	app.OnTerminate().BindFunc(func(te *core.TerminateEvent) error { defer sink.Close(); return te.Next() })
	var reported atomic.Uint64
	e.Router.Bind(&hook.Handler[*core.RequestEvent]{Id: "contextTracing", Priority: apis.DefaultLoadAuthTokenMiddlewarePriority - 1, Func: func(re *core.RequestEvent) error {
		if !strings.HasPrefix(re.Request.URL.Path, "/api/") || re.Request.URL.Path == "/api/health" {
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
		err := re.Next()
		if re.Auth == nil || re.Auth.Collection().Name != cfg.AuthCollection {
			return err
		}
		t.UserID = re.Auth.Id
		t.DurationMS = float64(time.Since(t.StartedAt)) / float64(time.Millisecond)
		t.Status = re.Status()
		if !re.Written() && err != nil {
			t.Status = router.ToApiError(err).Status
		}
		if t.Status == 0 {
			t.Status = 200
		}
		sink.Submit(t)
		if dropped := sink.Dropped.Load(); dropped > reported.Load() {
			previous := reported.Swap(dropped)
			if dropped > previous {
				app.Logger().Warn("context tracing records dropped", "count", dropped)
			}
		}
		return err
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
