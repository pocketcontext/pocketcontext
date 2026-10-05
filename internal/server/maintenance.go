package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/pocketcontext/pocketcontext/internal/database"
)

const maintenancePath = "/api/context/maintenance"

// requestDrain counts accepted mutations through completion of their handler,
// including file handling that can happen outside a database transaction.
type requestDrain struct {
	mu      sync.Mutex
	blocked bool
	active  int
	changed chan struct{}
}

func (d *requestDrain) signal() {
	if d.changed != nil {
		close(d.changed)
	}
	d.changed = make(chan struct{})
}

func (d *requestDrain) enter() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.blocked {
		return false
	}
	d.active++
	return true
}

func (d *requestDrain) leave() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active--
	d.signal()
}

func (d *requestDrain) block(v bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.blocked = v
	d.signal()
}

func (d *requestDrain) wait(ctx context.Context) error {
	d.mu.Lock()
	for d.active != 0 {
		if d.changed == nil {
			d.changed = make(chan struct{})
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
		d.mu.Lock()
	}
	d.mu.Unlock()
	return nil
}

// Only known reader operations bypass request admission. In particular OAuth,
// password authentication and administrative operations aren't presumed reads.
func maintenanceRead(r *http.Request) bool {
	p := r.URL.Path
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if r.Method == http.MethodOptions {
		return true
	}
	if r.Method == http.MethodPost {
		return p == "/api/context/query" || p == "/api/context/search" ||
			p == "/api/files/token" || p == "/api/realtime" ||
			(len(parts) == 4 && parts[0] == "api" && parts[1] == "collections" && parts[3] == "auth-refresh")
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	if p == "/" || p == "/up" || strings.HasPrefix(p, "/assets/") {
		return true
	}
	collectionRead := (len(parts) == 4 && parts[3] == "auth-methods") ||
		((len(parts) == 4 || len(parts) == 5) && parts[3] == "records")
	fileRead := len(parts) == 5 && parts[0] == "api" && parts[1] == "files" && r.URL.Query().Get("thumb") == ""
	return p == "/api/health" || p == "/api/context/schema" ||
		p == "/api/realtime" || fileRead ||
		(strings.HasPrefix(p, "/api/collections/") && collectionRead)
}

// RegisterMaintenance adds process-local maintenance control. Database enforcement
// is independent of this request layer; arbitrary external hook side effects are
// not a capability provided by SQLite and need application-specific treatment.
func RegisterMaintenance(app core.App, controller *database.Controller) {
	drain := &requestDrain{blocked: controller.Status().State != "writable"}
	var transition sync.Mutex
	app.Store().Set("pocketcontextMaintenanceReadOnly", drain.blocked)
	// PocketBase prints entire buffered log models when auxiliary persistence
	// fails. Drop persistent log entries during maintenance rather than triggering
	// that error path; no settings/database mutation or new logging sink is needed.
	app.OnModelCreate().Bind(&hook.Handler[*core.ModelEvent]{
		Id: "pocketcontextMaintenanceLogs", Priority: -1000,
		Func: func(e *core.ModelEvent) error {
			if _, ok := e.Model.(*core.Log); ok && controller.Status().State != "writable" {
				return nil
			}
			return e.Next()
		},
	})
	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		e.Router.Bind(&hook.Handler[*core.RequestEvent]{
			Id: "pocketcontextMaintenance", Priority: apis.DefaultLoadAuthTokenMiddlewarePriority + 10,
			Func: func(re *core.RequestEvent) error {
				if re.Request.URL.Path == maintenancePath || maintenanceRead(re.Request) {
					return re.Next()
				}
				if !drain.enter() {
					return re.JSON(http.StatusServiceUnavailable, map[string]any{"status": 503, "code": "maintenance_read_only", "message": "The application is read-only for maintenance."})
				}
				defer drain.leave()
				return re.Next()
			},
		})
		e.Router.GET(maintenancePath, func(re *core.RequestEvent) error {
			return re.JSON(http.StatusOK, controller.Status())
		}).Bind(apis.RequireSuperuserAuth())
		e.Router.PUT(maintenancePath, func(re *core.RequestEvent) error {
			var input struct {
				ReadOnly           *bool   `json:"readOnly"`
				ExpectedGeneration *uint64 `json:"expectedGeneration"`
			}
			dec := json.NewDecoder(http.MaxBytesReader(re.Response, re.Request.Body, 1024))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&input); err != nil || input.ReadOnly == nil || input.ExpectedGeneration == nil {
				return re.BadRequestError("readOnly and expectedGeneration are required.", nil)
			}
			if err := dec.Decode(new(any)); err != io.EOF {
				return re.BadRequestError("Expected one JSON object.", nil)
			}
			if !transition.TryLock() {
				return re.JSON(http.StatusConflict, map[string]string{"message": "A maintenance transition is in progress."})
			}
			defer transition.Unlock()
			ctx, cancel := context.WithTimeout(re.Request.Context(), 30*time.Second)
			defer cancel()
			var status database.Status
			var err error
			if *input.ReadOnly {
				status, err = controller.BeginFreeze(*input.ExpectedGeneration)
				if status.State != "writable" {
					drain.block(true)
					app.Store().Set("pocketcontextMaintenanceReadOnly", true)
				}
				if err == nil {
					err = drain.wait(ctx)
					if err == nil {
						status, err = controller.CompleteFreeze(ctx)
					}
				}
			} else {
				status, err = controller.Unfreeze(ctx, *input.ExpectedGeneration)
				if err == nil {
					app.Store().Set("pocketcontextMaintenanceReadOnly", false)
					drain.block(false)
				}
			}
			if err != nil {
				code := http.StatusServiceUnavailable
				if errors.Is(err, database.ErrGenerationConflict) || errors.Is(err, database.ErrTransition) {
					code = http.StatusConflict
				}
				return re.JSON(code, map[string]any{"message": "Maintenance transition did not complete; inspect status before retrying.", "maintenance": controller.Status()})
			}
			return re.JSON(http.StatusOK, status)
		}).Bind(apis.RequireSuperuserAuth())
		return e.Next()
	})
}
