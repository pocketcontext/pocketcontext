// Package server adds authenticated SQL reads to a PocketBase application.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

type Config struct {
	Tracing        tracing.Config          `json:"tracing"`
	AuthCollection string                  `json:"authCollection"`
	Tables         map[string][]string     `json:"tables"`
	TimeoutMS      int                     `json:"timeoutMs"`
	MaxRows        int                     `json:"maxRows"`
	MaxBytes       int                     `json:"maxBytes"`
	Snapshot       *sqlread.SnapshotConfig `json:"snapshot,omitempty"`
}

func LoadConfig(path string) (Config, error) {
	c := Config{TimeoutMS: 2000, MaxRows: 500, MaxBytes: 1048576}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("configuration must contain one JSON object")
	}
	if c.AuthCollection == "" || strings.HasPrefix(c.AuthCollection, "_") {
		return c, fmt.Errorf("authCollection must name an application auth collection")
	}
	if len(c.Tables) == 0 {
		return c, fmt.Errorf("tables must allow at least one context collection")
	}
	if c.TimeoutMS < 1 || c.TimeoutMS > 30000 || c.MaxRows < 1 || c.MaxRows > 10000 || c.MaxBytes < 1024 || c.MaxBytes > 10485760 {
		return c, fmt.Errorf("limits must be: timeoutMs 1..30000, maxRows 1..10000, maxBytes 1024..10485760")
	}
	if c.Snapshot != nil {
		if err := sqlread.NormalizeSnapshotConfig(c.Snapshot); err != nil {
			return c, err
		}
		if len(c.Snapshot.Filters) != len(c.Tables) {
			return c, fmt.Errorf("snapshot filters must match output tables")
		}
		for name, columns := range c.Tables {
			if len(columns) == 0 || strings.TrimSpace(c.Snapshot.Filters[name]) == "" {
				return c, fmt.Errorf("snapshot table %q requires explicit columns and a filter", name)
			}
		}
		for name, columns := range c.Snapshot.PolicyTables {
			if _, exists := c.Tables[name]; exists || len(columns) == 0 {
				return c, fmt.Errorf("snapshot policy table %q requires explicit columns and must not overlap output tables", name)
			}
		}
	}
	if err := c.Tracing.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Register loads configuration after migrations, when the serve command starts.
func Register(app core.App, configPath string) {
	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		cfg, err := LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("SQL configuration: %w", err)
		}
		if cfg.Tracing.Enabled {
			if err := registerTracing(app, e, cfg); err != nil {
				return err
			}
		}
		auth, err := app.FindCollectionByNameOrId(cfg.AuthCollection)
		if err != nil || !auth.IsAuth() {
			return fmt.Errorf("authCollection must be an existing auth collection")
		}
		validatedTables := make(map[string][]string, len(cfg.Tables))
		for name, columns := range cfg.Tables {
			validatedTables[name] = columns
		}
		if cfg.Snapshot != nil {
			for name, columns := range cfg.Snapshot.PolicyTables {
				validatedTables[name] = columns
			}
		}
		for name := range validatedTables {
			coll, err := app.FindCollectionByNameOrId(name)
			if err != nil || coll.Name != name || !coll.IsBase() || coll.System || strings.HasPrefix(name, "_") {
				return fmt.Errorf("SQL table %q must be a non-system base collection", name)
			}
			for _, field := range coll.Fields {
				if !field.GetHidden() {
					continue
				}
				columns := validatedTables[name]
				if len(columns) == 0 {
					return fmt.Errorf("SQL table %q has hidden fields; configure explicit public columns", name)
				}
				for _, column := range columns {
					if strings.EqualFold(column, field.GetName()) {
						return fmt.Errorf("SQL table %q includes hidden field %q", name, column)
					}
				}
			}
		}
		queryCfg := sqlread.Config{Tables: cfg.Tables, Timeout: time.Duration(cfg.TimeoutMS) * time.Millisecond, MaxRows: cfg.MaxRows, MaxBytes: cfg.MaxBytes}
		var schemaFn func(context.Context) ([]sqlread.Table, error)
		var queryFn func(context.Context, string, string) (sqlread.Result, time.Time, error)
		var closeFn func() error
		permissionModel := "shared"
		if cfg.Snapshot != nil {
			source, err := sqlread.NewSnapshotSource(filepath.Join(app.DataDir(), "data.db"), queryCfg, *cfg.Snapshot)
			if err != nil {
				return err
			}
			schemaFn, queryFn, closeFn = source.Schema, source.Query, source.Close
			permissionModel = "filtered-snapshot"
		} else {
			engine, err := sqlread.New(filepath.Join(app.DataDir(), "data.db"), queryCfg)
			if err != nil {
				return err
			}
			schemaFn, closeFn = engine.Schema, engine.Close
			queryFn = func(ctx context.Context, _ string, query string) (sqlread.Result, time.Time, error) {
				result, err := engine.Query(ctx, query)
				return result, time.Time{}, err
			}
		}
		app.OnTerminate().BindFunc(func(te *core.TerminateEvent) error { defer closeFn(); return te.Next() })
		e.Router.GET("/api/context/schema", func(re *core.RequestEvent) error {
			re.Response.Header().Set("Cache-Control", "no-store")
			schema, err := schemaFn(re.Request.Context())
			if err != nil {
				return re.InternalServerError("Cannot read context schema", nil)
			}
			response := map[string]any{"tables": schema, "permissionModel": permissionModel, "limits": map[string]int{"timeoutMs": cfg.TimeoutMS, "maxRows": cfg.MaxRows, "maxBytes": cfg.MaxBytes}}
			if cfg.Snapshot != nil {
				response["snapshotLimits"] = map[string]int{"timeoutMs": cfg.Snapshot.TimeoutMS, "maxRows": cfg.Snapshot.MaxRows, "maxBytes": cfg.Snapshot.MaxBytes, "maxConcurrent": cfg.Snapshot.MaxConcurrent}
			}
			return re.JSON(http.StatusOK, response)
		}).Bind(apis.RequireAuth(cfg.AuthCollection))
		e.Router.POST("/api/context/query", func(re *core.RequestEvent) error {
			re.Response.Header().Set("Cache-Control", "no-store")
			var body struct {
				SQL    string `json:"sql"`
				Format string `json:"format"`
			}
			re.Request.Body = http.MaxBytesReader(re.Response, re.Request.Body, 65536)
			decoder := json.NewDecoder(re.Request.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&body); err != nil {
				return re.BadRequestError("Expected JSON with sql and optional format", nil)
			}
			if err := decoder.Decode(new(any)); err != io.EOF {
				return re.BadRequestError("Expected one JSON object", nil)
			}
			if body.Format != "" && body.Format != "json" && body.Format != "csv" {
				return re.BadRequestError("format must be json or csv", nil)
			}
			trace := tracing.From(re.Request.Context())
			if trace != nil && cfg.Tracing.CaptureSQL {
				trace.SQL = body.SQL
				if len(trace.SQL) > 16384 {
					trace.SQL = trace.SQL[:16384]
					for !utf8.ValidString(trace.SQL) {
						trace.SQL = trace.SQL[:len(trace.SQL)-1]
					}
				}
			}
			started := time.Now()
			result, snapshotAt, err := queryFn(re.Request.Context(), re.Auth.Id, body.SQL)
			if trace != nil {
				trace.Rows = len(result.Rows)
				trace.Truncated = result.Truncated
			}
			logSQL(app, re, body.SQL, body.Format, started, result, err)
			if err != nil {
				if errors.Is(err, sqlread.ErrSnapshotLimit) {
					return re.JSON(http.StatusRequestEntityTooLarge, map[string]string{"message": "Authorized snapshot exceeds configured limits"})
				}
				if errors.Is(err, sqlread.ErrSnapshotBuild) {
					return re.InternalServerError("Cannot build authorized snapshot", nil)
				}
				if errors.Is(err, context.DeadlineExceeded) {
					return re.JSON(http.StatusRequestTimeout, map[string]string{"message": "SQL deadline exceeded"})
				}
				if errors.Is(err, sqlread.ErrBusy) {
					re.Response.Header().Set("Retry-After", "1")
					return re.JSON(http.StatusServiceUnavailable, map[string]string{"message": "Database busy; retry the query"})
				}
				return re.BadRequestError("SQL query rejected: "+err.Error(), nil)
			}
			encodeDone := tracing.Start(re.Request.Context(), "response.encode")
			var payload []byte
			contentType := "application/json"
			if body.Format == "csv" {
				payload, err = result.CSV()
				contentType = "text/csv; charset=utf-8"
			} else {
				payload, err = json.Marshal(result)
			}
			encodeDone()
			if err != nil {
				return re.InternalServerError("Cannot encode query result", nil)
			}
			if len(payload) > cfg.MaxBytes {
				return re.JSON(http.StatusRequestEntityTooLarge, map[string]string{"message": "Encoded result exceeds maxBytes; select fewer or smaller fields"})
			}
			if cfg.Snapshot != nil {
				re.Response.Header().Add("Access-Control-Expose-Headers", "X-Context-Scope, X-Context-Snapshot-At, X-Context-Truncated")
				re.Response.Header().Set("X-Context-Scope", "authorized-snapshot")
				re.Response.Header().Set("X-Context-Snapshot-At", snapshotAt.UTC().Format(time.RFC3339Nano))
			}
			re.Response.Header().Set("X-Context-Truncated", fmt.Sprint(result.Truncated))
			return re.Blob(http.StatusOK, contentType, payload)
		}).Bind(apis.RequireAuth(cfg.AuthCollection))
		if err = e.Next(); err != nil {
			closeFn()
			return err
		}
		return nil
	})
}

// logSQL writes one audit line per query with the agent identity and the SQL text.
func logSQL(app core.App, re *core.RequestEvent, sql, format string, started time.Time, result sqlread.Result, err error) {
	if format == "" {
		format = "json"
	}
	text := sql
	if runes := []rune(text); len(runes) > 2000 {
		text = string(runes[:2000])
	}
	attrs := []any{
		"auth", re.Auth.Id,
		"collection", re.Auth.Collection().Name,
		"durationMs", time.Since(started).Milliseconds(),
		"rows", len(result.Rows),
		"truncated", result.Truncated,
		"format", format,
		"sqlBytes", len(sql),
		"sql", text,
	}
	if err != nil {
		message := err.Error()
		if errors.Is(err, sqlread.ErrSnapshotBuild) {
			message = "Cannot build authorized snapshot"
		}
		app.Logger().Warn("context sql", append(attrs, "error", message)...)
		return
	}
	app.Logger().Info("context sql", attrs...)
}
