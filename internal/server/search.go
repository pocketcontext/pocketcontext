package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/searchread"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
	"github.com/pocketcontext/pocketcontext/internal/tracing"
)

// Search indexes may only contain fields already exposed by shared SQL. Actual
// index contents remain the responsibility of trusted application maintenance.
func validateSearchSources(app core.App, cfg searchread.Config, schema []sqlread.Table) error {
	allowed := make(map[string]map[string]bool, len(schema))
	for _, table := range schema {
		columns := make(map[string]bool, len(table.Columns))
		for _, column := range table.Columns {
			columns[column.Name] = true
		}
		allowed[table.Name] = columns
	}
	for alias, index := range cfg.Indexes {
		if _, err := app.FindCollectionByNameOrId(index.Table); err == nil {
			return fmt.Errorf("search index %q table must not name a PocketBase collection", alias)
		}
		collection, err := app.FindCollectionByNameOrId(index.Collection)
		if err != nil || collection.Name != index.Collection || !collection.IsBase() || collection.System {
			return fmt.Errorf("search index %q requires a non-system source base collection", alias)
		}
		columns := allowed[index.Collection]
		for _, name := range append([]string{"id"}, index.Columns...) {
			field := collection.Fields.GetByName(name)
			if !columns[name] || field == nil || field.GetHidden() {
				return fmt.Errorf("search index %q source field %q must be SQL-readable and nonhidden", alias, name)
			}
		}
	}
	return nil
}

func searchDiscovery(cfg searchread.Config, maxRows int) map[string]any {
	aliases := make([]string, 0, len(cfg.Indexes))
	for alias := range cfg.Indexes {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	return map[string]any{
		"indexes": aliases,
		"limits": map[string]int{
			"maxQueryBytes": searchread.MaxQueryBytes,
			"maxTerms":      searchread.MaxTerms,
			"maxLimit":      min(maxRows, searchread.MaxLimit),
			"defaultLimit":  min(maxRows, searchread.DefaultLimit),
		},
	}
}

func registerSearchRoute(app core.App, event *core.ServeEvent, cfg Config, engine *searchread.Engine) {
	event.Router.POST("/api/context/search", func(re *core.RequestEvent) error {
		re.Response.Header().Set("Cache-Control", "no-store")
		var body struct {
			Index string `json:"index"`
			Query string `json:"query"`
			Limit int    `json:"limit"`
		}
		re.Request.Body = http.MaxBytesReader(re.Response, re.Request.Body, 65536)
		decoder := json.NewDecoder(re.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			return re.BadRequestError("Expected JSON with index, query and optional limit", nil)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return re.BadRequestError("Expected one JSON object", nil)
		}
		started := time.Now()
		result, err := engine.Search(re.Request.Context(), body.Index, body.Query, body.Limit)
		if trace := tracing.From(re.Request.Context()); trace != nil {
			trace.Rows, trace.Truncated = len(result.Hits), result.Truncated
		}
		alias := "<unknown>"
		if _, configured := cfg.Search.Indexes[body.Index]; configured {
			alias = body.Index
		}
		attrs := []any{"auth", re.Auth.Id, "collection", re.Auth.Collection().Name, "index", alias, "durationMs", time.Since(started).Milliseconds(), "rows", len(result.Hits), "truncated", result.Truncated}
		if err != nil {
			app.Logger().Warn("Context search rejected", attrs...)
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				return re.JSON(http.StatusRequestTimeout, map[string]string{"message": "Search deadline exceeded"})
			case errors.Is(err, searchread.ErrBusy):
				re.Response.Header().Set("Retry-After", "1")
				return re.JSON(http.StatusServiceUnavailable, map[string]string{"message": "Database busy; retry the search"})
			case errors.Is(err, searchread.ErrIndexInvalid):
				return re.JSON(http.StatusServiceUnavailable, map[string]string{"message": "Search index unavailable"})
			case errors.Is(err, searchread.ErrResponseLimit):
				return re.JSON(http.StatusRequestEntityTooLarge, map[string]string{"message": "Search result exceeds maxBytes"})
			case errors.Is(err, searchread.ErrInvalidQuery):
				return re.BadRequestError("Search request rejected", nil)
			default:
				return re.InternalServerError("Cannot search index", nil)
			}
		}
		app.Logger().Info("Context search", attrs...)
		encodeDone := tracing.Start(re.Request.Context(), "response.encode")
		payload, err := json.Marshal(result)
		encodeDone()
		if err != nil {
			return re.InternalServerError("Cannot encode search result", nil)
		}
		if len(payload) > cfg.MaxBytes {
			return re.JSON(http.StatusRequestEntityTooLarge, map[string]string{"message": "Encoded search result exceeds maxBytes"})
		}
		re.Response.Header().Add("Access-Control-Expose-Headers", "X-Context-Truncated")
		re.Response.Header().Set("X-Context-Truncated", fmt.Sprint(result.Truncated))
		return re.Blob(http.StatusOK, "application/json", payload)
	}).Bind(apis.RequireAuth(cfg.AuthCollection))
}
