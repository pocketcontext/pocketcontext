package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext/pocketcontext/internal/sqlread"
)

func registerSnapshotStatus(e *core.ServeEvent, authCollection string, source *sqlread.SnapshotSource) {
	e.Router.GET("/api/context/snapshot", func(re *core.RequestEvent) error {
		re.Response.Header().Set("Cache-Control", "no-store")
		status, err := source.SnapshotStatus(re.Request.Context(), re.Auth.Id)
		if err != nil {
			if handled, responseErr := snapshotHTTPError(re, err); handled {
				return responseErr
			}
			return re.InternalServerError("Cannot read authorized snapshot status", nil)
		}
		setSnapshotHeaders(re, status)
		return re.JSON(http.StatusOK, status)
	}).Bind(apis.RequireAuth(authCollection))
}

func snapshotHTTPError(re *core.RequestEvent, err error) (bool, error) {
	status, code, message := 0, "", ""
	switch {
	case errors.Is(err, sqlread.ErrSnapshotPending):
		status, code, message = http.StatusServiceUnavailable, "snapshot_pending", "An authorized snapshot is being prepared; retry the read"
	case errors.Is(err, sqlread.ErrSnapshotResource):
		status, code, message = http.StatusServiceUnavailable, "snapshot_resource_limit", "Snapshot refresh cannot fit within the server resource budget"
	case errors.Is(err, sqlread.ErrSnapshotGeneration):
		status, code, message = http.StatusConflict, "snapshot_generation_conflict", "The requested snapshot generation is unavailable; restart the read"
	default:
		return false, nil
	}
	if status == http.StatusServiceUnavailable {
		re.Response.Header().Set("Retry-After", "1")
		re.Response.Header().Add("Access-Control-Expose-Headers", "Retry-After")
	}
	return true, re.JSON(status, map[string]string{"code": code, "message": message})
}

func setSnapshotHeaders(re *core.RequestEvent, snapshot sqlread.SnapshotMetadata) {
	headers := re.Response.Header()
	headers.Add("Access-Control-Expose-Headers", "X-Context-Snapshot-Generation, X-Context-Snapshot-At, X-Context-Snapshot-Stale, X-Context-Snapshot-Refreshing, X-Context-Snapshot-Rows, X-Context-Snapshot-Disk-Bytes")
	if snapshot.Generation != "" {
		headers.Set("X-Context-Snapshot-Generation", snapshot.Generation)
	}
	if !snapshot.CapturedAt.IsZero() {
		headers.Set("X-Context-Snapshot-At", snapshot.CapturedAt.UTC().Format(time.RFC3339Nano))
	}
	headers.Set("X-Context-Snapshot-Stale", strconv.FormatBool(snapshot.Stale))
	headers.Set("X-Context-Snapshot-Refreshing", strconv.FormatBool(snapshot.Refreshing))
	headers.Set("X-Context-Snapshot-Rows", strconv.FormatInt(snapshot.Rows, 10))
	headers.Set("X-Context-Snapshot-Disk-Bytes", strconv.FormatInt(snapshot.DiskBytes, 10))
}
