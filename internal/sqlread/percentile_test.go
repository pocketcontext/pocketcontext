package sqlread

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func TestPercentileValues(t *testing.T) {
	e, _ := fixture(t, Config{})
	for _, tc := range []struct {
		name, query string
		want        [][]any
	}{
		{"scales and interpolation", `SELECT median(value),percentile(value,75),percentile_cont(value,0.75),percentile_disc(value,0.75) FROM deals`, [][]any{{float64(50), float64(54), float64(54), float64(42)}}},
		{"endpoints", `SELECT percentile(value,0),percentile(value,100),percentile_cont(value,0),percentile_disc(value,1) FROM deals`, [][]any{{float64(42), float64(58), float64(42), float64(58)}}},
		{"empty", `SELECT median(value),percentile(value,50) FROM deals WHERE 0`, [][]any{{nil, nil}}},
		{"nulls", `WITH x(v) AS (VALUES(NULL),(2),(4)) SELECT median(v),percentile_cont(v,0.5) FROM x`, [][]any{{float64(3), float64(3)}}},
		{"moving window", `SELECT value,median(value) OVER w,percentile(value,75) OVER w,percentile_cont(value,0.75) OVER w,percentile_disc(value,0.75) OVER w FROM deals WINDOW w AS (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY value`, [][]any{{int64(42), float64(42), float64(42), float64(42), float64(42)}, {int64(58), float64(50), float64(54), float64(54), float64(42)}}},
		{"window removal", `WITH x(v) AS (VALUES(1),(3),(5),(9)) SELECT median(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM x ORDER BY v`, [][]any{{float64(1)}, {float64(2)}, {float64(4)}, {float64(7)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := e.Query(context.Background(), tc.query)
			if err != nil || !reflect.DeepEqual(r.Rows, tc.want) {
				t.Fatalf("rows = %#v, error = %v; want %#v", r.Rows, err, tc.want)
			}
		})
	}
}

func TestPercentileInvalidAndDeniedInputs(t *testing.T) {
	e, _ := fixture(t, Config{})
	for _, query := range []string{
		`SELECT percentile(value,-1) FROM deals`,
		`SELECT percentile(value,101) FROM deals`,
		`SELECT percentile_cont(value,1.1) FROM deals`,
		`SELECT percentile_disc(value,-0.1) FROM deals`,
		`SELECT percentile(value,NULL) FROM deals`,
		`SELECT percentile(value,'invalid') FROM deals`,
		`SELECT percentile(value,value) FROM deals`,
		`SELECT median(name) FROM people`,
		`SELECT median(1e999)`,
		`SELECT median(secret) FROM people`,
		`SELECT percentile((SELECT token FROM private),50) FROM deals`,
		`SELECT percentile_cont(secret,0.5) OVER () FROM people`,
		`SELECT percentile_disc(length(token),0.5) FROM private`,
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := e.Query(context.Background(), query); err == nil {
				t.Fatal("invalid or unauthorized query succeeded")
			}
		})
	}
	if _, err := e.Query(context.Background(), `SELECT median(value) FROM deals`); err != nil {
		t.Fatalf("connection unusable after errors: %v", err)
	}
}

func TestPercentileConcurrentCancellation(t *testing.T) {
	e, _ := fixture(t, Config{Timeout: 30 * time.Millisecond, MaxRows: 1})
	// A single result still accumulates inputs. Deadlines must interrupt both
	// aggregate accumulation and window preparation across the connection pool.
	queries := []string{
		`WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT median(n) FROM x`,
		`WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT percentile(n,95) FROM x`,
		`WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT percentile_cont(n,0.95) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM x`,
		`WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT percentile_disc(n,0.95) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM x`,
	}
	var wg sync.WaitGroup
	for _, query := range queries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Query(context.Background(), query); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected deadline, got %v", err)
			}
		}()
	}
	wg.Wait()
	for range queries {
		r, err := e.Query(context.Background(), `SELECT median(value) FROM deals`)
		if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != float64(50) {
			t.Fatalf("reader unusable after cancellation: %#v, %v", r, err)
		}
	}
}

func TestPercentileSnapshotVisibility(t *testing.T) {
	_, path, cfg, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, cfg, sc)
	for _, tc := range []struct {
		requester string
		want      float64
	}{{"alice", 150}, {"bob", 900}} {
		r, _, err := s.Query(context.Background(), tc.requester, `SELECT median(salary),percentile(salary,50),percentile_cont(salary,0.5) FROM pay`)
		if err != nil || !reflect.DeepEqual(r.Rows, [][]any{{tc.want, tc.want, tc.want}}) {
			t.Fatalf("%s: %#v, %v", tc.requester, r, err)
		}
		cleanSnapshot(t, s)
	}
	for _, query := range []string{`SELECT median(secret) FROM pay`, `SELECT percentile(length(employee),50) FROM managers`} {
		if _, _, err := s.Query(context.Background(), "alice", query); err == nil {
			t.Fatalf("unauthorized query succeeded: %s", query)
		}
		cleanSnapshot(t, s)
	}
}

func TestPercentileHeapLimit(t *testing.T) {
	// SQLite's heap limit is process-wide and can only be lowered. Exercise
	// allocation failure in a subprocess so other tests retain the normal cap.
	if os.Getenv("POCKETCONTEXT_PERCENTILE_HEAP_TEST") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPercentileHeapLimit$")
		cmd.Env = append(os.Environ(), "POCKETCONTEXT_PERCENTILE_HEAP_TEST=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("heap-limit subprocess: %v\n%s", err, output)
		}
		return
	}
	e, db := fixture(t, Config{Timeout: 5 * time.Second})
	if _, err := db.Exec(`PRAGMA hard_heap_limit=8388608`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.Query(context.Background(), `WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<10000000) SELECT median(n) FROM x`)
			var sqliteErr sqlite3.Error
			if !errors.As(err, &sqliteErr) || sqliteErr.Code != sqlite3.ErrNomem {
				t.Errorf("expected bounded allocation failure, got %v", err)
			}
		}()
	}
	wg.Wait()
	if _, err := e.Query(context.Background(), `SELECT median(value) FROM deals`); err != nil {
		t.Fatalf("reader unusable after allocation failures: %v", err)
	}
}
