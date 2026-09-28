package sqlread

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestJSONTraversal(t *testing.T) {
	e, _ := fixture(t, Config{})
	for _, tc := range []struct {
		sql  string
		want [][]any
	}{
		{`SELECT key,value FROM json_each('["a",2]') ORDER BY key`, [][]any{{int64(0), "a"}, {int64(1), int64(2)}}},
		{`SELECT fullkey,atom FROM json_tree('{"a":[1,2]}') WHERE atom IS NOT NULL ORDER BY fullkey`, [][]any{{"$.a[0]", int64(1)}, {"$.a[1]", int64(2)}}},
		{`SELECT p.id,j.value FROM people p,json_each(json_array(p.name)) j`, [][]any{{"p1", "Ada"}}},
		{`WITH j AS (SELECT value FROM json_each('[1,2]')) SELECT sum(value) FROM j`, [][]any{{int64(3)}}},
		{`SELECT count(*) FROM json_each('[1,2]')`, [][]any{{int64(2)}}},
		{`SELECT value FROM json_tree('{"a":[1]}','$.a') WHERE type='integer'`, [][]any{{int64(1)}}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			r, err := e.Query(context.Background(), tc.sql)
			if err != nil || !reflect.DeepEqual(r.Rows, tc.want) {
				t.Fatalf("%#v %v", r, err)
			}
		})
	}
	for _, q := range []string{
		`SELECT value FROM people,json_each(json_array(secret))`,
		`SELECT value FROM json_each((SELECT json_group_array(token) FROM private))`,
		`SELECT value FROM json_tree((SELECT json_group_array(sql) FROM sqlite_schema))`,
		`SELECT value FROM json_each((SELECT json_group_array(name) FROM pragma_table_info('private')))`,
		`SELECT value FROM json_each((SELECT json_group_array(token) FROM leak))`,
		`WITH json_each AS (SELECT token AS value FROM private) SELECT value FROM json_each`,
		`SELECT value FROM json_each('[1]') UNION SELECT token FROM private`,
		`SELECT * FROM json_each('invalid')`,
		`SELECT * FROM json_tree('{}','invalid')`,
		`SELECT * FROM jsonb_each('[1]')`,
	} {
		if r, err := e.Query(context.Background(), q); err == nil {
			t.Fatalf("allowed %s: %#v", q, r)
		}
	}
	if _, err := e.Query(context.Background(), `SELECT value FROM json_each('[3]')`); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`SELECT secret FROM people`, `SELECT sql FROM sqlite_schema`} {
		if _, err := e.Query(context.Background(), q); err == nil {
			t.Fatalf("authorization lost after errors: %s", q)
		}
	}

}

func TestJSONModuleNameCollision(t *testing.T) {
	for _, ddl := range []string{`CREATE TABLE JSON_EACH(value TEXT); INSERT INTO JSON_EACH VALUES('private')`, `CREATE TABLE json_tree(value TEXT); INSERT INTO json_tree VALUES('private')`, `CREATE VIEW json_each AS SELECT token AS value FROM private`, `CREATE VIRTUAL TABLE json_each USING fts3(value); INSERT INTO json_each VALUES('private')`} {
		t.Run(ddl, func(t *testing.T) {
			e, db := fixture(t, Config{})
			if _, err := e.Query(context.Background(), `SELECT value FROM json_each('[1]')`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			// This also exercises already-open connections after a schema change.
			for _, q := range []string{`SELECT value FROM json_each`, `SELECT count(*) FROM json_tree`, `SELECT value FROM temp.json_each('[1]')`} {
				if _, err := e.Query(context.Background(), q); err == nil || !strings.Contains(err.Error(), "reserved SQL module names") {
					t.Fatalf("%s: %v", q, err)
				}
			}
		})
	}
}

func TestJSONTraversalLimits(t *testing.T) {
	e, _ := fixture(t, Config{MaxRows: 2, Timeout: 30 * time.Millisecond})
	r, err := e.Query(context.Background(), `SELECT value FROM json_each('[1,2,3,4]')`)
	if err != nil || len(r.Rows) != 2 || !r.Truncated {
		t.Fatalf("%#v %v", r, err)
	}
	_, err = e.Query(context.Background(), `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<100000000) SELECT sum(j.value + seq.n) FROM seq,json_each('[1,2,3,4]') j`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err = e.Query(context.Background(), `SELECT value FROM json_each('[1]')`); err != nil {
		t.Fatalf("after cancel: %v", err)
	}
	for _, q := range []string{`SELECT secret FROM people`, `SELECT sql FROM sqlite_schema`} {
		if _, err = e.Query(context.Background(), q); err == nil {
			t.Fatalf("authorization lost after cancel: %s", q)
		}
	}
}

func TestJSONSnapshotVisibility(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	for _, tc := range []struct {
		user string
		want [][]any
	}{{"alice", [][]any{{int64(100)}, {int64(200)}}}, {"bob", [][]any{{int64(900)}}}, {"stranger", [][]any{}}} {
		r, _, err := s.Query(context.Background(), tc.user, `SELECT j.value FROM pay,json_each(json_array(salary)) j ORDER BY j.value`)
		if err != nil || !reflect.DeepEqual(r.Rows, tc.want) {
			t.Fatalf("%s: %#v %v", tc.user, r, err)
		}
	}
	for _, q := range []string{`SELECT value FROM pay,json_each(json_array(secret))`, `SELECT value FROM json_each((SELECT json_group_array(requester) FROM managers))`} {
		if _, _, err := s.Query(context.Background(), "alice", q); err == nil {
			t.Fatalf("allowed %s", q)
		}
	}
	if _, err := db.Exec(`DELETE FROM managers WHERE requester='alice'`); err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Query(context.Background(), "alice", `SELECT j.value FROM pay,json_tree(json_array(salary)) j`)
	if err != nil || len(r.Rows) != 0 {
		t.Fatalf("revocation: %#v %v", r, err)
	}
	cleanSnapshot(t, s)
}

func TestJSONSchemaChangeDuringQuery(t *testing.T) {
	e, db := fixture(t, Config{})
	e.db.SetMaxOpenConns(1)
	conn, err := e.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = conn.Raw(func(raw any) error {
		// Override an already-allowed scalar only in this isolated test. The WAL
		// writer commits a conflicting name after authorization, during SQLite step.
		return raw.(roConn).c.RegisterFunc("abs", func(value int64) (int64, error) {
			called = true
			_, err := db.Exec(`CREATE TABLE json_each(value INTEGER); INSERT INTO json_each VALUES(999)`)
			return value, err
		}, false)
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.Query(context.Background(), `SELECT abs(value) FROM json_each('[7]')`)
	if err != nil || !called || !reflect.DeepEqual(r.Rows, [][]any{{int64(7)}}) {
		t.Fatalf("in-flight query: %#v, called=%t, %v", r, called, err)
	}
	if _, err = e.Query(context.Background(), `SELECT value FROM json_each`); err == nil || !strings.Contains(err.Error(), "reserved SQL module names") {
		t.Fatalf("post-commit collision: %v", err)
	}
}
