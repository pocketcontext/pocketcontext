package sqlread

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func snapshotFixture(t *testing.T) (*sql.DB, string, Config, SnapshotConfig) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`PRAGMA journal_mode=WAL;
 CREATE TABLE pay(id TEXT, salary INTEGER, secret TEXT);
 CREATE TABLE managers(requester TEXT, employee TEXT);
 INSERT INTO pay VALUES('a',100,'private-a'),('b',200,'private-b'),('c',900,'private-c');
 INSERT INTO managers VALUES('alice','a'),('alice','b'),('bob','c');`)
	if err != nil {
		t.Fatal(err)
	}
	return db, path, Config{Tables: map[string][]string{"pay": {"id", "salary"}}, Timeout: time.Second, MaxRows: 100, MaxBytes: 4096}, SnapshotConfig{Filters: map[string]string{"pay": "EXISTS (SELECT 1 FROM managers WHERE requester=:requester AND employee=pay.id)"}, PolicyTables: map[string][]string{"managers": {"requester", "employee"}}}
}
func newTestSnapshot(t *testing.T, path string, c Config, sc SnapshotConfig) *SnapshotSource {
	t.Helper()
	s, e := NewSnapshotSource(path, c, sc)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func cleanSnapshot(t *testing.T, s *SnapshotSource) {
	t.Helper()
	entries, err := os.ReadDir(s.root)
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if err != nil || len(entries) != len(s.cache) {
		t.Fatalf("snapshot files not cleaned: %v %v", entries, err)
	}
}
func TestSnapshotIsolationAndRevocation(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	for _, tc := range []struct {
		user string
		sum  int64
	}{{"alice", 300}, {"bob", 900}} {
		r, at, e := s.Query(context.Background(), tc.user, "SELECT sum(salary) FROM pay")
		if e != nil || r.Rows[0][0] != tc.sum || at.IsZero() {
			t.Fatalf("%s: %#v %v %v", tc.user, r, at, e)
		}
		cleanSnapshot(t, s)
	}
	if _, e := db.Exec("DELETE FROM managers WHERE requester='alice'"); e != nil {
		t.Fatal(e)
	}
	r, _, e := s.Query(context.Background(), "alice", "SELECT id,salary FROM pay")
	if e != nil || len(r.Rows) != 0 || len(r.Columns) != 2 {
		t.Fatalf("revocation/empty schema: %#v %v", r, e)
	}
	for _, query := range []string{"SELECT * FROM managers", "SELECT secret FROM pay", "SELECT * FROM sqlite_schema", "ATTACH DATABASE 'source.db' AS source", "DELETE FROM pay", "BEGIN", "SELECT load_extension('x')", "SELECT * FROM pay; SELECT * FROM pay"} {
		if _, _, e := s.Query(context.Background(), "bob", query); e == nil {
			t.Fatalf("accepted %s", query)
		}
		cleanSnapshot(t, s)
	}
	r, _, e = s.Query(context.Background(), "alice' OR 1=1 --", "SELECT id FROM pay")
	if e != nil || len(r.Rows) != 0 {
		t.Fatalf("identity interpolation: %#v %v", r, e)
	}
	if _, _, e = s.Query(context.Background(), "", "SELECT * FROM pay"); !errors.Is(e, ErrSnapshotBuild) {
		t.Fatal(e)
	}
	schema, e := s.Schema(context.Background())
	if e != nil || len(schema) != 1 || len(schema[0].Columns) != 2 {
		t.Fatalf("schema %#v %v", schema, e)
	}
	root := s.root
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(root); !os.IsNotExist(e) {
		t.Fatal("root remains", e)
	}
}
func TestSnapshotConfigRejected(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	for _, filter := range []string{"", "salary=:other", "salary=?", "salary=:requester OR salary=:other", "1); DELETE FROM pay; --", "EXISTS(SELECT 1 FROM sqlite_schema)", "secret='x'", "load_extension('x')"} {
		sc.Filters = map[string]string{"pay": filter}
		s, e := NewSnapshotSource(path, c, sc)
		if e == nil {
			s.Close()
			t.Fatalf("accepted %q", filter)
		}
	}
	sc.Filters = map[string]string{"pay": "1", "extra": "1"}
	if s, e := NewSnapshotSource(path, c, sc); e == nil {
		s.Close()
		t.Fatal("extra filter")
	}
	sc.Filters = map[string]string{"pay": "1"}
	sc.PolicyTables = map[string][]string{"PAY": {"id"}}
	if s, e := NewSnapshotSource(path, c, sc); e == nil {
		s.Close()
		t.Fatal("overlap")
	}
	sc.PolicyTables = nil
	c.Tables["pay"] = nil
	if s, e := NewSnapshotSource(path, c, sc); e == nil {
		s.Close()
		t.Fatal("implicit columns")
	}
	for _, bad := range []SnapshotConfig{{TimeoutMS: -1}, {TimeoutMS: 30001}, {MaxRows: 1000001}, {MaxBytes: 1023}, {MaxConcurrent: 17}} {
		if NormalizeSnapshotConfig(&bad) == nil {
			t.Fatal("invalid limits")
		}
	}
}
func TestSnapshotLimitsCancellationAndCapacity(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	sc.MaxRows = 1
	s := newTestSnapshot(t, path, c, sc)
	r, at, e := s.Query(context.Background(), "alice", "SELECT count(*) FROM pay")
	if !errors.Is(e, ErrSnapshotLimit) || !at.IsZero() || len(r.Rows) != 0 {
		t.Fatalf("partial snapshot: %#v %v %v", r, at, e)
	}
	cleanSnapshot(t, s)
	sc.MaxRows = 100
	sc.MaxBytes = 1024
	s2 := newTestSnapshot(t, path, c, sc)
	if _, e = db.Exec("UPDATE pay SET salary=? WHERE id='a'", strings.Repeat("x", 1100)); e != nil {
		t.Fatal(e)
	}
	_, _, e = s2.Query(context.Background(), "alice", "SELECT id FROM pay")
	if !errors.Is(e, ErrSnapshotLimit) {
		t.Fatal("byte cap", e)
	}
	cleanSnapshot(t, s2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, e = s2.Query(ctx, "alice", "SELECT id FROM pay")
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	cleanSnapshot(t, s2)
	sc.MaxConcurrent = 1
	sc.TimeoutMS = 10
	s3 := newTestSnapshot(t, path, c, sc)
	s3.slots <- struct{}{}
	_, _, e = s3.Query(context.Background(), "bob", "SELECT id FROM pay")
	<-s3.slots
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("queue timeout", e)
	}
	cleanSnapshot(t, s3)
}
func TestSnapshotPreservesStorageClasses(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	_, e := db.Exec(`CREATE TABLE values_test(b BOOLEAN,d DATE,blob BLOB,n INTEGER,r REAL,z TEXT);
 INSERT INTO values_test VALUES(-1,'not-a-date',x'00ff',42,1.25,NULL);`)
	if e != nil {
		t.Fatal(e)
	}
	c.Tables = map[string][]string{"values_test": {"b", "d", "blob", "n", "r", "z"}}
	sc.Filters = map[string]string{"values_test": "1"}
	sc.PolicyTables = nil
	s := newTestSnapshot(t, path, c, sc)
	r, _, e := s.Query(context.Background(), "alice", "SELECT quote(b),quote(d),hex(blob),typeof(n),typeof(r),typeof(z) FROM values_test")
	if e != nil {
		t.Fatal(e)
	}
	want := []string{"-1", "'not-a-date'", "00FF", "integer", "real", "null"}
	for i, v := range want {
		if r.Rows[0][i] != v {
			t.Fatalf("storage changed: %#v", r)
		}
	}
}

func TestSnapshotExportTimeout(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc.TimeoutMS = 20
	sc.Filters["pay"] = `(WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n)>0`
	s := newTestSnapshot(t, path, c, sc)
	_, at, err := s.Query(context.Background(), "alice", "SELECT count(*) FROM pay")
	if !errors.Is(err, context.DeadlineExceeded) || !at.IsZero() {
		t.Fatalf("timeout: %v %v", at, err)
	}
	cleanSnapshot(t, s)
}

func BenchmarkSnapshot(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "source.db")
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			_, err = db.Exec(`CREATE TABLE records(id INTEGER, owner TEXT, amount INTEGER, body TEXT);
 WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO records SELECT x,'alice',x,'synthetic record content' FROM n;`, count)
			if err != nil {
				b.Fatal(err)
			}
			cfg := Config{Tables: map[string][]string{"records": {"id", "owner", "amount", "body"}}, Timeout: time.Second, MaxBytes: 4096, MaxRows: 100}
			s, err := NewSnapshotSource(path, cfg, SnapshotConfig{Filters: map[string]string{"records": "owner=:requester"}})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, _, err := s.Query(context.Background(), "alice", "SELECT sum(amount) FROM records")
				if err != nil || r.Rows[0][0] != int64(count*(count+1)/2) {
					b.Fatalf("%#v %v", r, err)
				}
			}
		})
	}
}
