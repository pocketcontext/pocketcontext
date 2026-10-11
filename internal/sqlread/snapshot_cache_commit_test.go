package sqlread

import (
	"context"
	"os"
	"sync"
	"testing"
)

func TestSnapshotCacheCommitDuringBuildIsNeverRetained(t *testing.T) {
	db, path, cfg, sc := snapshotFixture(t)
	sc.Filters["pay"] = "length(:requester) > 0 AND (" + sc.Filters["pay"] + ")"
	s := newTestSnapshot(t, path, cfg, sc)
	s.source.db.SetMaxOpenConns(1)
	conn, err := s.source.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var changed sync.Once
	var writeErr error
	err = conn.Raw(func(raw any) error {
		return raw.(roConn).c.RegisterFunc("length", func(value string) (int, error) {
			changed.Do(func() {
				// This runs after acquisition of the source read transaction,
				// with no sleeps or reliance on the relative speed of goroutines.
				_, writeErr = db.Exec("DELETE FROM managers WHERE requester='alice'")
			})
			return len(value), writeErr
		}, false)
	})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Query(context.Background(), "alice", "SELECT sum(salary) FROM pay")
	if err != nil || writeErr != nil || len(r.Rows) != 1 || r.Rows[0][0] != int64(300) {
		t.Fatalf("initiating transaction lost consistent view: %#v %v %v", r, err, writeErr)
	}
	if len(s.cache) != 0 {
		t.Fatal("precommit snapshot retained after revocation")
	}
	entries, err := os.ReadDir(s.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("unretained snapshot leaked: %v %v", entries, err)
	}
	r, _, err = s.Query(context.Background(), "alice", "SELECT id FROM pay")
	if err != nil || len(r.Rows) != 0 {
		t.Fatalf("next request reused revoked access: %#v %v", r, err)
	}
	if len(s.cache) != 1 {
		t.Fatal("unchanged current snapshot was not cached")
	}
}
