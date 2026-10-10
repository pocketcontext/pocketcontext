package sqlread

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
)

func readIDs(t *testing.T, s *SnapshotSource, r *FilteredRead, requester string, ids []string) ([]string, error) {
	t.Helper()
	var got []string
	err := s.ReadFiltered(context.Background(), requester, func(tx *FilteredTx) error {
		return tx.Read(r, ids, func(values []any) error {
			got = append(got, values[0].(string))
			return nil
		})
	})
	sort.Strings(got)
	return got, err
}

func TestFilteredReadAuthorizationAndRevocation(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	all, err := s.PrepareFilteredRead(FilteredReadSpec{Table: "pay", Columns: []string{"id", "salary"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		user string
		ids  []string
		want string
	}{
		{"alice", nil, "a,b"}, {"bob", nil, "c"}, {"nobody", nil, ""},
		{"alice", []string{"a", "c"}, "a"}, {"alice", []string{}, ""}, {"bob", []string{"a", "b"}, ""},
		{"alice' OR 1=1 --", nil, ""},
	} {
		got, err := readIDs(t, s, all, tc.user, tc.ids)
		if err != nil || strings.Join(got, ",") != tc.want {
			t.Fatalf("%s %v: %v %v", tc.user, tc.ids, got, err)
		}
	}
	narrowed, err := s.PrepareFilteredRead(FilteredReadSpec{Table: "pay", Columns: []string{"id"}, Where: "salary > 150 OR :requester = 'bob'"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readIDs(t, s, narrowed, "alice", nil); err != nil || strings.Join(got, ",") != "b" {
		t.Fatalf("where: %v %v", got, err)
	}
	if _, err := db.Exec("DELETE FROM managers WHERE requester='alice' AND employee='b'"); err != nil {
		t.Fatal(err)
	}
	if got, err := readIDs(t, s, all, "alice", nil); err != nil || strings.Join(got, ",") != "a" {
		t.Fatalf("revocation: %v %v", got, err)
	}
	if _, err := readIDs(t, s, all, "", nil); !errors.Is(err, ErrSnapshotBuild) {
		t.Fatalf("empty requester: %v", err)
	}
	// Caller errors pass through unchanged.
	sentinel := errors.New("caller")
	if err := s.ReadFiltered(context.Background(), "alice", func(*FilteredTx) error { return sentinel }); err != sentinel {
		t.Fatal(err)
	}
	// The ID-list permission never leaks into ordinary snapshot queries.
	for i := 0; i < 8; i++ {
		if _, _, err := s.Query(context.Background(), "alice", "SELECT id FROM pay"); err != nil {
			t.Fatal(err)
		}
	}
	cleanSnapshot(t, s)
}

func TestFilteredReadRejectsUnsafeSpecs(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	for _, spec := range []FilteredReadSpec{
		{Table: "managers", Columns: []string{"requester"}},
		{Table: "pay", Columns: []string{"secret"}},
		{Table: "pay", Columns: nil},
		{Table: "missing", Columns: []string{"id"}},
		{Table: "pay", Columns: []string{"id"}, Where: "  "},
		{Table: "pay", Columns: []string{"id"}, Where: "salary=:other"},
		{Table: "pay", Columns: []string{"id"}, Where: "salary=?"},
		{Table: "pay", Columns: []string{"id"}, Where: "id IN (SELECT value FROM json_each(:ids))"},
		{Table: "pay", Columns: []string{"id"}, Where: "1); DELETE FROM pay; --"},
		{Table: "pay", Columns: []string{"id"}, Where: "secret='x'"},
		{Table: "pay", Columns: []string{"id"}, Where: "EXISTS(SELECT 1 FROM sqlite_schema)"},
		{Table: "pay", Columns: []string{"id"}, Where: "load_extension('x')"},
	} {
		if _, err := s.PrepareFilteredRead(spec); err == nil {
			t.Fatalf("accepted %+v", spec)
		}
	}
	c.Tables = map[string][]string{"pay": {"salary"}}
	sc.Filters = map[string]string{"pay": "salary > 0"}
	noID := newTestSnapshot(t, path, c, sc)
	if _, err := noID.PrepareFilteredRead(FilteredReadSpec{Table: "pay", Columns: []string{"salary"}}); err == nil {
		t.Fatal("accepted table without configured id")
	}
}

func TestFilteredReadSharesSnapshotLimits(t *testing.T) {
	_, path, c, sc := snapshotFixture(t)
	sc.MaxRows = 1
	s := newTestSnapshot(t, path, c, sc)
	r, err := s.PrepareFilteredRead(FilteredReadSpec{Table: "pay", Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readIDs(t, s, r, "alice", nil); !errors.Is(err, ErrSnapshotLimit) {
		t.Fatalf("row limit: %v", err)
	}
	if got, err := readIDs(t, s, r, "bob", nil); err != nil || len(got) != 1 {
		t.Fatalf("after limit: %v %v", got, err)
	}
	// Limits apply across all reads of one transaction.
	err = s.ReadFiltered(context.Background(), "bob", func(tx *FilteredTx) error {
		for i := 0; i < 2; i++ {
			if err := tx.Read(r, nil, func([]any) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, ErrSnapshotLimit) {
		t.Fatalf("transaction limit: %v", err)
	}
	other := newTestSnapshot(t, path, c, sc)
	if err := other.ReadFiltered(context.Background(), "bob", func(tx *FilteredTx) error { return tx.Read(r, nil, func([]any) error { return nil }) }); err == nil {
		t.Fatal("accepted a read prepared for another source")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.ReadFiltered(ctx, "bob", func(*FilteredTx) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestFilteredReadRejectsShadowedJSONEach(t *testing.T) {
	db, path, c, sc := snapshotFixture(t)
	s := newTestSnapshot(t, path, c, sc)
	r, err := s.PrepareFilteredRead(FilteredReadSpec{Table: "pay", Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE json_each(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := readIDs(t, s, r, "alice", []string{"a"}); !errors.Is(err, ErrSnapshotBuild) {
		t.Fatalf("shadowed json_each: %v", err)
	}
}
