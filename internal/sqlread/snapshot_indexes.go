package sqlread

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// Snapshot indexes contain only explicitly exported values. We reconstruct
// ordinary, nonunique indexes from inspected keys, never source SQL or names.
// Like exported columns, this schema is captured at startup; changing source
// schema requires a restart.
type snapshotIndex struct {
	table string
	keys  []string
}

func (i snapshotIndex) statement(number int) string {
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)", quote(fmt.Sprintf("_snapshot_index_%d", number)), quote(i.table), strings.Join(i.keys, ","))
}

func inspectSnapshotIndexes(ctx context.Context, conn *sql.Conn, tables map[string][]string) ([]snapshotIndex, error) {
	// The held connection remains read-only. Temporarily allow only this
	// internal metadata inspection through the normal authorizer; caller SQL
	// cannot run on the connection until it is returned to the pool.
	var trusted *bool
	if err := conn.Raw(func(raw any) error { trusted = raw.(roConn).trusted; *trusted = true; return nil }); err != nil {
		return nil, err
	}
	defer func() { *trusted = false }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	indexes := []snapshotIndex{}
	for _, table := range names {
		allowed := map[string]bool{}
		for _, column := range tables[table] {
			allowed[strings.ToLower(column)] = true
		}
		keys, err := snapshotIntegerPrimaryKey(ctx, tx, table, allowed)
		if err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		add := func(keys []string) {
			key := strings.Join(keys, ",")
			if len(keys) != 0 && !seen[key] {
				indexes = append(indexes, snapshotIndex{table: table, keys: keys})
				seen[key] = true
			}
		}
		add(keys)
		indexNames, err := snapshotSourceIndexNames(ctx, tx, table)
		if err != nil {
			return nil, err
		}
		for _, name := range indexNames {
			keys, err := snapshotSourceIndexKeys(ctx, tx, name, allowed)
			if err != nil {
				return nil, err
			}
			add(keys)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return indexes, nil
}

// INTEGER PRIMARY KEY normally aliases the source rowid and therefore has no
// separate index_list entry. The snapshot deliberately keeps ordinary columns
// and independent rowids, so give that exported key an ordinary index too.
func snapshotIntegerPrimaryKey(ctx context.Context, tx *sql.Tx, table string, allowed map[string]bool) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA table_info("+quote(table)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	count, name := 0, ""
	for rows.Next() {
		var cid, notnull, primary int
		var column, typ string
		var def any
		if err := rows.Scan(&cid, &column, &typ, &notnull, &def, &primary); err != nil {
			return nil, err
		}
		if primary != 0 {
			count++
			if allowed[strings.ToLower(column)] && strings.EqualFold(strings.TrimSpace(typ), "INTEGER") {
				name = column
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 1 && name != "" {
		return []string{quote(name) + ` COLLATE "BINARY" ASC`}, nil
	}
	return nil, nil
}

func snapshotSourceIndexNames(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA index_list("+quote(table)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var seq, unique, partial int
		var name, origin string
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		// A partial predicate may reference private columns or change meaning
		// on the projection, so it is never copied or evaluated here.
		if partial == 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, rows.Err()
}

func snapshotSourceIndexKeys(ctx context.Context, tx *sql.Tx, index string, allowed map[string]bool) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA index_xinfo("+quote(index)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []string{}
	usable := true
	for rows.Next() {
		var seq, cid, descending, key int
		var name, collation sql.NullString
		if err := rows.Scan(&seq, &cid, &name, &descending, &collation, &key); err != nil {
			return nil, err
		}
		if key == 0 {
			continue // Auxiliary rowid/primary-key storage is not an index key.
		}
		coll := strings.ToUpper(collation.String)
		if cid < 0 || !name.Valid || !allowed[strings.ToLower(name.String)] || (coll != "BINARY" && coll != "NOCASE" && coll != "RTRIM") {
			usable = false // Expressions, private columns and custom collations.
			continue
		}
		direction := " ASC"
		if descending != 0 {
			direction = " DESC"
		}
		keys = append(keys, quote(name.String)+" COLLATE "+quote(coll)+direction)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !usable {
		return nil, nil
	}
	return keys, nil
}
