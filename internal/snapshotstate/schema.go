// Package snapshotstate defines server-owned transactional change tracking.
// The query engine only reads this metadata; installation uses the ordinary
// application writer and respects its maintenance fence.
package snapshotstate

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

const Table = "_context_snapshot_versions"

type Definition struct {
	Name string
	Kind string
	SQL  string
}

func Quote(value string) string   { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
func literal(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

// Definitions never copy application SQL. Every trigger rotates a table token
// in the same transaction as its row change, including changes by other SQLite
// writers, cascades and application triggers. Rollbacks roll back tokens too.
func Definitions(tables []string) []Definition {
	result := []Definition{{Table, "table", `CREATE TABLE "` + Table + `" (table_name TEXT PRIMARY KEY NOT NULL, version TEXT NOT NULL CHECK(length(version)=32))`}}
	names := append([]string(nil), tables...)
	sort.Strings(names)
	for _, table := range names {
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			digest := sha256.Sum256([]byte(table + "/" + operation))
			name := fmt.Sprintf("_context_snapshot_%x", digest[:16])
			ddl := "CREATE TRIGGER " + Quote(name) + " AFTER " + operation + " ON " + Quote(table) +
				" BEGIN INSERT INTO " + Quote(Table) + " (table_name,version) VALUES (" + literal(table) +
				",lower(hex(randomblob(16)))) ON CONFLICT(table_name) DO UPDATE SET version=excluded.version; END"
			result = append(result, Definition{name, "trigger", ddl})
		}
	}
	return result
}

func SeedSQL(table string) string {
	return "INSERT INTO " + Quote(Table) + " (table_name,version) VALUES (" + literal(table) +
		",lower(hex(randomblob(16)))) ON CONFLICT(table_name) DO NOTHING"
}
