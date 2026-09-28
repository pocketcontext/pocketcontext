# Filtered snapshots

Filtered snapshots let authenticated accounts query different subsets of application data with SQL. The application defines the rows and columns each requester may read. PocketContext copies those rows into a fresh private SQLite database, then runs the agent's SQL against that database using its existing read restrictions.

This is an optional mode. Existing configurations without `snapshot` retain shared-workspace access.

## Configuration

For example, an application has `documents` and a server-controlled `document_readers` collection that grants a reader access to a document:

```json
{
  "authCollection": "agents",
  "tables": {
    "documents": ["id", "title", "body"]
  },
  "timeoutMs": 2000,
  "maxRows": 500,
  "maxBytes": 1048576,
  "snapshot": {
    "filters": {
      "documents": "EXISTS (SELECT 1 FROM document_readers r WHERE r.document = documents.id AND r.reader = :requester)"
    },
    "policyTables": {
      "document_readers": ["document", "reader"]
    },
    "timeoutMs": 5000,
    "maxRows": 100000,
    "maxBytes": 16777216,
    "maxConcurrent": 4
  }
}
```

The application creates these collections and owns their write rules. The `reader` value is an account id in `authCollection`. PocketContext binds `:requester` to the authenticated account's id; neither the query body nor SQL can change that binding. The identity is a bound parameter, not interpolated text.

Each `tables` entry needs an explicit, nonempty column list and a nonempty `filters` entry. A filter is a trusted SQL predicate, not a complete query. Use `"1"` to intentionally expose all rows of a table. Omitted filters are configuration errors. SQL references in filters are restricted to configured output columns and `policyTables` columns. Additional bind parameters are not supported.

`policyTables` entries require explicit columns and must not overlap output tables. They are available to export filters only. They are absent from the snapshot and schema response. Neither output nor policy configuration may include auth collections, system collections, hidden fields, views, or virtual tables. Configuration is administrator-controlled and takes effect at startup; restart after changing schema or configuration.

For a management hierarchy, an application can maintain grants for each direct and indirect manager or express that relationship in its filters. HR membership and access to one's own records are separate policy decisions. PocketContext does not infer them or maintain the hierarchy. A grant table must be updated consistently with the relationships it represents.

## Request lifecycle

1. Authenticate through the normal PocketBase token mechanism.
2. Acquire a snapshot concurrency slot and begin a read transaction on the source database.
3. Evaluate the configured filters with the requester id and copy all approved rows into a fresh database. All exports share the source transaction.
4. Finish the destination writes and open the destination with the restricted read-only SQL engine. The source database is never attached to this connection.
5. Execute the agent query, close the query connections, and remove the temporary database.

All application reads still enter through the authenticated context endpoints. Exporting is an internal server operation, not an additional database connection supplied to the agent. Record writes continue through PocketBase's standard REST API.

The query and schema routes remain `/api/context/query` and `/api/context/schema`. Queries keep the existing JSON or CSV result format. Successful snapshot queries include `X-Context-Scope: authorized-snapshot` and `X-Context-Snapshot-At`. Schema discovery describes the configured output columns even when no rows are visible. It does not reveal the export filters or policy tables.

An aggregate describes the visible population. `SELECT count(*) FROM documents` counts permitted documents, not every source document. SQL can still join, aggregate, and use CTEs across the exported tables. Relations to absent rows will not resolve in those joins.

Snapshots copy selected column names, declared types, and stored values. They do not copy source indexes, constraints, triggers, or custom collations. Queries over unusual source collations may therefore behave differently; ordinary PocketBase field tables use the supported default semantics. Source rowids are not an exported identity: use the configured record `id` column.

## Limits and failures

Snapshot limits are separate from the existing query result limits:

| Setting | Default | Meaning |
| --- | --- | --- |
| `snapshot.timeoutMs` | 5000 | Export deadline, including waiting for a concurrency slot |
| `snapshot.maxRows` | 100000 | Total exported rows across all tables |
| `snapshot.maxBytes` | 16777216 | Sum of JSON-encoded exported row sizes; SQLite storage also has a bounded page allocation |
| `snapshot.maxConcurrent` | 4 | Concurrent snapshot requests, including query execution and cleanup |

An export that exceeds its limits fails without executing the agent query. It never returns a partial snapshot that could produce misleading aggregates. Export limits return HTTP 413, deadlines return 408, and a busy source returns 503. Other export failures return a generic server error without source SQL details. Successful exports remain subject to the normal query timeout, row limit, and encoded response limit; query results can still report `truncated: true`.

Every query builds its full configured visible dataset, even if the submitted SQL selects one table or uses `LIMIT 1`. Measure export time and temporary storage for the application's largest permitted dataset and expected concurrency before choosing limits.

The permitted ranges are 1–30000 milliseconds, 1–1000000 rows, 1024–67108864 bytes, and 1–16 concurrent requests. Omitted or zero limits use the defaults. Source reads use a pool of at most four connections; additional requests wait within the export deadline. The byte budget does not represent total process memory: Go allocations, SQLite caches, and concurrent requests add overhead.

A synthetic end-to-end benchmark is available:

```sh
CGO_ENABLED=1 go test -tags sqlite_math_functions,sqlite_percentile,sqlite_fts5 ./internal/sqlread \
  -run '^$' -bench '^BenchmarkSnapshot$' -benchtime=3x
```

## Authorization and freshness

There is no snapshot cache. The next request reads current policy and application data in a new source transaction. A request already in progress can finish using the policy and data it captured. The snapshot timestamp describes the capture's start, not a guarantee of revocation at response delivery. Previously delivered results cannot be revoked.

Filters are the authorization policy for SQL. PocketBase record API rules are not evaluated during export. Protect list/view routes, files, custom endpoints, and writes to roles or relationships separately. Agents must use individual account tokens; a shared service token gives every caller that account's visibility. An incorrect filter can authorize too much data, so application tests should cover both permitted and denied relationships.

Temporary databases contain private data. They use private directories and files, with no source database attached, and are removed after success, failure, or cancellation and during normal shutdown. Abrupt process or machine failure can leave temporary files for operator or operating-system cleanup. Deleting a file does not promise secure erasure. Keep the temporary storage outside source control and application backups.

The authorizer, read-only connections, function allowlist, and resource limits remain necessary: snapshots restrict which data is present; they do not grant arbitrary filesystem access or SQL writes. This boundary protects against untrusted SQL, not a compromised server process or an administrator with access to source data. Query audit logs retain the requester id and submitted SQL, so their access policy must account for literals users include in queries.
