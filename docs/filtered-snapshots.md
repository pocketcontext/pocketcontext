# Filtered snapshots

Filtered snapshots let authenticated accounts query different subsets of application data with SQL. The application defines the rows and columns each requester may read. PocketContext copies those rows into a private SQLite database, then runs the agent's SQL against that database using its existing read restrictions. It reuses the authenticated requester's copy while the source database remains unchanged.

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
    "maxConcurrent": 4,
    "maxConcurrentPerRequester": 1
  }
}
```

The application creates these collections and owns their write rules. The `reader` value is an account id in `authCollection`. PocketContext binds `:requester` to the authenticated account's id; neither the query body nor SQL can change that binding. The identity is a bound parameter, not interpolated text.

Each `tables` entry needs an explicit, nonempty column list and a nonempty `filters` entry. A filter is a trusted SQL predicate, not a complete query. Use `"1"` to intentionally expose all rows of a table. Omitted filters are configuration errors. SQL references in filters are restricted to configured output columns and `policyTables` columns. Additional bind parameters are not supported.

`policyTables` entries require explicit columns and must not overlap output tables. They are available to export filters only. They are absent from the snapshot and schema response. Neither output nor policy configuration may include auth collections, system collections, hidden fields, views, or virtual tables. Configuration is administrator-controlled and takes effect at startup; restart after changing schema or configuration.

For a management hierarchy, an application can maintain grants for each direct and indirect manager or express that relationship in its filters. HR membership and access to one's own records are separate policy decisions. PocketContext does not infer them or maintain the hierarchy. A grant table must be updated consistently with the relationships it represents.

## Request lifecycle

1. Authenticate through the normal PocketBase token mechanism.
2. Check optional per-account admission, acquire a snapshot concurrency slot, and check the source database's commit version.
3. Reuse the requester's immutable snapshot if its version still matches. Otherwise, evaluate all configured filters and copy approved rows from one source read transaction into a private database. Concurrent requests for that requester coordinate one build.
4. Execute the agent query through the restricted read-only snapshot engine. The source database is never attached to this connection.
5. Release the reader. Retain an unchanged snapshot for reuse; remove invalidated or evicted copies after their active readers finish.

A dedicated physical read-only SQLite connection checks `PRAGMA data_version`
before reuse and after construction. Any committed source-database change
invalidates retained copies, including changes to unexported policy or auth
tables. Values are never compared across different monitor connections. A failed
monitor check fails the request without using cached data. A commit during
construction permits that request to finish on its consistent source transaction,
but its copy is not retained. Requests after the commit must observe the new
state. Rollbacks do not invalidate copies.

This is transparent caching, not a stale-read mode: there are no client snapshot
IDs or refresh endpoints. Read-after-write and conflict-recovery lookups retain
their existing semantics. Every request is authenticated independently. SQL and
configuration restrictions remain unchanged; configuration/schema changes still
require a restart.

Filters containing date/time functions bypass reuse because their results can
change without a database write. Detection uses SQLite's authorizer, including
quoted function names; even constant-argument uses conservatively bypass caching.

At most `snapshot.maxConcurrent` requester snapshots are retained, evicting the
least recently used. Existing per-snapshot row, encoded-byte and physical-page
limits remain enforced. Active readers can temporarily retain evicted files;
the existing concurrency limit bounds those additional copies. Each retained
engine has its own bounded read-connection pool. Graceful shutdown closes the
engines and removes temporary storage. A process crash may leave private temp
files, as with uncached construction; cached copies are never recovered or reused
after restart.

All application reads still enter through the authenticated context endpoints. Exporting is an internal server operation, not an additional database connection supplied to the agent. Record writes continue through PocketBase's standard REST API.

The query and schema routes remain `/api/context/query` and `/api/context/schema`. Queries keep the existing JSON or CSV result format. Successful snapshot queries include `X-Context-Scope: authorized-snapshot` and `X-Context-Snapshot-At`. The latter is the copy's original source-transaction time; it remains unchanged on cache hits, whose source version is rechecked for every request. Schema discovery describes the configured output columns even when no rows are visible. It does not reveal the export filters or policy tables.

An aggregate describes the visible population. `SELECT count(*) FROM documents` counts permitted documents, not every source document. SQL can still join, aggregate, and use CTEs across the exported tables. Relations to absent rows will not resolve in those joins.

Snapshots copy selected column names, declared types, and stored values. At startup, the server also inspects ordinary source indexes and primary keys. It builds corresponding nonunique snapshot indexes only when every key is an exported column, preserving key order, direction and built-in `BINARY`, `NOCASE` or `RTRIM` collation. Partial indexes, expression indexes, unexported keys and custom collations are skipped. Source index SQL and names are never replayed; snapshot indexes use internal names. Index creation shares the existing export deadline and physical page budget and must finish before the snapshot can serve queries. The `snapshot.indexes` trace span measures that construction work.

Snapshots do not copy source constraints, triggers, column collations or custom collations. Queries over unusual source collations may therefore behave differently; ordinary PocketBase field tables use the supported default semantics. Source rowids are not an exported identity: use the configured record `id` column. Restart after source schema or index changes.

## Limits and failures

Snapshot limits are separate from the existing query result limits:

| Setting | Default | Meaning |
| --- | --- | --- |
| `snapshot.timeoutMs` | 5000 | Export deadline, including waiting for a concurrency slot |
| `snapshot.maxRows` | 100000 | Total exported rows across all tables |
| `snapshot.maxBytes` | 16777216 | Sum of JSON-encoded exported row sizes; SQLite storage also has a bounded page allocation |
| `snapshot.maxConcurrent` | 4 | Concurrent snapshot requests, including query execution and cleanup |
| `snapshot.maxConcurrentPerRequester` | 0 (disabled) | Admitted SQL/filtered-read requests per authenticated account, including capacity waiting |

When `maxConcurrentPerRequester` is positive (at most `maxConcurrent`), admission is checked atomically before waiting for a global slot. Excess same-account requests return HTTP 429 with `Retry-After: 1`; retry after waiting. Admission remains held through query execution and cleanup and is released on failures and cancellation. Tracking contains only live admitted requests, with no idle account entries. Set the account limit below global concurrency to stop one account monopolizing all slots. Multiple accounts can still exhaust global capacity. Omitted or zero values preserve the existing behavior. The limit applies to SQL snapshots and trusted filtered reads used by private search; it does not limit unrelated application endpoints or search execution after its filtered read completes.

An export that exceeds its limits fails without executing the agent query. It never returns a partial snapshot that could produce misleading aggregates. Export limits return HTTP 413, deadlines return 408, and a busy source returns 503. Other export failures return a generic server error without source SQL details. Successful exports remain subject to the normal query timeout, row limit, and encoded response limit; query results can still report `truncated: true`.

Every cold cache miss builds its full configured visible dataset, even if the submitted SQL selects one table or uses `LIMIT 1`. Optional tracked refreshes below can reuse unchanged tables. Measure cold builds, warm reads, write-heavy workloads and temporary storage for the application's largest permitted dataset and expected concurrency. Frequent source commits can reduce the cache benefit.

The permitted ranges are 1–30000 milliseconds, 1–1000000 rows, 1024–67108864 bytes, and 1–16 concurrent requests. Omitted or zero limits use the defaults. Source reads use a pool of at most four connections; additional requests wait within the export deadline. The byte budget does not represent total process memory: Go allocations, SQLite caches, and concurrent requests add overhead.

A synthetic end-to-end benchmark is available:

```sh
CGO_ENABLED=1 go test -tags sqlite_math_functions,sqlite_percentile,sqlite_fts5 ./internal/sqlread \
  -run '^$' -bench '^BenchmarkSnapshot$' -benchtime=3x
```

## Filtered reads and search

`SnapshotSource.ReadFiltered` gives trusted server features one source read transaction for the authenticated requester. Reads are declared at startup with `PrepareFilteredRead`: an output table, configured output columns and an optional trusted predicate that follows filter rules. Each read applies the table's snapshot filter, optionally restricted to a list of record IDs bound through the built-in `json_each`. The ID-list permission exists only on the connection held for that transaction. Filtered reads share the snapshot concurrency slots, timeout and `maxRows`/`maxBytes` accounting, and fail without partial results in the same way as exports. A table or view named `json_each` makes them fail.

Search with filtered snapshots uses these reads to build [private indexes](search.md#private-search-for-filtered-snapshots). Shared search indexes are rejected with `snapshot`.

## Optional transactional change tracking

Set `snapshot.trackChanges` to `true` to track changes to configured output and
policy tables and the authentication collection. The default remains `false`,
using global `data_version` invalidation without changing the source schema.

On first enabled startup, the ordinary application writer installs the internal
`_context_snapshot_versions` table and three server-owned triggers per tracked
table. Each insert, update or delete rotates that table's token inside the same
transaction. This covers other SQLite writers, cascades and application triggers;
rollback also rolls back the token. It adds a small metadata write per changed
row. The metadata is never exported or accessible through agent SQL.

Installation needs a writable maintenance state. A frozen startup can verify an
already installed configuration without writing; it cannot install new tracking.
Unexpected existing definitions refuse startup. Enabling this option therefore
requires an intentional application release and synthetic restore/deployment
rehearsal, not merely raising a query limit. Database backups retain the table
and triggers. Disabling tracking stops its use but leaves installed metadata and
triggers intact; removing them is explicit operator maintenance.

The monitor checks `data_version` on every request and reads committed table
tokens when it changes. Unrelated table writes can reuse existing snapshots.
SQLite's prepare-time authorizer records every table read by each export filter,
including subqueries, recursive CTEs and ordinary exported tables used as policy.
A changed dependency refreshes its dependent exports. An authentication-table
change refreshes every export. Date/time policies still bypass reuse entirely.

Refresh copies the previous private, immutable snapshot into a new private file,
then replaces affected tables and indexes from one source read transaction.
Unchanged rows count toward the same total row/byte limits. Active readers keep
their original file. If a relevant commit overlaps the refresh, the tentative
copy is discarded and retried as a full export within the existing deadline;
mixed old/new visibility is never returned. A full export can finish on its
consistent source transaction even under continuous writes. There is no background stale SQL response, and
read-after-write semantics remain intact. Tracking is at table granularity:
it does not distinguish different spaces or append only newly inserted rows.

Schema changes after startup fail closed until restart validates tracking and
export definitions again. Missing tokens, changed triggers or a failed monitor
never permit cached access. Initial definition validation and schema-version
capture share one read transaction. Snapshot sharing remains per requester;
matching membership sets alone do not establish identical visibility.

## Authorization and freshness

SQL snapshots are reused only after a fresh source-version check. Private search
independently reads current authorized source rows before looking up an FTS index
by content fingerprint; it does not construct the cached SQL snapshot. A request
already in progress can finish using its captured policy and data. The snapshot
timestamp describes the capture's start, not a guarantee of revocation at response
delivery. Previously delivered results cannot be revoked.

Filters are the authorization policy for SQL. PocketBase record API rules are not evaluated during export. Protect list/view routes, files, custom endpoints, and writes to roles or relationships separately. Agents must use individual account tokens; a shared service token gives every caller that account's visibility. An incorrect filter can authorize too much data, so application tests should cover both permitted and denied relationships.

Temporary databases contain private data. They use private directories and files, with no source database attached, and are removed after success, failure, or cancellation and during normal shutdown. Abrupt process or machine failure can leave temporary files for operator or operating-system cleanup. Deleting a file does not promise secure erasure. Keep the temporary storage outside source control and application backups.

The authorizer, read-only connections, function allowlist, and resource limits remain necessary: snapshots restrict which data is present; they do not grant arbitrary filesystem access or SQL writes. This boundary protects against untrusted SQL, not a compromised server process or an administrator with access to source data. Query audit logs retain the requester id and submitted SQL, so their access policy must account for literals users include in queries.

## Snapshot reuse benchmark

`BenchmarkSnapshotWikiHistory` uses synthetic 350-page knowledge with 3,500
revisions and 7,000 passages (approximately 27 MiB of text). On the development
ARM64 host, three fixed iterations repeated three times gave:

| Workload | Before caching (`2fb371c`) | Cached |
| --- | --- | --- |
| Cold construction and query | 216 ms | 202–210 ms |
| Repeated query, unchanged source | 217 ms | 4.20–4.26 ms |
| Four concurrent warm queries, batch wall time | 267 ms | 4.54–4.60 ms |
| Query after a source commit | 220 ms | 204–214 ms |

Warm Go allocations fell from approximately 69 MB to 8 KB per query. These are
local synthetic measurements, not network latency, browser load times or a
production service-level promise. Cold and write-invalidated reads still copy the
visible corpus. Query complexity and source write frequency remain significant.

```sh
CGO_ENABLED=1 go test -tags sqlite_math_functions,sqlite_percentile,sqlite_fts5 ./internal/sqlread \
  -run '^$' -bench '^BenchmarkSnapshotWikiHistory$' -benchtime=3x -count=3
```

## Snapshot index benchmark

`BenchmarkSnapshotWikiJoins` adds synthetic citations, links, sources and
publication manifests to the 350-page, 3,500-revision, 7,000-passage corpus. Its
six-query reader workload validates the manifest, lists published pages, reads
a page, joins its citation evidence and loads both link directions. Every query
checks its expected row count and rejects truncation. Both variants permit
SQLite automatic query indexes; the comparison changes only persistent snapshot
indexes.

On the development Linux ARM64 host, three fixed iterations repeated three times
gave the following local results:

| Workload | Without snapshot indexes | With 15 snapshot indexes |
| --- | --- | --- |
| Cold construction plus six reader queries | 355–358 ms | 294–302 ms |
| Six warm reader queries | 86–88 ms | 3.16–3.36 ms |
| Snapshot file size | 45,633,536 bytes | 46,637,056 bytes |

Index construction adds work to each cold build and consumes physical storage;
in this workload, faster joins offset that construction cost. The separate
`cold_lookup` case measures construction followed by one trivial lookup, without
the six-query join benefit. These synthetic results do not predict production
latency, and shared-host load can materially affect cold measurements. Measure
the application's own cold and warm workloads before changing build limits.

```sh
CGO_ENABLED=1 go test -tags sqlite_math_functions,sqlite_percentile,sqlite_fts5 ./internal/sqlread \
  -run '^$' -bench '^BenchmarkSnapshotWikiJoins$' -benchtime=3x -count=3
```
