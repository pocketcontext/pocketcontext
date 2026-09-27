# PocketContext

PocketContext lets coding agents query application data with SQL. All writes use PocketBase's standard REST API.

The server embeds PocketBase v0.40.4. By default, a separate read-only SQLite connection executes agent queries against the same database. Optional [filtered snapshots](docs/filtered-snapshots.md) copy only rows permitted by application-defined filters into an isolated database for each query. No replication service is required. PocketContext has no application frontend. PocketBase's built-in administration dashboard remains available for server administration.

## Build

You need Go 1.27 or later and a C compiler. The SQL reader uses `github.com/mattn/go-sqlite3` for its SQLite authorizer. PocketBase's write connections use the same SQLite library to share its process-local locking state. Agent reads use separate connections opened read-only. Build and test with the `sqlite_math_functions` tag, as the Makefile does; without it SQLite lacks math functions such as `sqrt` and `ceil`, so `go test ./...` fails.

```sh
make test
make build
```

## Run an application

Applications own `pb_migrations`, optional `pb_hooks`, and `pocketcontext.json`. DealContext is the first application, at `https://github.com/pocketcontext/dealcontext`.

From the application directory:

```sh
../pocketcontext/bin/pocketcontext migrate up --dir ./pb_data
../pocketcontext/bin/pocketcontext serve --dir ./pb_data --http 127.0.0.1:8090
```

Migrations and hooks default to `./pb_migrations` and `./pb_hooks`. Override them with `--migrationsDir` and `--hooksDir`. Set `--contextConfig` to use a configuration outside the working directory. `migrate` and `superuser` commands do not require SQL configuration. Keep the database on local disk and bind to localhost unless you configure network access and TLS.

Example configuration:

```json
{
  "authCollection": "agents",
  "tables": {
    "documents": ["id", "title", "body", "created"],
    "decisions": []
  },
  "timeoutMs": 2000,
  "maxRows": 500,
  "maxBytes": 1048576
}
```

An empty column list exposes the collection's current nonhidden columns. The server rejects that shortcut when a collection contains hidden fields. Explicit column lists provide a more stable contract. Configuration can expose only non-system base collections. Restart the server after schema or permission changes. Schema discovery describes the columns authorized at startup.

Create the application's auth collection in a migration. Provision accounts through an administrator, then authenticate through PocketBase's normal `auth-with-password` endpoint. SQL routes accept tokens only from the configured collection, including no special bypass for superuser tokens.

## Read with SQL

```sh
curl -sS http://127.0.0.1:8090/api/context/schema \
  -H "Authorization: $POCKETCONTEXT_TOKEN"

curl -sS http://127.0.0.1:8090/api/context/query \
  -H "Authorization: $POCKETCONTEXT_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT id, title FROM documents ORDER BY created DESC LIMIT 20"}'
```

JSON responses contain `columns`, positional `rows`, and `truncated`. Request `"format":"csv"` for CSV. Both formats return `X-Context-Truncated`. A truncated response is incomplete. Narrow the query or request another page using a stable ordering. CSV represents NULL as an empty field; use JSON when that distinction matters.

The reader supports joins, aggregates, CTEs, and common SQLite functions. It accepts one statement, with an optional trailing semicolon. It denies writes, schema changes, transactions, PRAGMAs, attachments, metadata reads, and functions outside its allowlist. It also rejects, before running it, any statement that SQLite does not report as read-only, such as `VACUUM` and `VACUUM INTO`. Views and virtual tables are not currently configurable; query the allowed base tables with joins or CTEs. SQLite can omit database identity in authorization callbacks for `COUNT(*)` over a CTE, so the reader may reject that form. Use `COUNT(cte.column)` when the column is non-null, or aggregate from the base table.

The default limits are two seconds, 500 rows, and 1 MiB of encoded results. The HTTP request limit is 64 KiB. The reader also limits SQLite scalar allocation sizes and sets a process-wide SQLite heap cap of 256 MiB, shared with PocketBase and snapshot construction. Queries that exceed a row or accumulated result limit return partial results with `truncated: true`. Oversized SQLite values and invalid or unauthorized SQL return 400. Expired query deadlines return 408. CSV or JSON encoding that exceeds the final response cap returns 413. A busy database returns 503 with `Retry-After`; retry the same query.

The reader uses at most four connections. Additional concurrent queries wait for a connection and return 408 if none becomes free before the deadline. `EXPLAIN` and `EXPLAIN QUERY PLAN` are permitted for allowed tables and reveal index names and page numbers, not data.

## Write through PocketBase

Use `/api/collections/{collection}/records` to create records and `/api/collections/{collection}/records/{id}` to update or delete them. PocketBase applies its collection rules, validation, and hooks. The SQL endpoint cannot perform these operations.

## Permission model

Without a `snapshot` configuration, every account in `authCollection` can query every configured column and row. Do not use that shared mode for accounts that must see different rows in the same configured table.

With [filtered snapshots](docs/filtered-snapshots.md), each query runs against a fresh database containing the rows allowed by application-owned filters for the authenticated requester. Every output table requires explicit columns and a filter. Policy tables can participate in filtering without being exposed to agent queries. Neither mode inherits PocketBase's per-record API rules; applications must also protect standard record APIs and policy-changing writes.

Keep config and schema changes under administrative control. SQL authorization captures the configured columns at startup; restart after changing hidden fields or collection definitions. Renaming a collection and creating a new one with the old name keeps the old column allowlist for that name until restart. Error messages distinguish unknown columns from unauthorized ones, so agents can discover the names of hidden fields and unexposed tables, but not their contents. Row limits and timeouts constrain individual queries, not aggregate traffic. Put traffic controls in front of the service when exposing it to a network. Every query is written to the PocketBase log with the agent record id, duration, row count, and SQL text; failed queries log at warning level.

`pb_data` contains application data and credentials and must stay outside Git. PocketBase remains pre-1.0, so review upstream migration notes before upgrading. Back up the application before upgrades and test restores.

## Optional request traces

Enable tracing per application in `pocketcontext.json`:

```json
"tracing": {
  "enabled": true,
  "service": "peoplecontext",
  "path": "./pb_data/traces.jsonl",
  "captureSql": false,
  "maxBytes": 16777216
}
```

Set `"delivery": "buffer"` instead of supplying `path` for client-controlled delivery. In buffer mode, only requests carrying `X-Context-Trace: 1` are traced. Clients receive `X-Context-Request-Id`, then fetch the completed trace with `GET /api/context/traces/{request_id}` using the same source application's bearer token. The response is the version-1 JSON object described below, with `Cache-Control: no-store`. The server writes no trace files and holds no ObserveContext credentials; the client uploads telemetry separately.

Retrieval requires the configured ordinary auth collection and the trace's original account and token key. Superusers cannot bypass ownership. Token revocation also makes old traces unavailable to fresh sessions. Missing, expired, malformed and other-account identifiers return 404; authentication failures retain the application's ordinary denial behavior. Client-chosen correlation IDs are never lookup keys. Retrieval is repeatable until expiry and is never itself traced. A response can reach the client before trace finalization: clients may retry 404 briefly, but telemetry failures must not retry the underlying business operation.

Buffer records expire after 120 seconds and are removed on subsequent buffer access. Oldest records are evicted at 1,024 total records, 64 records per account, `maxBytes` total JSON bytes, or 1 MiB JSON bytes per account. Individual records above 64 KiB are dropped. Process restart loses all records. In buffer mode SQL text requires both server `captureSql: true` and client `X-Context-Capture-Sql: 1`; omitted headers never capture SQL. `delivery` defaults to `file`, preserving existing spool behavior and server-controlled SQL capture.

For file delivery, the parent directory must exist and be private. Trace files use mode 0600; existing public files and symlinks are rejected. Tracing is disabled by default. It records completed authenticated application API requests, including failures, and omits health requests, guests and superusers. Use a separate spool per process; keep tracing disabled on a trace-ingestion service to avoid recursive collection.

Clients may send `X-Context-Correlation-Id` (1–128 ASCII letters, digits, underscores or hyphens). `X-Context-Request-Id` returns an independently generated server request identifier. Identity comes from authentication, never client telemetry. Traces include matched route patterns, method, status, authenticated user ID, start time, elapsed milliseconds, row count, truncation and timing spans. They never contain authorization headers, query-string parameters, REST bodies or query results. Optional SQL capture is capped at 16 KiB and may include sensitive literals; enable it only when the trace audience may read them.

SQL spans measure prepare (including connection-pool waiting), execute, and scan (including SQLite stepping and result accumulation). Filtered queries also measure snapshot capacity waiting, export/build, and reader initialization. Response encoding is measured separately. Authentication is measured around the token-loading middleware. Other REST and HTTP work currently has total request timing, not individual application-hook or transaction spans. Completed traces do not report in-flight progress or network/client elapsed time. Durations are server wall time; phase totals need not exhaust the request duration.

The format is one JSON object per line with `version: 1`, `request_id`, optional `correlation_id`, `service`, `method`, `route`, `started_at`, `duration_ms`, `status`, `user_id`, optional `sql`, `rows`, `truncated`, and `spans`. Each span has `name`, `offset_ms`, and `duration_ms`. An external collector can import these records into a separate application such as ObserveContext through REST; the server has no dependency on that application.

The writer queues at most 256 records and retains the active file plus one `.1` rotation, each up to `maxBytes` (1 MiB–1 GiB). This is bounded diagnostic telemetry, not a durable audit log: queue overflow, file failures, process crashes or an importer falling behind rotation can lose records. Application requests never wait for spool I/O. Drop counts are reported in the PocketBase log on subsequent requests; graceful shutdown drains queued records. Rotate/import within the configured capacity and exclude trace files from source control.
