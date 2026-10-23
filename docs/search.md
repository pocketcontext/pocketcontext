# Optional full-text search

PocketContext offers full-text search through a separate authenticated endpoint.
It is opt-in. Shared-workspace applications use application-maintained shared
indexes: all accounts in the configured auth collection can search every
configured index. Filtered-snapshot applications use
[private indexes](#private-search-for-filtered-snapshots), built from rows the
requester may read. Background snapshots store their indexes on disk; the default
synchronous mode uses an in-memory cache. PocketBase record API rules filter neither
mode. A `snapshot` configuration accepts only private indexes; shared-workspace
configurations accept only shared indexes.

## Configuration

An application first creates its index in a trusted migration and maintains it
through its ordinary write hooks. Then add `search` to `pocketcontext.json`:

```json
{
  "authCollection": "users",
  "tables": {"documents": ["id", "title", "body"]},
  "search": {
    "indexes": {
      "documents": {
        "table": "documents_search",
        "collection": "documents",
        "columns": ["title", "body"],
        "snippetColumn": "body",
        "weights": [5, 1]
      }
    }
  },
  "timeoutMs": 2000,
  "maxRows": 500,
  "maxBytes": 1048576
}
```

Each index maps one base collection. Its `record_id` value is the source record's
`id`; indexed column names match source fields. Source IDs and every indexed
field must already be authorized in `tables`. Auth collections, hidden fields,
system collections and sources omitted from SQL permissions are rejected.
The FTS table itself is not a PocketBase collection and is never added to
`tables`.

Only the documented canonical content-storing FTS5 schema is supported, with
`unicode61` tokenization, a `record_id UNINDEXED` column and the configured text
columns. External-content indexes, contentless indexes, custom tokenizers and
additional columns are rejected. Index weights are fixed by configuration, not
selected by a requester. Restart after configuration changes.

For the configuration above, execute this exact canonical DDL in the migration:

```sql
CREATE VIRTUAL TABLE "documents_search" USING fts5("record_id" UNINDEXED, "title", "body", tokenize='unicode61');
```

The stored DDL must match the canonical form, including identifier quoting and
internal spacing. The server's internal `searchread.CanonicalDDL` helper is the
reference for migration generators.
There may be at most 16 indexes and 16 indexed columns per index. Identifiers
start with an ASCII letter, contain only ASCII letters, digits or underscores,
and have at most 64 characters; the `sqlite_` prefix is reserved. Names are
configured with their exact case. Weights, when supplied, follow column order
and must be finite numbers from 0 to 100; omitted weights default to 1.

## Request and response

```sh
curl -sS http://127.0.0.1:8090/api/context/search \
  -H "Authorization: $POCKETCONTEXT_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"index":"documents","query":"deployment safety","limit":20}'
```

Unscoped indexes accept `index`, `query` and optional `limit`. The default limit
is 20, bounded by the application's `maxRows`; the maximum is the smaller of
100 and `maxRows`. Query text is limited to 4096 UTF-8 bytes and 16
whitespace-separated terms. Terms become quoted FTS phrases combined with AND.
Words such as `OR` are literal terms, not query operators; SQL, raw FTS syntax,
client-selected columns and ranking expressions are not accepted.

The response contains `index`, `hits`, `truncated` and `hasMore`. The latter two
are equal and indicate that at least one additional matching hit exists. Each hit has `id`, `score`
and `excerpt`. Lower BM25 scores rank first, with record ID as the tie-breaker.
Excerpts are plain text, including any untrusted markup stored by the application;
clients must escape them before HTML rendering. A truncated response is incomplete.
Responses use `Cache-Control: no-store` and `X-Context-Truncated`.

Authenticated schema discovery includes a `search` object listing available
aliases and request limits; internal FTS tables do not appear in SQL tables.
Without a `search` configuration, the search route is absent.

Invalid requests return 400, expired deadlines 408, and oversized results 413.
Private indexes additionally return 404 for an unavailable scope and 413 when
authorized content exceeds its limits (see below).
A busy database returns 503 with `Retry-After`; an invalidated index returns 503
with a generic unavailable message. Search logs contain account attribution,
validated index alias, duration and result counts, but not query text or excerpts.

Search uses its own pool of at most four read-only connections. The configured
query timeout includes waiting for a connection, index validation and ranking.
Startup validation of all configured indexes also shares that timeout.
The application's result-byte limit and the shared SQLite heap limit also apply.
An output limit does not bound the work required to validate or rank an index.

## Optional scope and stable pagination

An index may additionally declare both `scope` and `generation`:

```json
"scope": {"collection": "publications", "field": "manifest"},
"generation": {
  "collection": "search_state",
  "record": "pagesindexstate",
  "field": "generation"
}
```

These collections must be non-system base collections. Their `id` and configured
fields must be nonhidden and SQL-readable. The scope field contains a JSON object
whose values are source record IDs; object keys are unique opaque identifiers.
Scope controls result membership, not access: every authenticated account can
select every configured scope. Applications must protect derived state writes
through their own ordinary REST rules and hooks.

For a scoped index, `scope` is required and selects the scope record by its ID:

```json
{"index":"documents","query":"deployment safety","scope":"publication0001","limit":20,"offset":0}
```

The response additionally includes `scope` and an opaque `generation`. The
server derives this SHA-256 fingerprint from the application token, immutable
index configuration, ranking contract version, SQLite version/source identity,
selected scope ID and validated membership. A membership change also invalidates
old pages even if the application forgot to rotate its raw token.
A configuration change (including weights) therefore invalidates old result pages
even when the stored application token is unchanged. An identical restart retains
the fingerprint. Clients must replay the returned token, never the raw application
record value. This fingerprint is a consistency check, not an authorization token. To fetch later pages,
keep the index, query, scope and limit unchanged, advance `offset` by the hits
already received, and send that generation as `expectedGeneration`. Offsets range
from 0 to 10,000; any positive offset requires `expectedGeneration`. A supplied
generation is checked even at offset zero. A mismatch returns HTTP 409 without
hits; restart from zero instead of appending results from another generation.
Unscoped indexes reject nonempty scope/generation and nonzero offset fields.

Scope membership is applied **before** ordering and limiting. Results use global
index BM25 statistics, ordered by score then source record ID. Adding history can
change the ranking within an unchanged scope, so an immutable scope alone cannot
stabilize pagination. There is no total count or promise of historically
reproducible ranking. One extra hit determines `hasMore`.

Each request reads scope membership, generation, validated index and results in
one read transaction. Scope JSON must be an object of at most 10,000 entries and
4 MiB, with unique keys and string values. Keys, values, scope IDs and generation
tokens must be nonblank valid UTF-8 of at most 128 bytes without NUL. Every member
must exist in the source collection. Missing index entries are allowed (for
example, an application may intentionally omit archived source revisions).
Missing or malformed scope/generation records fail with generic HTTP 503; they
never fall back to unscoped search. `json_each` is a reserved schema-object name
in databases using search, preventing objects from impersonating the built-in
used by fixed membership queries.

Applications must rotate the opaque generation token in the same transaction as
**every** index mutation, including rebuilds, and preserve this relationship in
backups/restores. The server checks the token; it cannot prove that application
maintenance rotated it. Do not derive generation from connection `data_version`,
row counts or latest publication identity. Continuous mutations may repeatedly
invalidate pagination, which clients must surface explicitly.

The search connection's scalar allocation allowance is at least 4 MiB to read
bounded scope documents independently of the response byte cap. Scope decoding,
member existence checks and pagination still share the query deadline; encoded
responses retain the configured `maxBytes` limit. Large offsets and broad queries
can require substantial ranking work despite a small response limit.

## Index consistency and ownership

PocketContext validates canonical index and internal-table schemas and requires
nonempty, unique UTF-8 IDs of at most 128 bytes that refer to source records.
Validation and search run in
one read transaction, so a concurrent schema change cannot redirect a query to
a differently configured index. ID validation scans the corpus on each request;
this has O(N) overhead even for a selective search. Benchmark the intended corpus
and concurrency before enabling search.

Source field permissions are validated at startup, as with SQL reads. Restart
after changing source collection definitions, hidden fields or permissions.

These structural checks cannot prove that indexed text matches its declared
source fields. Only content visible to the entire search audience may be copied
into the index. Even text omitted from excerpts can influence matches and scores.
Application migrations and maintenance are trusted parts of this boundary.

Update or delete index entries in the same transaction as the source state
change. Rebuilds must expose either the previous complete index or the replacement,
with rollback restoring both source and index state. Agent writes continue through
ordinary REST; search never performs writes or repairs an index.

For a publishing application, index only committed published revisions and update
the index as part of publication. PocketContext does not infer publication state.
Validate draft exclusion, publication rollback, deletion, index rebuilds and
populated backup/restore in the adopting application. Enabling this server feature
does not itself change WikiContext or any other application.

Applications must build with all tags in the server Makefile, including
`sqlite_fts5`, when adopting this server version. Arbitrary SQL still cannot read
FTS tables, internal storage tables or SQLite metadata, issue PRAGMAs, or load
extensions. The search pool only executes server-generated parameterized SQL;
it is not exposed as an alternate SQL endpoint.

## Private search for filtered snapshots

Applications with a `snapshot` configuration may enable search only with
private indexes. Results, excerpts, scores, ordering, `hasMore` and generation
tokens then depend only on rows the requester's snapshot filters admit. There is
no application-maintained FTS table. By default, each request reads authorized
rows and searches an in-memory FTS5 index built from exactly those rows.
[Background snapshots](#background-snapshots-and-disk-backed-search) instead
stream authorized rows into generation-owned files.

```json
{
  "authCollection": "users",
  "tables": {
    "page_revisions": ["id", "space", "title", "summary", "body"],
    "publications": ["id", "space", "manifest"]
  },
  "snapshot": {
    "filters": {
      "page_revisions": "EXISTS (SELECT 1 FROM space_members m WHERE m.user = :requester AND m.space = page_revisions.space)",
      "publications": "EXISTS (SELECT 1 FROM space_members m WHERE m.user = :requester AND m.space = publications.space)"
    },
    "policyTables": {"space_members": ["user", "space"]}
  },
  "search": {
    "maxIndexBytes": 8388608,
    "cacheBytes": 67108864,
    "indexes": {
      "pages": {
        "isolation": "private",
        "collection": "page_revisions",
        "columns": ["title", "summary", "body"],
        "snippetColumn": "body",
        "weights": [8, 3, 1],
        "scope": {"collection": "publications", "field": "manifest"},
        "members": "all"
      },
      "notes": {
        "isolation": "private",
        "collection": "page_revisions",
        "columns": ["title", "body"],
        "snippetColumn": "body",
        "where": "page_revisions.space <> 'archive'"
      }
    }
  }
}
```

Index fields:

- `isolation` must be `"private"`. `table` and `generation` are rejected.
- `collection`, `columns`, `snippetColumn` and `weights` follow the shared rules.
  The source must be a snapshot output table; its `id` and every indexed column
  must be configured output columns and nonhidden fields. Values must be text or
  NULL (indexed as empty text); any other value fails the request with 503.
- Optional `scope` names an output table and a configured nonhidden field holding
  the same JSON object of member IDs as shared scopes. `members` is `"all"`
  (default) or `"visible"`, and is accepted only with `scope`.
- Optional `where` is a trusted predicate on the source table that narrows rows.
  It follows snapshot filter rules: one expression, may bind only `:requester`,
  and may reference only configured columns and policy tables. It never widens
  the source filter. In background mode, it may reference only exported tables
  and columns: policy-only tables are absent from the completed snapshot.
- Optional `snapshotScopeWhere` selects scope rows to index before publishing a
  background snapshot. It requires both `scope` and `snapshot.background`; see
  below. Shared indexes and synchronous private indexes reject it.

Search-level limits: `maxIndexBytes` (default 8 MiB, 1 KiB to 64 MiB) caps the
UTF-8 bytes of IDs and indexed values read for one request. `cacheBytes`
(default 64 MiB, from `maxIndexBytes` to 1 GiB) bounds the cache described
below. Both are rejected in shared-workspace mode. Background mode accepts these
settings for configuration compatibility but uses the snapshot disk budget instead.

### Authorization semantics

Request fields and validation match scoped shared search: `scope` is required
for scoped indexes and rejected otherwise; `offset` is 0 to 10,000 and positive
offsets require `expectedGeneration`. Scope-less private indexes also return a
generation and support stable pagination.

In the default synchronous mode, every request runs one filtered snapshot read transaction for the authenticated
account, sharing the snapshot slots, timeout and `maxRows`/`maxBytes` limits:

1. A scoped request reads the scope row through the scope table's filter. A
   missing and an invisible scope both return HTTP 404 `Search scope not
   found`. A visible scope with malformed membership returns 503.
2. Member IDs are decoded and deduplicated, then read through the source filter
   (and `where`). With `members: "all"`, any missing or invisible member fails
   with generic 503 and no hits; results are never partial. With `"visible"`,
   only visible members are indexed.
3. A scope-less request reads every source row visible to the requester,
   narrowed by `where`.

Authorized reads run before any cache lookup, so policy-table changes,
revocation and content changes apply to the next request. Exceeding
`maxIndexBytes` or the snapshot limits returns 413 `Authorized search content
exceeds configured limits`; the server never falls back to shared or partial
data. Snapshot build failures, such as an invalid filter after a schema change,
return generic 503.

### Ranking isolation

The generation is a SHA-256 fingerprint of the private ranking contract version,
SQLite version/source identity, index configuration, scope ID (or a scope-less
marker) and every included row's ID and indexed values, sorted by ID. A fresh
FTS5 table with the canonical shape and `unicode61` tokenizer is filled
in ID order, made `query_only` and queried with the same fixed BM25, snippet and
`ORDER BY score, record_id` query as shared search. BM25 statistics therefore
come only from the requester's included rows. Two accounts with identical
included content receive identical hits, scores, excerpts and generations,
regardless of other accounts' data. Paging is stable while the requester's
included content is unchanged; another account's changes never invalidate it.
The generation is a consistency fingerprint, not an authorization token.

Built indexes are cached by fingerprint in an LRU bounded by `cacheBytes` and 64
entries. An entry is charged the larger of its indexed bytes and its in-memory
database size. Each index uses one pinned SQLite connection; queries on it are
serialized with cancellable waits. A failed connection returns an error and is discarded, never empty
results. Index builds are limited to two at a time and to an in-memory database
of about four times `maxIndexBytes`. At most four private searches retain
authorized rows or active indexes at once; additional requests wait before
reading content, bounded by the query timeout or caller cancellation. Evicted
indexes that are still in use remain alive until their requests finish, so
`cacheBytes` is not a total process-memory cap.

The authorization read uses the snapshot timeout; index build and ranking then
use the configured query `timeoutMs`. Responses keep the `maxBytes` limit.

### Background snapshots and disk-backed search

Opting into [`snapshot.background`](filtered-snapshots.md) switches private search
to SQLite FTS5 files alongside each requester's complete SQL snapshot. SQL and
search read the same immutable, authorized generation; neither search nor caller
SQL attaches the source database. The server streams rows into the index instead
of retaining the complete text corpus in Go memory. Only fixed, parameterized
search queries can access these files. Their FTS tables and internal metadata
remain absent from the arbitrary SQL endpoint.

All unscoped indexes are built before a new snapshot is published. Scoped indexes
may select frequently used scopes with a trusted `snapshotScopeWhere` predicate.
For example, an application can prepare its latest publication in every visible
space:

```json
"snapshotScopeWhere": "publications.sequence = (SELECT max(p.sequence) FROM publications p WHERE p.space = publications.space)"
```

The scope collection's `sequence` and `space` columns must be configured snapshot
outputs. The predicate is startup-validated, can bind only `:requester`, and can
read only exported tables and columns. It selects from the already authorized
snapshot and cannot widen access. There is a maximum of 10,000 prepared scopes
per index. The server has no built-in publication or space rules.

The SQL generation becomes available only after every selected index finishes.
A failed primary index build leaves the previous complete SQL/search generation
available. Other scopes, such as historical publications, are indexed on their
first search and cached within that generation. A failed historical build removes
its partial file and leaves the SQL generation usable. Each scope gets its own
FTS corpus, so BM25 statistics exclude both invisible and unselected records.
The existing `members: "all"` and `"visible"` rules still apply.

Search accepts the background SQL freshness options `consistency` (`"available"`
or `"fresh"`), `atLeast` and `snapshotGeneration`. The response includes a
`snapshot` object with the SQL generation, capture time, refreshing/stale flags,
row and disk counts, and any last build error. The same snapshot headers as SQL
are returned. `snapshotGeneration` requires that exact current snapshot and
returns 409 after it has been replaced; it does not reopen retired generations.
`atLeast` permits a newer snapshot and is useful for reading SQL records after
search. These request fields are rejected outside background private search.

The top-level search `generation` remains a fingerprint of the included text and
ranking configuration. It is distinct from `snapshot.generation`: unrelated
source changes may replace the SQL snapshot without changing scoped search
ranking. Continue pagination with `expectedGeneration`; restart after a 409.
Adding `snapshotGeneration` also rejects unrelated snapshot replacements.

Available reads may serve the previous completed generation while a replacement
builds. Data, grants and revocations therefore have the same eventual freshness
as background SQL. Ordinary authentication and separate REST/file checks remain
live; callers must handle a search result that is no longer available through
those interfaces. Use `consistency: "fresh"` when an operation needs to observe
source changes committed before its freshness check.

The legacy `maxIndexBytes`, `cacheBytes`, snapshot `maxRows` and snapshot
`maxBytes` limits do not cap these streamed indexes. Instead,
`snapshot.background.maxDiskBytes` and `minFreeBytes` cover the combined SQL and
FTS files, including current, building and still-leased retired generations.
`maxValueBytes` bounds individual source values. Scope JSON retains its 10,000
entries and 4 MiB limit. Physical index exhaustion returns the snapshot
storage-unavailable 503 response; indexes never become partial search results.
Historical index files remain until their generation is removed, so searching
many historical scopes can consume the shared disk budget.

Primary and historical index builds use `buildTimeoutMs` (default 120 seconds); a historical request's
cancellation also cancels its build. Ranking then gets its own query `timeoutMs`,
and response limits remain unchanged. The worker and index-build limits are upper
bounds: disk growth is serialized while one build reserves the remaining global
budget. Other builds wait with cancellation until that reservation is released.
FTS connections use a small pager
cache, disable mmap and use disk temporary storage; these settings do not remove
SQLite's process-wide heap limit or make CPU and memory costs independent of
document size. SQLite snippet generation for heavily repeated matches may defer
cancellation until its current callback returns; the deadline is not hard
wall-clock preemption. Benchmark representative documents, scopes and concurrent users.

Completed index files have private permissions and are atomically renamed into
place. A search holds a generation lease through ranking and closes its FTS
connection before releasing the lease. Obsolete SQL and search files are removed
after their final reader finishes. No open FTS connection is retained by the
legacy in-memory cache in this mode.

### Residual risks and scaling

- The per-request scans and memory-cache limits below describe the default
  synchronous mode. Background mode instead follows the disk budgets and
  eventual authorization semantics above.
- Timing reflects server load and cache state. A cache hit can reveal that some
  account recently searched identical content, which the requester can already
  read. Other accounts' corpus size does not affect authorized reads or ranking,
  but shared CPU and snapshot slots do.
- Snapshot filters, policy tables and `where` are trusted application
  configuration. A filter that admits too much exposes it to search as well.
- Every request reads all included rows, and each distinct content set costs an
  index build. Cost grows with the visible corpus, not the result size. Large
  visible sets approach `maxIndexBytes` and snapshot limits and then fail with
  413; such applications need narrower scopes or `where` predicates. Benchmark
  intended corpora and concurrency.
- Cached indexes hold copies of authorized text in process memory until evicted.
  SQLite's process-wide 256 MiB heap limit also applies; keep `cacheBytes` well
  below it.
