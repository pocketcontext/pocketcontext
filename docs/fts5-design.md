# FTS5 authorization boundary

Status: the separate opt-in search surface is implemented. FTS5 is compiled into
the normal build but remains inaccessible through arbitrary SQL. See the
[search contract](search.md) for configuration, maintenance and request details.
The synthetic driver probe below records why a separate surface is necessary.

## Verified findings

The opt-in [probe](../internal/sqlread/fts5_probe_test.go) uses the pinned
`github.com/mattn/go-sqlite3` v1.14.52, an isolated temporary database and separate
`mode=ro`, `query_only=1` connections. Run it from this repository:

```sh
go test -tags 'sqlite_math_functions sqlite_percentile sqlite_fts5 fts5_probe' \
  ./internal/sqlread -run '^TestFTS5AuthorizerProbe$' -v -count=1
```

With `sqlite_fts5`, a synthetic content-storing FTS5 table supports `MATCH`,
`bm25` and `snippet`; the prepared search statement reports itself read-only.
Cold preparation reads the index's `_config` table. Executing the query also
requests `PRAGMA main.data_version` and reads `_config`, `_idx`, `_content` and
`_docsize`. Warming the virtual table with `SELECT rowid ... LIMIT 0` does not
remove the runtime requirements. These are observations for this driver and
fixture, not an exhaustive list of FTS5 operations.

Denying shadow-table reads prevents the search. Allowing the named shadow tables
and the narrow `data_version` pragma permits it, but also permits direct reads of
the stored text from `_content`. The driver authorizer receives identical
operation/table/column/database arguments for some internal and direct reads.
A static allowlist cannot distinguish them. The probe deliberately demonstrates
that exposure; its policy must not be copied into the production SQL reader.

SQLite describes the underlying [FTS5 shadow tables and auxiliary
functions](https://www.sqlite.org/fts5.html), and the
[authorizer interface](https://www.sqlite.org/c3ref/set_authorizer.html).
The driver exposes operation, two operation arguments and database name; it does
not expose SQLite's trigger/view context argument. Adding that argument alone
has not been shown to distinguish FTS-internal work.

## Implemented boundary

The authenticated `POST /api/context/search` route uses fixed SQL
and a separate read-only connection pool. Preserve the existing `/query` and
`/schema` base-collection restrictions. The search pool must never execute client
SQL, client identifiers or client SQL fragments.

An unscoped request contains a configured index alias, query text and a bounded
limit. Scoped indexes additionally accept a scope record ID, bounded offset and
expected generation token, as described in the [scoped search contract](search.md#optional-scope-and-stable-pagination). The implementation quotes each whitespace-separated input term as
an FTS phrase, escapes embedded double quotes, and combines terms with `AND`.
Do not accept raw FTS expressions initially. Reject empty input and cap input
bytes, term count and output rows. Return stable application record IDs, a
numeric relevance score and a bounded plain-text excerpt, ordered by score and
record ID. Excerpts remain untrusted application text; clients must escape them
when rendering HTML. No client-selected ranking functions, weights, tokenizer,
column selection or SQL ordering are accepted. Pagination is available only for
configured scopes with application-maintained generation tokens.

Configuration uses a separate `search.indexes` map. Each alias declares:

- An exact FTS table name and explicit indexed column names. The fixed
  `record_id UNINDEXED` column maps to the source collection's `id`.
- One source base collection whose matching indexed columns and `id` must
  already be allowed under shared-workspace SQL and nonhidden.
- A fixed snippet column and fixed ranking weights, with bounded numeric values.
- Optionally, a nonhidden SQL-readable JSON membership field on a scope base
  collection and a text generation field on a fixed application record. These
  declarations must be supplied together.

The implementation accepts content-storing indexes, a fixed tokenizer and a
restricted canonical DDL shape. It rejects external-content/contentless indexes,
arbitrary modules, custom tokenizers, extra columns and indirect content sources.
It validates actual DDL and shadow-table metadata against that shape rather than
inferring safety from a name prefix. Duplicate IDs, empty IDs and mismatched
index schemas fail validation. Reading actual indexed text still relies on trusted application
maintenance: source declarations alone cannot prove that the application copied
the correct text into an index.

Authenticate using the same configured ordinary auth collection. Search is
shared-workspace access: every authenticated account may search every configured
index. Reject any configuration combining search with filtered snapshots.

Initialization validates each index before serving traffic. Every new pool
connection opens read-only, establishes query-only mode and SQLite resource
limits, and installs its narrowly enumerated authorizer. It permits exact
shadow-table columns, the observed internal pragma and the metadata reads used
by fixed validation queries. It receives only server-generated parameterized
SQL and checks that prepared statements are read-only. The authorizer remains
installed during requests, including cold virtual-table initialization.

Each search validates canonical FTS/shadow schemas and source IDs within the
same read transaction as retrieval. Duplicate, empty and orphan record IDs are
rejected. This scans the corpus even for selective queries; the
[synthetic benchmark](search-benchmark.md) measures that cost. No cache of
validation decisions is used. Scoped searches validate a bounded JSON object,
check that its members exist in the source collection, and filter membership
before ordering and limiting. Scope and generation are read in that same
transaction. A stale expected generation returns 409 without hits. The fixed
queries use only the built-in `json_each` value column; a schema object bearing
that reserved name invalidates search.

## Application responsibilities

Applications create and maintain indexes through trusted migrations and write
hooks; agent writes continue through ordinary REST. PocketContext does not
choose which revisions constitute published content or infer visibility from
PocketBase API rules.

Index updates, deletion and record-ID mapping must commit in the same database
transaction as the source state change. WikiContext must update searchable
content in the publication transaction, excluding drafts. An all-history index
may retain superseded published revisions; scope membership selects which
revisions appear in results for each publication. A search statement must see a coherent committed corpus. Rebuilds
must preserve the old complete corpus until a replacement commits, and rollback
must restore both publication and search state. Every corpus mutation must rotate
the generation token transactionally, including rebuilds that preserve scope
identity. Global BM25 statistics can change historical rankings when new revisions
are indexed; generation checking prevents mixed result pages rather than freezing
old scores. Scope is not a requester-specific access policy. Applications must test these
invariants, validate backup/restore behavior, and adopt the server pin explicitly.

Only text visible to the entire search audience may enter an index. Even indexed
columns omitted from results can affect matches and relevance. Filtering results
from a global index is therefore not a filtered-snapshot design. Requester-local
indexes over already-filtered exports require a separate cost and isolation
assessment and are outside the first release.

## Application adoption gates

Shadow-table and pragma exceptions belong only in the fixed-query search pool.
Compiling FTS5 alone does not authorize arbitrary SQL access or establish an
application's search consistency.

The separate bounded route is available for applications to adopt intentionally.
The two-row authorizer probe establishes capabilities and security requirements;
the synthetic corpus benchmark is not a live WikiContext performance result.
Before application adoption, measure relevance, latency, index size, concurrent
reads/writes and publication cost against batched ranked search on representative
authorized data.

Server tests cover authentication, direct shadow/metadata denial through SQL,
malformed input, hidden source fields, invalid DDL, read-only initialization,
cancellation and connection reuse. Application adoption additionally requires
transaction rollback, deletion, rebuild and populated restore checks. Exercise
broad matches under concurrent load; output limits alone do not bound ranking
work. Run server tests/build and affected application suites.
Enabling the compile tag must be consistent across server, application and
container builds; arbitrary extension loading remains denied.
