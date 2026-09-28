# FTS5 feasibility and proposed boundary

Status: design and synthetic driver probe only. FTS5 is not enabled in the normal
build, configuration or SQL endpoint. This document proposes a separate search
surface; it does not promise full-text SQL support.

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

## Proposed first implementation

Use a dedicated authenticated `POST /api/context/search` route with fixed SQL
and a separate read-only connection pool. Preserve the existing `/query` and
`/schema` base-collection restrictions. The search pool must never execute client
SQL, client identifiers or client SQL fragments.

A request would contain only a configured index alias, query text and a bounded
limit. The first version should quote each whitespace-separated input term as
an FTS phrase, escape embedded double quotes, and combine terms with `AND`.
Do not accept raw FTS expressions initially. Reject empty input and cap input
bytes, term count and output rows. Return stable application record IDs, a
numeric relevance score and a bounded plain-text excerpt, ordered by score and
record ID. Excerpts remain untrusted application text; clients must escape them
when rendering HTML. No client-selected ranking functions, weights, tokenizer,
column selection, SQL ordering or pagination offsets in the first version.

Proposed configuration is a separate `search.indexes` map. Each alias declares:

- An exact FTS table name, explicit indexed column names and one `UNINDEXED`
  application record-ID column.
- For every index column, its source base collection and explicit nonhidden
  source columns. All must already be allowed under shared-workspace SQL.
- A fixed snippet column and fixed ranking weights, with bounded numeric values.

Start with content-storing indexes, a fixed tokenizer and a restricted canonical
DDL shape. Reject external-content/contentless indexes, arbitrary modules,
custom tokenizers, extra columns and indirect content sources. Validate actual
DDL and shadow-table metadata against that shape; do not infer safety from a
name prefix. Duplicate IDs, empty IDs and stale/mismatched schema must fail
validation. Reading actual indexed text still relies on trusted application
maintenance: source declarations alone cannot prove that the application copied
the correct text into an index.

Authenticate using the same configured ordinary auth collection. Search is
shared-workspace access: every authenticated account may search every configured
index. Reject any configuration combining search with filtered snapshots.

Trusted initialization validates each index before serving traffic. Every new
pool connection must open read-only, establish query-only mode and existing
SQLite resource limits, then initialize only validated indexes before installing
its narrowly enumerated authorizer. The search authorizer may permit exact
shadow-table columns and the observed internal pragma because it receives only
server-generated parameterized SQL. Verify prepared statements are read-only;
do not remove the authorizer during a request. Pool growth, replacement and
schema invalidation need explicit tests. Unexpected authorization requirements
must fail closed rather than add broad allowances.

## Application responsibilities

Applications create and maintain indexes through trusted migrations and write
hooks; agent writes continue through ordinary REST. PocketContext does not
choose which revisions constitute published content or infer visibility from
PocketBase API rules.

Index updates, deletion and record-ID mapping must commit in the same database
transaction as the source state change. WikiContext must update searchable
content in the publication transaction, excluding drafts and superseded
revisions. A search statement must see a coherent committed corpus. Rebuilds
must preserve the old complete corpus until a replacement commits, and rollback
must restore both publication and search state. Applications must test these
invariants, validate backup/restore behavior, and adopt the server pin explicitly.

Only text visible to the entire search audience may enter an index. Even indexed
columns omitted from results can affect matches and relevance. Filtering results
from a global index is therefore not a filtered-snapshot design. Requester-local
indexes over already-filtered exports require a separate cost and isolation
assessment and are outside the first release.

## Decision and next gates

**No-go:** add shadow-table or pragma exceptions to the arbitrary SQL reader,
or enable FTS5 and claim search is production-ready.

**Go:** implement and review the separate bounded route after a representative
WikiContext benchmark justifies the index. Measure relevance, latency, index
size, concurrent reads/writes and publication cost against batched ranked search.
The current two-row probe measures capability and authorization behavior only.

Before release, test authentication, direct shadow/metadata denial through SQL,
identifier collisions, malformed input, hidden source fields, invalid DDL,
read-only initialization, cancellation and connection reuse. Exercise broad
matches under concurrent load; output limits alone do not bound ranking work.
Validate transaction rollback, deletion, rebuild and populated restore in the
adopting application. Run server tests/build and affected application suites.
Enabling the compile tag must be consistent across server, application and
container builds; arbitrary extension loading remains denied.
