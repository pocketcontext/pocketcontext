# Optional full-text search

PocketContext offers full-text search through a separate authenticated endpoint.
It is opt-in and supports only shared-workspace applications. All accounts in the
configured auth collection can search every configured index. PocketBase record
API rules do not filter search results. A configuration combining `search` with
`snapshot` is rejected.

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

The request accepts only `index`, `query` and optional `limit`. The default limit
is 20, bounded by the application's `maxRows`; the maximum is the smaller of
100 and `maxRows`. Query text is limited to 4096 UTF-8 bytes and 16
whitespace-separated terms. Terms become quoted FTS phrases combined with AND.
Words such as `OR` are literal terms, not query operators; SQL, raw FTS syntax,
client-selected columns and ranking expressions are not accepted.

The response contains `index`, `hits` and `truncated`. Each hit has `id`, `score`
and `excerpt`. Lower BM25 scores rank first, with record ID as the tie-breaker.
Excerpts are plain text, including any untrusted markup stored by the application;
clients must escape them before HTML rendering. A truncated response is incomplete.
Responses use `Cache-Control: no-store` and `X-Context-Truncated`.

Authenticated schema discovery includes a `search` object listing available
aliases and request limits; internal FTS tables do not appear in SQL tables.
Without a `search` configuration, the search route is absent.

Invalid requests return 400, expired deadlines 408, and oversized results 413.
A busy database returns 503 with `Retry-After`; an invalidated index returns 503
with a generic unavailable message. Search logs contain account attribution,
validated index alias, duration and result counts, but not query text or excerpts.

Search uses its own pool of at most four read-only connections. The configured
query timeout includes waiting for a connection, index validation and ranking.
Startup validation of all configured indexes also shares that timeout.
The application's result-byte limit and the shared SQLite heap limit also apply.
An output limit does not bound the work required to validate or rank an index.

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
