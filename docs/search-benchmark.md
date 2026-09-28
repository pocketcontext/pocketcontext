# Synthetic search benchmark

Measured on 2026-09-28 with the pinned SQLite driver, Linux amd64, an Intel Xeon
Skylake virtual CPU and Go's reported parallelism of four. These are local
synthetic measurements, not live WikiContext performance or a latency guarantee.
The benchmark and corpus generator are in
[benchmark_test.go](../internal/searchread/benchmark_test.go).

## Corpus and methods

The deterministic corpus contains 3,000 wiki-shaped pages across ten topics,
with titles, summaries, variable-length bodies and cross-topic references.
There are 2,700 published pages and 300 drafts. Both search methods exclude the
drafts. No private wiki records or production databases are used.

The baseline is one batched SQL scan using case-insensitive substring matching
and field weights of 8 for title, 3 for summary and 1 for body. It returns IDs,
scores and a fixed body excerpt. The FTS measurement calls the public engine
`Search` method with the same weights and a 20-hit limit. It includes read-only
connection handling, transactional schema/ID validation, ranking, snippets and
response bounds. Queries use whole ASCII words supported by both methods;
substring and FTS token matching are not generally equivalent.

A diagnostic also executes fixed FTS SQL directly. It omits engine authorization,
validation and response bounds, and must not be used as production performance
or as a reason to remove those checks.

## Results

The table reports the median of three benchmark runs, each targeting one second.
Times are milliseconds per query; they exclude HTTP, authentication and network
costs. Connections and database pages are warm after benchmark calibration. Other
validation work ran on the same host during this measurement; the ranges below
show that variation and should not be treated as confidence intervals.

| Query workload | Batched lexical scan | Search engine FTS | Raw FTS diagnostic |
| --- | ---: | ---: | ---: |
| Ten rotating topic queries, e.g. `publication rollback` | 36.53 | 33.71 | 8.24 |
| Broad match, `evidence` | 37.32 | 56.00 | 23.22 |
| No match, `quasar hyperdrive` | 25.78 | 27.43 | 0.066 |

Topic search was modestly faster than the baseline; broad and no-match searches
were slower. Engine topic runs ranged from 33.22 to 34.78 ms and broad runs from
52.49 to 67.89 ms. Broad baseline runs ranged from 33.08 to 42.49 ms. No-match
engine runs were 27.07–27.55 ms despite raw FTS taking only 0.062–0.070 ms.

The engine retains whole-index checks for invalid, duplicate and orphan IDs on
every request, including no-match queries. Validation reads the canonical FTS
content table's `c0` column directly after checking its exact DDL in the same
transaction. This preserves all ID checks while avoiding virtual-table content
lookups; no validation cache or relaxed authorizer is involved. The O(N)
validation cost remains. These measurements are not a controlled comparison of
validation implementations; broad ranking adds its own cost.

For topic searches, the engine allocated about 338 KB of Go memory per operation,
compared with about 10 KB for the baseline. These are cumulative Go allocations,
not peak process memory; they do not account for SQLite's C heap.

Both methods achieved 20 relevant published hits out of 20 on each of the ten
topic queries. Relevance here means the page's primary generated topic matches
the query; the test checks 200 hits per method. This establishes synthetic topic
retrieval and draft exclusion, not improved editorial relevance or real-world
search quality. The broad and absent-term queries have no relevance judgment.

| Storage or publication measurement | Result |
| --- | ---: |
| Source database | 10,006,528 bytes |
| Additional initial FTS storage | 10,448,896 bytes |
| Initial index creation/population/commit | 151 ms median |
| Update 20 published pages, source only | 5.02 ms median |
| Update the same pages and index in one transaction | 9.96 ms median |

Publication uses source rowids that remain unchanged during this synthetic run
for FTS deletion and reinsertion, separate fixtures for each method, and SQLite's
default rollback journal. This is not a production identity contract: source
rowids may change after maintenance; applications must preserve the stable
`record_id` mapping. The
three source-plus-index runs ranged from 9.71 to 13.34 ms; source-only runs ranged
from 3.10 to 6.73 ms. WAL, larger documents,
merge history, durable storage and concurrent clients can change those costs.
Initial index timing excludes engine startup validation. After repeated updates,
the indexed database reached 21,966,848 bytes; this is not a long-term size bound.

## Adoption decision

Enable this optional capability when token matching, weighted BM25 and generated
snippets justify its cost for an application's actual workload. This benchmark
does **not** justify calling the current engine faster than batched ranked search.
Leave search unconfigured where those capabilities are unnecessary.

Before a WikiContext adoption, run representative authorized relevance judgments
and concurrent read/publication measurements, including deadlines and memory.
Any future validation optimization must retain rejection of malformed or orphan
IDs and coherent publication snapshots. Source permissions are validated at
startup, as with shared SQL; restart after source schema or permission changes.
FTS and shadow-table DDL are checked against their canonical definitions during
each search. Filtered snapshots remain unsupported.

Independent synthetic tests in
[security_review_test.go](../internal/searchread/security_review_test.go) cover
four concurrent readers using replacement connections during transactional index
rebuilds, malformed IDs outside the requested matches, literal treatment of query
operators, and populated backup/restore
preserving IDs, scores and excerpts. These complement the engine and HTTP tests;
they are not a substitute for application publication and complete-backup tests.

## Reproduce

Use a private temporary directory on a filesystem with sufficient free space.
The tests create and remove isolated databases through Go's `t.TempDir`.

```sh
go test -tags 'sqlite_math_functions sqlite_percentile sqlite_fts5' \
  ./internal/searchread -run '^TestSyntheticSearchRelevance$' \
  -bench '^BenchmarkSynthetic(Search|Publication)$' -benchmem -benchtime=1s -count=3 -v

go test -tags 'sqlite_math_functions sqlite_percentile sqlite_fts5' \
  ./internal/searchread \
  -run '^TestSearch(ConcurrentPublicationSnapshot|OperatorsRemainLiteral|PopulatedBackupRestore|RejectsMalformedIDs)$'
```
