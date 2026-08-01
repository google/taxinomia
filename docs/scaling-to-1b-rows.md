# Scaling Taxinomia to 1,000,000,000 Rows

**Status:** design analysis. The dictionary-encoding prototype described in §7 is
committed and measured; everything else is proposed, not built. Written
2026-08-01.

**Settled constraints** (not open questions):

- The target is **10^9 rows per table**, with **queries executed in parallel**.
- Every table has **at least one primary key column** — unique, so `d = n`.
- The engine is **built here**, in this repository. Adopting an external engine
  is out of scope.
- **Nothing is ever displayed in full.** Not leaf rows, not groups, not the group
  tree. Therefore nothing is ever computed in full.

---

## 1. The governing principle: the viewport is the unit of work

A user looking at a grouped billion-row table sees perhaps fifty lines. They see
the first page of top-level groups, a handful of those expanded, and one page of
leaf rows inside whichever group they opened. Everything else — the other 65,000
groups, the sub-groups underneath them, the 10^9 row identities — is never drawn.

The engine must therefore be organised around producing **a window**, not around
producing a result set that a renderer then slices. That inverts the current
model, in which grouping builds the complete tree and the display limit is
applied afterwards.

This is the constraint that determines the architecture. Encoding, chunking and
parallelism all matter, but they are in service of it: they make the *scan* that
a window needs cheap, and they bound the state that a window leaves behind.

### The cost model this implies

| Quantity | Required cost | Never allowed to be |
|---|---|---|
| Leaf row identities | O(page) | O(rows) |
| Group membership lists | O(page), only for the open group | O(rows) |
| Sub-group levels | O(expanded groups) | O(groups at level 0) x O(distinct at level 1) |
| Level-0 group aggregates | O(distinct) state | O(rows) state |
| Sorting | top-k heap, k = page size | full sort |
| Scan | O(selected chunks), prunable | O(table) unconditionally |

The single line that matters most: **state must scale with the number of distinct
values and the size of the viewport, never with the number of rows.**

## 2. Where today's code contradicts it

[core/tables/table_view.go](../core/tables/table_view.go) already carries a
`displayLimit` and a top-k heap, so the intent is present — but the pruning
happens after the work, not instead of it:

- `groupFirstColumnInTable` ([line 519](../core/tables/table_view.go#L519))
  builds **every** level-0 group with a complete `[]uint32` membership list.
- `groupSubsequentColumnsInTable` ([lines 534-589](../core/tables/table_view.go#L534-L589))
  then walks **every** group of **every** parent block and groups within it,
  materialising the entire tree depth-first.
- `ComputeAggregates` ([line 374](../core/tables/table_view.go#L374)) computes
  aggregates for every group at every level.
- Only then does `sortGroupsInBlockTopK` ([line 397](../core/tables/table_view.go#L397))
  select the top k — and only for the first block, and by group value rather
  than by aggregate.

So the top-k saves a comparison-sort but nothing else: the tree, the membership
lists and the aggregates have all been built by the time it runs. At 10^6 rows
that is a modest waste. At 10^9 it is the difference between a query that
returns and one that does not.

The same pattern holds one level down: `GroupIndices` returns
`map[uint32][]uint32` — every group's full membership — and `Filter` returns a
materialised index list. Both interfaces hand back O(rows) of data that the UI
will discard.

## 3. What the execution model becomes

A query is not "group this table". It is **"given this filter, this grouping,
this ordering, this expansion state and this viewport, produce the visible
lines"**. Concretely, five phases:

**A. Prune.** Use per-chunk zone maps (min/max per column) to eliminate chunks
that cannot satisfy the filter. For a selective filter on a sorted dimension this
removes most of the table before any row is touched.

**B. Level-0 aggregate pass.** Scan the surviving chunks accumulating
`counts[code]` and per-group aggregates. Output is O(distinct), not O(rows) —
one array, not a tree. This is the only phase that touches the bulk of the data,
and §4 covers when it can be skipped too.

**C. Order and window.** Rank the level-0 groups by whatever the user sorted on
and take the visible slice with a top-k heap. Groups outside the window are
identified but never elaborated.

**D. Expand on demand.** For each *visible and expanded* group only, scan that
group's rows to produce its child level. With storage sorted by the grouping
dimensions, a group's rows are a **contiguous chunk range**, so expansion costs
O(that group), not O(table). Recurse for deeper levels, always only through
expanded nodes.

**E. Materialise the page.** Resolve actual row identities and cell values only
for the leaf rows actually visible, and only within the open group. Sorting leaf
rows uses a bounded heap of size `offset + limit`, never a full sort of the
group's rows.

**Cache A-B per query shape.** The level-0 state is O(distinct) — kilobytes to a
few megabytes. Cache it keyed by `(table, filters, grouping)` so that scrolling,
expanding and collapsing are served from memory with no rescan. This is what
makes interaction feel instant: the expensive pass happens once per query shape,
not once per viewport change.

**Everything is cancellable.** Viewport changes arrive faster than billion-row
scans complete. Chunk-level work items must observe a context so a superseded
query stops immediately rather than competing for cores with its successor.

## 4. When the full scan can be avoided as well

Phase B is O(selected rows) in the general case. Whether it can be avoided
depends on the ordering the user asked for, and being precise here matters more
than a slogan:

- **Ordered by aggregate** ("zones with the most machines"): you cannot know the
  top ten without a value for all of them, so the pass is unavoidable. It stays
  O(rows) in time but O(distinct) in space, parallelises across chunks, and
  prunes with the filter.
- **Ordered by group key** (alphabetical, chronological, natural): if storage is
  sorted by that key, the first page of groups lives in the first chunks.
  **Early termination applies** — read chunks until the window is full, then
  stop. O(visible), not O(rows).
- **Pre-aggregated dimensions.** Per-chunk partial aggregates for declared
  grouping dimensions can be computed once at load. Grouping by such a dimension
  then merges ~15,000 chunk summaries instead of scanning 10^9 rows, for any
  ordering. This is the strongest form of "never compute in full", and it is
  cheap for the handful of dimensions people actually group by.

So: the group tree, the membership lists and the row identities are *never* fully
computed, unconditionally. The level-0 aggregate pass is avoidable in two of the
three cases, and bounded in the third.

## 5. Storage that supports this

**Chunking is the foundation.** Fixed row groups of 64k-256k rows, each with
min/max zone maps and chunk-local addressing `(chunkID, offset)`. Pruning,
parallelism, bounded scratch memory, cancellation granularity and spill-to-disk
all derive from it. Chunk-local `uint32` offsets also keep row addressing 4 bytes
wide permanently — `uint32` covers 4.29e9 globally, which fits 10^9 with only 4x
headroom.

**Sort order is a query-performance decision, not a storage detail.** Sorting by
`(low-cardinality dimensions..., primary_key)` gives four things at once: long
runs in the leading columns (RLE to near nothing), effective zone-map pruning on
the columns people filter by, contiguous chunk ranges per group so phase D is
scoped, and early termination in phase C when the ordering is by group key. The
primary key stays unique as the tie-breaker suffix.

**Per-role encodings.** At 10^9 rows a single low-cardinality string column
retains ~24 GB unencoded, so encoding is not a tuning decision — it is the only
representation that exists. Which encoding depends on the column's role:

| Column role | Encoding | Per-row cost at 10^9 |
|---|---|---|
| Low-cardinality dimension | Dictionary, uint8/uint16 codes | 1-2 GB |
| Leading sort-key dimension | Dictionary + RLE over runs | ~0 |
| Dense integer PK | **Implicit** — key *is* the row position | **0 bytes** |
| Sorted integer PK | Delta + frame-of-reference bitpacking | often < 1 byte |
| String PK | Front-coded arena + sparse index | ~5-10 bytes |
| High-cardinality non-key string | Arena: one `[]byte` blob + offsets | ~5 bytes + content |
| Numeric measure | Plain, or bitpacked to observed range | 1-8 bytes |

Note that the fallback for a high-cardinality column is **never** today's
`[]string`. The choice is dictionary versus arena; `[]string` leaves the design
entirely. This also removes Go's largest GC liability: 10^9 string headers is
10^9 pointers traced every cycle, whereas code arrays and byte arenas are
pointer-free and invisible to the collector.

## 6. The primary key is the hard column

Every table has one, and it is the pathological case: `d = n`, so dictionary
encoding is strictly negative (the dictionary holds every value *and* you pay for
codes on top), and `map[string]int` for reverse lookup is impossible — ~50+ bytes
per entry puts a 10^9-entry map past 50 GB with a cache miss per probe.

Three observations make it tractable:

**Joins hit the key index `d` times, not `n` times.** Joins run FK to PK, and the
FK column is low-cardinality and dictionary-encoded. Resolving once per distinct
FK code gives at most tens of thousands of lookups per join instead of 10^9. An
O(log n) key lookup is therefore entirely acceptable, which removes the need for
a hash index at all.

**A sparse index replaces the map.** With sorted storage, PK lookup is a binary
search over one index entry per chunk plus a scan within that chunk. At 65,536
rows per chunk, 10^9 rows needs ~15,000 index entries — kilobytes.

**Detail-panel lookups are single-row.** The other consumer of the key index is
"show me this entity", which is one lookup per request. Nothing needs a hash map.

An immediate consequence for code already on disk: `CompactStringColumn` should
short-circuit on `IsKey()`. The PK is precisely the column that today burns a
full n/2 scan before declining to compact.

## 7. What the dictionary prototype measured, and what it got wrong

[core/columns/dict_column.go](../core/columns/dict_column.go) implements
`DictStringColumn[K Unsigned]`. Measured at 1M rows on an i7-1185G7 (full tables
in [core/columns/BENCHMARKS.md](../core/columns/BENCHMARKS.md)):

| Operation | StringColumn | Dict | |
|---|---|---|---|
| GroupIndices, 100 distinct | 21.8 ms | 2.5 ms | **8.6x** |
| GroupIndices, 100k subset | 2.60 ms | 0.25 ms | **10.4x** |
| Filter, expensive predicate | 36.9 ms | 2.05 ms | **18x** |
| Build | 24.8 ms | 11.7 ms | **2.1x** |
| Retained memory (100 distinct) | 24.6 MB | 1.07 MB | **23x** |
| GroupIndices allocations | 1,818 | **8** | |

The encoding and the finding that the code can serve as the group key both carry
over. But the prototype optimises `GroupIndices(indices) map[uint32][]uint32` —
an interface that returns full membership for every group, which §1 says must not
exist. **The right interface is three narrower ones:**

```go
GroupCounts(sel Selection) []uint32                          // O(distinct)
GroupAggregates(sel Selection, spec AggSpec) []Agg           // O(distinct)
GroupMembers(sel Selection, code uint32, off, n int) []RowID // O(page), one group
```

That is not a small edit — it is the difference between the batch model and the
viewport model, and it should be made before any further tuning of the encoding.

The cardinality-threshold work (`CompactStringColumn`'s `d <= n/2` rule) measured
three real defects: it sanctions a **1.5x memory regression at its own
boundary**, it expresses an absolute constraint as a ratio, and it misses the
case where grouping a small subset of a high-cardinality column is **7.8x
slower**. Fixes are an absolute cap, released interning maps, an `IsKey()`
short-circuit and a subset fallback. All of it is worth doing for the code as it
stands today, and none of it matters at 10^9, where every column is encoded by
role rather than by threshold.

## 8. Parallelism

**Intra-query** parallelism falls out of chunking: chunks are immutable and
independent, so phases A, B and D are per-chunk partials plus a merge. With a
**global, table-level dictionary**, codes are comparable across chunks, so
merging partial group counts is array addition rather than key translation. The
cost is one dictionary-unification pass at load — build per-chunk dictionaries
during a parallel load, unify, emit per-chunk translation tables — recovered on
every query afterwards.

Count-based grouping parallelises cleanly: a 65,536-entry `uint32` count array is
256 KB per worker, comfortably per-core, merged by summation.

**Inter-query** parallelism needs three rules:

- **Nothing mutates a column after finalize.** The prototype's `Ranks()` lazily
  populates a cache field with no synchronisation — two concurrent queries
  sorting the same column race today. It needs `sync.Once` or precomputation at
  finalize, and the rule must be explicit so the next lazy cache does not
  reintroduce it.
- **One global worker pool, not one per query.** Ten concurrent queries each
  spawning `GOMAXPROCS` workers oversubscribes badly; a shared chunk-work queue
  with per-query fairness and cancellation is required.
- **Per-request state is O(distinct + viewport).** This is the same rule as §1,
  seen from the concurrency side: it is what allows many simultaneous queries to
  coexist, because each one's footprint is bounded by its result window rather
  than by the table.

## 9. Language: Go, C++, or both

The GC argument that usually drives a rewrite dissolves once storage is encoded:
Go does not scan pointer-free spans, so code arrays and byte arenas cost the
collector nothing. It is today's `[]string` that is unviable, and §5 removes it
regardless of language.

**What C++ genuinely offers:** portable SIMD intrinsics and reliable
auto-vectorisation, worth a real 4-8x on *compute-bound* kernels — bitpacking and
unpacking, bitmap operations, string comparison, run decoding — with no portable
Go equivalent; explicit memory control (arenas, huge pages, NUMA pinning);
monomorphised typed kernels without interface dispatch; and a mature ecosystem in
exactly this domain.

**What argues against a rewrite:** pure scans are memory-bandwidth-bound and Go
saturates memory bandwidth; the orders of magnitude on the table are algorithmic,
not linguistic — a Go engine that never materialises what it will not display
beats a C++ engine that does, by a margin no instruction set closes; the cost is
the entire repository, since server, templates, protobuf pipeline, loaders and
view model are all Go; and memory-safety bugs under 10^9-row parallelism are
brutal in exactly the component most likely to have them.

**Recommendation: stay in Go, and let chunking keep the door open.** cgo's
per-call overhead is tens of nanoseconds — irrelevant when the call is made once
per 65,536-row chunk rather than once per row. Bazel already builds both
languages, so a `cc_library` of kernels behind a batch-granularity interface is a
contained, reversible experiment. Do the algorithmic work first, measure, then
port individual kernels — bitmap operations and bitpacking first, as the most
compute-bound — if measurement justifies it. Choosing a language before the
algorithm optimises the constant factor of the wrong algorithm.

## 10. Suggested order of work

1. **Fix the current prototype** — absolute cap instead of `n/2`, release
   interning maps at finalize, `IsKey()` short-circuit, subset fallback in
   `GroupIndices`, `sync.Once` on `Ranks`.
2. **Change the grouping interface** to counts / aggregates / paged members, and
   make `groupSubsequentColumnsInTable` expand only visible, expanded groups.
   This is the change that matters most and it is testable at today's scale.
3. **Bitmap selections** end to end, replacing `[]uint32` and `[]int`.
4. **Chunking** with zone maps and chunk-local addressing.
5. **Sorted storage, sparse PK index, per-role encodings.**
6. **Parallel executor** — global pool, per-chunk partials, cancellation.
7. **Vectorized joins** via per-code resolution tables.
8. **Per-chunk pre-aggregation** for declared dimensions.
9. **Measure, then consider C++ kernels** for whatever remains compute-bound.

Steps 2 and 3 change the ceiling from ~10^6 to ~10^8 and can be done inside the
current structure. Step 4 is what makes 10^9 reachable. Steps 5-8 are what make
it usable.

## 11. Open questions

1. **Storage residency.** Does the working set stay in RAM at 10^9 rows across a
   realistic column set, or is mmap-backed on-disk storage with page-cache
   residency required? This decides whether chunks are heap objects or file
   ranges, and should be settled before chunking is implemented.
2. **Ingest format.** Textproto and CSV cannot deliver 10^9 rows. A binary,
   chunk-aligned, memory-mappable format is a prerequisite, and it interacts
   directly with the loader design in [data_sources.md](data_sources.md).
3. **Sort key selection.** Automatic from cardinality statistics, or declared per
   source in `datasource.proto`?
4. **Update model.** Append-only or mutable? Sorted storage plus global
   dictionaries strongly favours immutable chunks with background merge.
5. **Expansion state ownership.** The viewport model requires the engine to know
   which groups are expanded, so `engine.Request` in
   [architecture-split.md](architecture-split.md) must carry viewport and
   expansion state. Note this is a *query* concept — which subtrees to compute —
   not a presentation concept leaking inward.

   It also settles that document's §6 decision 3, and in the opposite direction
   to what it assumed: because the engine now returns only a window (a few
   hundred rows plus O(distinct) group summaries), **plain copy-out DTOs are
   affordable and simpler**, and the zero-copy reader interface is unnecessary.
   The laziness belongs *inside* the engine — never computing what will not be
   shown — not at the seam. The seam's cost argument assumed the engine hands
   back whole result sets, which the viewport model removes.
6. **Concurrency budget.** How many simultaneous queries, at what latency target?
   This sizes the worker pool and decides whether per-query memory needs hard
   caps.
