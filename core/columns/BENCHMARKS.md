# Column Benchmarks

Benchmark results for column operations on an Intel Core i7-8550U @ 1.80GHz (Windows/amd64).

Run benchmarks with:
```bash
go test -bench=. -benchmem ./core/columns/
```

## Results Summary

### GroupIndices Comparison (1M rows, 100 groups)

| Column Type | Time (ms) | Memory (MB) | Allocs  |
|-------------|-----------|-------------|---------|
| Bool        |        18 |          21 |      66 |
| Uint32      |        22 |          14 |   1,709 |
| Duration    |        37 |          14 |   1,818 |
| Datetime    |        41 |          14 |   1,820 |
| String      |        48 |          14 |   1,818 |

### GroupIndices Edge Cases (1M rows)

| Groups      | Column Type | Time (ms) | Memory (MB) |   Allocs |
|-------------|-------------|-----------|-------------|----------|
| 1 (best)    | Uint32      |        18 |          21 |       36 |
| 1 (best)    | String      |        32 |          21 |       37 |
| 100         | Uint32      |        22 |          14 |    1,709 |
| 100         | String      |        48 |          14 |    1,818 |
| 1,000       | Uint32      |        24 |          13 |   11,020 |
| 1,000       | String      |        62 |          13 |   12,040 |
| 1M (worst)  | Uint32      |       372 |         176 |1,008,199 |
| 1M (worst)  | String      |       668 |         283 |1,016,390 |

### StringColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) |   Allocs |
|----------------|------|--------|-----------|-------------|----------|
| Append         |   1M |      - |       196 |          96 |1,000,063 |
| FinalizeColumn |   1M |      - |      0.01 |        0.01 |       11 |
| GetString      |   1M |      - |       3.5 |           0 |        0 |
| GroupIndices   |   1M |    100 |        49 |          14 |    1,818 |
| GroupIndices   |   1M |  1,000 |        62 |          13 |   12,040 |
| GroupIndices   |  10M |    100 |       547 |         197 |    2,718 |

### Uint32Column

| Operation      | Rows | Groups | Time (ms) | Memory (MB) |   Allocs |
|----------------|------|--------|-----------|-------------|----------|
| Append         |   1M |      - |        10 |          21 |       38 |
| FinalizeColumn |   1M |      - |      0.01 |        0.01 |       11 |
| GetString      |   1M |      - |        85 |         1.8 |  900,001 |
| GroupIndices   |   1M |    100 |        22 |          14 |    1,709 |
| GroupIndices   |   1M |  1,000 |        24 |          13 |   11,020 |
| GroupIndices   |  10M |    100 |       217 |         197 |    2,609 |

### BoolColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) | Allocs |
|----------------|------|--------|-----------|-------------|--------|
| Append         |   1M |      - |       4.7 |         5.2 |     34 |
| FinalizeColumn |   1M |      - |         0 |           0 |      0 |
| GetString      |   1M |      - |       3.4 |           0 |      0 |
| GroupIndices   |   1M |      2 |        22 |          21 |     66 |

### DatetimeColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) |   Allocs |
|----------------|------|--------|-----------|-------------|----------|
| Append         |   1M |      - |        65 |         128 |       39 |
| FinalizeColumn |   1M |      - |       175 |          76 |    8,200 |
| GetString      |   1M |      - |       224 |          24 |1,000,000 |
| GroupIndices   |   1M |    100 |        40 |          14 |    1,820 |

### DurationColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) | Allocs  |
|----------------|------|--------|-----------|-------------|---------|
| Append         |   1M |      - |        14 |          42 |      40 |
| FinalizeColumn |   1M |      - |         0 |           0 |       0 |
| GetString      |   1M |      - |        70 |           8 | 999,000 |
| GroupIndices   |   1M |    100 |        36 |          14 |   1,818 |

## Key Observations

1. **GroupIndices Performance**: Uint32 is ~2.2x faster than String due to no string hashing overhead.

2. **Append Performance**: Uint32 is ~19x faster than String (no string allocation/copying).

3. **GetString Performance**: String is fastest (direct return), Uint32/Duration/Datetime require formatting.

4. **FinalizeColumn**: Bool and Duration are essentially free (no index building needed).

5. **Memory**: GroupIndices allocates ~14MB for result maps with 100 groups.

6. **Edge Cases**:
   - **1 group (best case)**: ~32ms String, ~18ms Uint32 - dominated by slice append operations
   - **1M groups (worst case)**: ~668ms String, ~372ms Uint32 - 14x slower than 100 groups due to map overhead and 1M allocations

## Raw Output

```
goos: windows
goarch: amd64
pkg: github.com/google/taxinomia/core/columns
cpu: Intel(R) Core(TM) i7-8550U CPU @ 1.80GHz
BenchmarkStringColumn_Append_1M-8                              6    196075267 ns/op   96026166 B/op  1000063 allocs/op
BenchmarkStringColumn_FinalizeColumn_1M-8                 106592        11496 ns/op       6952 B/op       11 allocs/op
BenchmarkStringColumn_GetString_1M-8                         306      3463480 ns/op          0 B/op        0 allocs/op
BenchmarkStringColumn_GroupIndices_1M_100Groups-8             24     49146425 ns/op   14133728 B/op     1818 allocs/op
BenchmarkStringColumn_GroupIndices_1M_1000Groups-8            19     62409058 ns/op   13192752 B/op    12040 allocs/op
BenchmarkStringColumn_GroupIndices_10M_100Groups-8             2    547266850 ns/op  196815328 B/op     2718 allocs/op
BenchmarkUint32Column_Append_1M-8                            100     10145750 ns/op   21096470 B/op       38 allocs/op
BenchmarkUint32Column_FinalizeColumn_1M-8                 142903         8173 ns/op       4648 B/op       11 allocs/op
BenchmarkUint32Column_GetString_1M-8                          13     85260477 ns/op    1800468 B/op   900001 allocs/op
BenchmarkUint32Column_GroupIndices_1M_100Groups-8             56     21842030 ns/op   14126650 B/op     1709 allocs/op
BenchmarkUint32Column_GroupIndices_1M_1000Groups-8            49     23941876 ns/op   13079999 B/op    11020 allocs/op
BenchmarkUint32Column_GroupIndices_10M_100Groups-8             5    217398940 ns/op  196808251 B/op     2609 allocs/op
BenchmarkBoolColumn_Append_1M-8                              332      4651922 ns/op    5241669 B/op       34 allocs/op
BenchmarkBoolColumn_FinalizeColumn_1M-8                1000000000         0.36 ns/op          0 B/op        0 allocs/op
BenchmarkBoolColumn_GetString_1M-8                           387      3372294 ns/op          0 B/op        0 allocs/op
BenchmarkBoolColumn_GroupIndices_1M-8                         52     22037290 ns/op   21155608 B/op       66 allocs/op
BenchmarkDatetimeColumn_Append_1M-8                           19     65173526 ns/op  127920172 B/op       39 allocs/op
BenchmarkDatetimeColumn_FinalizeColumn_1M-8                    6    175094533 ns/op   75518664 B/op     8200 allocs/op
BenchmarkDatetimeColumn_GetString_1M-8                         5    223960760 ns/op   24000000 B/op  1000000 allocs/op
BenchmarkDatetimeColumn_GroupIndices_1M_100Groups-8           26     40472381 ns/op   14131824 B/op     1820 allocs/op
BenchmarkDurationColumn_Append_1M-8                          100     14222418 ns/op   41678243 B/op       40 allocs/op
BenchmarkDurationColumn_FinalizeColumn_1M-8            1000000000         0.80 ns/op          0 B/op        0 allocs/op
BenchmarkDurationColumn_GetString_1M-8                        16     70451281 ns/op    7992023 B/op   999000 allocs/op
BenchmarkDurationColumn_GroupIndices_1M_100Groups-8           30     36459350 ns/op   14131491 B/op     1818 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/String-8        25     47984520 ns/op   14133728 B/op     1818 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Uint32-8        49     22219524 ns/op   14126635 B/op     1709 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Bool-8          64     17995416 ns/op   21155592 B/op       66 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Duration-8      31     37231994 ns/op   14131497 B/op     1818 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Datetime-8      32     41358834 ns/op   14131824 B/op     1820 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1Group/String-8             36     31780539 ns/op   21096321 B/op       37 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1Group/Uint32-8             68     17845099 ns/op   21096429 B/op       36 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1MGroups/String-8            2    668237400 ns/op  283135920 B/op  1016390 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1MGroups/Uint32-8            3    372349467 ns/op  175635949 B/op  1008199 allocs/op
PASS
ok  github.com/google/taxinomia/core/columns  52.095s
```

---

## Dictionary encoding prototype (DictStringColumn)

**Different machine — do not compare against the numbers above.** Measured on an
Intel Core i7-1185G7 @ 3.00GHz (Windows/amd64), `-benchtime 20x -count 3`, median
of three runs. `StringColumn` was re-measured on the same machine so the two
columns of each table are comparable.

Run with:
```bash
go test ./core/columns/ -run '^$' -bench 'BenchmarkDict_' -benchtime 20x -count 3
go test ./core/columns/ -run TestMemoryFootprint -v
```

### Time — 1M rows

| Operation | Distinct | StringColumn | DictStringColumn | Speedup |
|-----------|----------|--------------|------------------|---------|
| GroupIndices, full range | 100 | 21.8 ms | 2.5 ms (uint8) | **8.6x** |
| GroupIndices, full range | 1,000 | 27.6 ms | 5.1 ms (uint16) | **5.4x** |
| GroupIndices, 100k subset | 100 | 2.60 ms | 0.25 ms (uint8) | **10.4x** |
| Filter, cheap predicate | 100 | 1.94 ms | 2.09 ms (uint8) | 0.9x (parity) |
| Filter, expensive predicate | 100 | 36.9 ms | 2.05 ms (uint8) | **18x** |
| GetString, full scan | 100 | 2.03 ms | 2.03 ms (uint8) | 1.0x |
| Append (build) | 100 | 24.8 ms | 11.7 ms (uint8) | **2.1x** |
| CompactStringColumn | 100 | — | 23.1 ms (one-off) | — |

### Allocations — GroupIndices, 1M rows

| Distinct | StringColumn | DictStringColumn |
|----------|--------------|------------------|
| 100 | 14.1 MB / 1,818 allocs | 4.0 MB / **8 allocs** |
| 1,000 | 13.2 MB / 12,040 allocs | 4.1 MB / **10 allocs** |

The allocation collapse comes from laying every group out in one backing array
(counting sort) instead of growing a slice per group.

### Retained memory — 1M rows, 100 distinct values

| | Retained |
|---|---|
| StringColumn | 24.58 MB |
| DictStringColumn[uint8] | 1.07 MB |
| **Ratio** | **23x** |

Measured with the column owning its strings, as it does after a load.

### Reading the results

- **Grouping is where it pays.** The code is the group key, so there is no
  hashing and no string comparison per row.
- **Filter only wins when the predicate is expensive.** With a cheap predicate
  the output allocation dominates and the two are level; the win appears once
  the predicate costs more than a code lookup, because it runs once per distinct
  value instead of once per row.
- **Random single-row reads are a wash** — the extra indirection costs about
  what the narrower scan saves.
- **Building is faster, not slower**, despite the interning map: fewer bytes are
  moved and the GC has ~1M fewer live objects to trace.

### Prototype fixes (2026-08-02)

Same machine as the prototype numbers above (i7-1185G7), `-benchtime 20x
-count 3`, median of three runs. Four defects measured by the cardinality sweep
were fixed:

- `CompactStringColumn` now uses absolute bounds (`n >= 4096 && d <= 65536`)
  instead of the `d <= n/2` ratio, and declines key columns without scanning.
- `FinalizeColumn` releases the interning map for non-key columns.
- `GroupIndices` falls back to map grouping when the subset is smaller than
  1/8 of the dictionary, instead of allocating dense arrays sized by it.
- `Ranks()` is guarded by `sync.Once`; concurrent sorts no longer race.

**Small subset (1,000 rows of 1M), by dictionary cardinality** — before the
fix, the d=500k case was **7.8x slower** than `StringColumn`; it is now faster:

| Distinct | StringColumn | DictStringColumn (uint32) | |
|----------|--------------|---------------------------|---|
| 100 | 46.4 µs / 28 KB | 4.6 µs / 10 KB | **10.1x** (dense path) |
| 10,000 | 123.1 µs / 273 KB | 64.7 µs / 168 KB | **1.9x** (map path) |
| 500,000 | 110.2 µs / 273 KB | 65.8 µs / 168 KB | **1.7x** (map path) |

**Cost of deciding not to compact** (1M rows):

| Distinct | Before (n/2 rule) | After |
|----------|-------------------|-------|
| 600,000 | counts up to 500k entries | 5.3 ms, bails at 65,537 |
| 1,000,000 (key) | full counting scan | **25 ns** (`IsKey` short-circuit) |

### Chunked columns (2026-08-03, phase 3a)

Same machine (i7-1185G7), `-benchtime 10x -count 3`, median of three runs,
1M rows. Chunked columns store values in fixed 65,536-row heap chunks
(`DefaultChunkSize`) with `(chunkID, offset)` addressing; flat is the
pre-existing single-slice column.

| Operation | Flat | Chunked | |
|---|---|---|---|
| Build, int64 | 5.85 ms | 3.09 ms | **1.9x** — appending never re-copies full chunks; flat pays doubling-growth copies |
| FilterSelection, int64 (50% selective) | 2.02 ms | 2.32 ms | 0.87x — chunk-loop overhead, the price of chunking on a cheap scan |
| GroupCounts, dict 100 distinct, full universe | 2.34 ms | 0.54 ms | **4.3x** — the chunk-wise loop skips the per-row `ForEachRow` closure the flat dense path pays |
| FilterSelection, dict (50% of values) | 1.60 ms | 1.37 ms | ~parity |

Notes:

- The dict `GroupCounts` win is not chunking itself but the chunk-at-a-time
  scan shape it forces; the flat dense path could adopt the same direct loop.
  It is also the shape the phase-5 parallel executor runs per worker.
- The int64 filter penalty (~15%) is the honest cost of chunked addressing on
  a memory-bandwidth-bound scan with a trivial predicate. Zone-map pruning
  (3b) exists to make such scans skip chunks entirely.

### Zone-map pruning (2026-08-03, phase 3b)

Same machine, `-benchtime 5x -count 3`, median of three runs, 1M rows
(16 chunks). Naive is `FilterSelection` with the equivalent predicate — a
scan of every chunk; pruned is the structured `FilterSelectionEqual`/`Range`,
which skips chunks whose recorded min/max exclude the target. Sorted data is
the layout the design targets (`scaling-to-1b-rows.md` §5: storage sorted by
the filtered dimensions).

| Filter | Naive | Pruned | |
|---|---|---|---|
| Equality, int64, sorted (1 of 16 chunks can match) | 15.9 ms | 0.62 ms | **26x** |
| Range of 10k rows, int64, sorted | 12.6 ms | 1.59 ms | **7.9x** |
| Equality, int64, unsorted (every chunk can match) | 8.4 ms | 8.2 ms | ~parity — bound checks cost nothing measurable; the direct compare also avoids the naive path's per-row closure call |
| Equality, dict, sorted | 2.4 ms | 0.37 ms | **6.5x** |

Notes:

- The pruned floor (~0.4–0.6 ms) is dominated by allocating and zeroing the
  1M-bit result `Selection`, not by scanning — the surviving chunk itself is
  only 65k rows.
- A dict equality on a value absent from the dictionary returns without
  touching any chunk at all, regardless of sortedness.

### Loaders build chunked tables (2026-08-04, phase 3c)

End-to-end through `TableView.ApplyFilters` (the path a URL filter takes),
`BenchmarkApplyFiltersExactSorted` in `core/tables`: 1M rows sorted by the
filtered column, exact-match filter, `-benchtime 3x -count 3`, medians.

| Table | Time | Allocated | |
|---|---|---|---|
| Plain `StringColumn` (per-row scan) | 8.8 ms | 131 KB | |
| Chunked `ChunkedStringColumn` (structured path, zone-map pruned) | 0.84 ms | 262 KB | **10.5x** |

The chunked path allocates one extra 1M-bit `Selection` (the structured
filter's result, intersected into the running selection), which is where the
extra 131 KB comes from — constant per filter, not per row.

### Sorted storage (2026-08-04, phase 4a)

`BenchmarkSortByKey1M` in `core/tables`: load-time cost of sorting a 1M-row
three-column table (5-value string dimension, unique descending int64 pk,
float64 measure) into its declared physical order. `-benchtime 3x -count 3`,
medians. The cost is one permutation sort (`sort.SliceStable` over
`CompareAtIndex`) plus a reorder-rebuild + finalize of every column.

| Sort key | Time |
|---|---|
| `(pk)` | 484 ms |
| `(dim, pk)` | 622 ms |

Paid once per table at load (rebuild-on-start is the accepted model until
persistence is built). The interface-call-per-compare permutation sort
dominates; per-chunk parallel sorting belongs to the 5a/5b executor work if
load time ever matters.

### Sparse PK index + per-role encoding selection (2026-08-06, phase 4b)

`FinalizeColumn` on a 1M-row unique int64 key column (entity-typed, default
chunk size), measured as heap retained by finalize
(`TestSortedKeyFinalizeRetainedMemory` in `core/tables`):

| Storage order | Retained by finalize | Holds |
|---|---|---|
| Sorted by the key | **384 B** | zone maps only — no reverse-lookup map |
| Shuffled | 37.7 MB | `map[int64]uint32` reverse index |

Reverse lookup (`GetIndex`), `BenchmarkChunkedKeyLookup_*`, 1M rows,
`-benchtime 100000x -count 3`, medians:

| Path | Time per lookup |
|---|---|
| Sparse index (sorted storage: binary search over chunk firsts + in-chunk) | 180 ns |
| Reverse-lookup map (unsorted storage) | 63 ns |

The 3x per-lookup cost buys the elimination of the only per-row lookup
structure (scaling doc §6: joins hit the key index once per distinct FK
code, detail lookups once per request — at 10^9 rows the map is impossible,
the search is kilobytes). A string PK's map is several times larger per row
than int64's; it disappears the same way. Until phase 6a memoizes join
resolution per code, joined-column row access pays the 3x per row on sorted
PK targets.

Per-role encoding selection (`DataTable.SelectEncodings`, applied by
`Manager.LoadData` after the sort) re-encodes string columns by role:
declared sort-key dimensions are dictionary-encoded regardless of row count,
other string columns by the existing thresholds, keys never. On sorted
storage the dictionary comes out sorted, so a dict key column's value
lookups binary-search the dictionary and its interning map is released too.

### Arena + front-coded string storage (2026-08-07, phase 4c)

String storage retained at 1M rows, 12-byte sorted keys, each column owning
its content (`TestStringStorageRetainedMemory` in `core/tables`):

| Representation | Retained | Per row |
|---|---|---|
| `[]string` chunks (ChunkedStringColumn) | 32.7 MB | ~33 B (16 B header + individual content allocs) |
| Arena (blob + offsets) | 17.8 MB | ~18 B (4 B offset + packed bytes) |
| Front-coded arena (sorted key) | **7.5 MB** | **~7.5 B** (offset + shared-prefix suffixes) |

The front-coded figure lands inside the scaling doc's 5-10 B/row budget for
a string PK (docs/scaling-to-1b-rows.md, section 5). Arena blobs and offset
arrays are pointer-free: the collector no longer traces a header per row.

Operations, 1M rows, `-benchtime 3x -count 3`, medians:

| Operation | Plain / arena | Front-coded |
|---|---|---|
| Substring scan (`FilterSelection`) | plain 4.9 ms, arena 5.1 ms | (sequential decode, not benched) |
| Random `GetString` | arena 300 ns (zero-copy) | 933 ns (decodes <= 15 entries) |
| `GetIndex` on sorted key | plain sparse search 2.2 us | **1.4 us** (restart-aware search) |

Scan parity is the point: arena reads are zero-copy `unsafe.String` views,
so eliminating the headers costs the scan path nothing. Front-coded random
access pays a decode from the nearest restart (interval 16); its reverse
lookup is faster than the plain sparse search because the binary search
walks zero-copy restart values and decodes at most one restart span.

Structured filters on the front-coded key never scan: equality and IN are
sparse-index lookups, range is two binary searches over a contiguous row
range (sorted storage).

## Parallel executor (2026-08-07, phase 5a)

Filter scans now run on the process-wide worker pool (`core/executor`,
docs/scaling-to-1b-rows.md section 8): chunks are batched into spans and
scanned concurrently, each span writing its own word-aligned range of the
shared Selection bitmap without locking. `BenchmarkParallelFilter`, 1M rows
(16 chunks), 8 hardware threads, `-benchtime 20x -count 3`, medians:

| Scan (unpruned worst case) | Sequential | Parallel | Speedup |
|---|---|---|---|
| Opaque predicate (`FilterSelection`) | 2.13 ms | 0.85 ms | 2.5x |
| Equality on unsorted data | 1.60 ms | 0.59 ms | 2.7x |

The speedup is below the thread count because the scans are
memory-bandwidth-bound (section 9's argument); the win compounds with zone
map pruning, which removes chunks before they are scheduled at all. The
same pool serves every concurrent query (per-job round-robin plus the
submitting goroutine draining its own job), and cancellation is observed
between spans, so a superseded request stops at chunk granularity.

## Per-chunk grouping partials (2026-08-07, phase 5b)

The level-0 grouping pass — group counts plus the scatter of every selected
row into a contiguous per-group layout — now runs as per-chunk partials on
the same worker pool, merged in span order (`PartitionGroups`,
docs/scaling-to-1b-rows.md section 8). Dictionary-coded columns merge dense
partial count arrays by array addition (the global dictionary makes codes
comparable across chunks); hash-grouped columns merge keyed partials in
first-appearance order, so codes, counts, firsts and member order are
bit-identical to the sequential pass. `BenchmarkPartitionGroups`, 1M rows
(16 chunks), 1000 groups, 8 hardware threads, `-benchtime 5x -count 3`,
medians:

| Partition (counts + scatter) | Sequential | Parallel | Speedup |
|---|---|---|---|
| Hash grouping (chunked int64) | 44.6 ms | 21.4 ms | 2.1x |
| Dense dictionary codes | 16.5 ms | 4.0 ms | 4.1x |

The sequential leg is exactly the pre-5b grouping build (GroupCounts plus
the GroupAggregates scatter), so the comparison is the honest before/after.
The dense path parallelises better because its per-row work is an array
increment with no hashing; both stay below the thread count for the section
9 bandwidth reasons. Explicit index-list selections (child-level grouping)
and non-chunked columns keep the sequential path unchanged.

## Per-code join tables (2026-08-10, phase 6a)

Joins with a dictionary-encoded FK column now resolve once per distinct FK
value instead of once per row (`PerCodeJoiner`, docs/scaling-to-1b-rows.md
section 6: joins hit the key index d times, not n times). The first lookup
builds a code -> target-row table with one `GetIndex` per dictionary entry;
every lookup after that is two array reads. `BenchmarkJoin_*`, 1M-row dict
FK (d=1000) against a 100k-row front-coded sorted PK, `-benchtime 3x
-count 3`, medians:

| Join over 1M rows | Per-row joiner | Per-code joiner | Speedup |
|---|---|---|---|
| `Lookup` sweep | 587 ms | 10.1 ms | 58x |
| Joined-column `GroupCounts` | 488 ms | 47.1 ms | 10.4x |
| Allocations (sweep) | 12 MB, 750k allocs | 5.4 KB, 250 allocs | — |

The per-row path pays string materialization plus the section 6 O(log n)
restart-aware search on every row; the per-code path pays that d times at
build and then reads `codes[row]` and `targets[code]`. The joined
GroupCounts residue (47 ms) is the per-row hash grouping of resolved
values, not the join — that shape is 6b's per-chunk pre-aggregation work.
The memo builds lazily under `sync.Once`, so concurrent queries share one
build and unqueried joins never pay it; codes interned after the build
(append-only growth) fall back to direct resolution.

## Per-chunk pre-aggregation (2026-08-15, phase 6b)

Level-0 leaf aggregates over a dictionary-encoded grouping dimension now
merge cached per-chunk (code, partial) summaries instead of rescanning every
row (`PerCodeAggs`, docs/scaling-to-1b-rows.md section 4). The summary is
built once per (dimension, measure) pair in one parallel pass; after that a
selection that covers a chunk entirely takes the chunk's cached partials
without touching a row, and only chunks the selection cuts through are
scanned. `BenchmarkPerCodeAggs`, 1M rows, d=1000 dict dimension in runs (the
sorted-storage shape), float64 measure, `-benchtime 10x -count 3`, medians:

| Per-code aggregate pass | Time |
|---|---|
| Per-row reference (the old leaf accumulation shape) | 5.5 ms |
| First call (parallel summary build + merge) | 1.5 ms |
| Warm merge of cached chunk summaries | **19 µs** |

End to end, `BenchmarkGroupAggregates1M` in `core/tables`: a full grouping
request (level-0 partition + leaf aggregates) on 1M rows, 1000 groups, two
numeric measures, same flags, medians:

| Full grouping request | Per-row leaves | Pre-aggregated | Speedup |
|---|---|---|---|
| 1M rows, 1000 groups, 2 measures | 21.7 ms | 1.9 ms | **11.2x** |

Even the first bulk request beats the per-row pass (the summary build is a
parallel chunk sweep on the worker pool; per-row accumulation is a
sequential interface-call-per-row walk). States assembled from partials
format identically to per-row accumulation; float sums may differ in the
last ulp because chunk subtotals associate differently. A dimension whose
chunks each hold too many distinct codes (high cardinality in random order)
declines permanently under the entry budget rather than retaining
O(chunks × distinct) state; small-subset selections keep the per-row path,
whose cost is proportional to the subset.
