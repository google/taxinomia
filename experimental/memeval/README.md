# memeval — what memory would C++ actually reclaim?

Measurement harness for **Open question 1** in `docs/execution-plan.md`: after
phase 7 rejected C++ kernels for *compute* (~1.07–1.13x), what would moving to
C++ buy in *memory*? The method is to measure the gap between what Go retains
and the analytic flat-layout (C) minimum for the same data, plus the GC costs
a C++ heap would not pay. Whatever the gap is, that — and only that — is the
C++ ceiling.

Like `experimental/kerneleval`, this is a standalone nested module, excluded
from gazelle in the root `BUILD.bazel`: it is never built by `go build ./...`
from the repo root, by `go test ./...`, or by Bazel. It imports
`core/columns` via a `replace` to the parent module.

## Running

```sh
cd experimental/memeval
go build .

# Retained bytes/row per representation vs the flat minimum (10^8 rows;
# the map-bearing case runs at rows/10):
./memeval -mode retained -rows 100000000

# GC cost of a large live heap — pointer-free chunked dataset (~4.8 GiB across
# int64+float64+uint32+dict16+arena) vs the []string representation:
./memeval -mode gc -case pointerfree -rows 100000000
./memeval -mode gc -case gostrings  -rows 100000000
# Heap headroom knobs (no C++ needed):
./memeval -mode gc -case pointerfree -rows 100000000 -memlimit 6000000000
./memeval -mode gc -case pointerfree -rows 100000000 -gogc 25

# Transient allocations of grouping/filtering at 10^8 rows:
./memeval -mode query -rows 100000000
```

## Method notes

- **Retained** = `HeapAlloc` after two forced GCs, built minus empty. The flat
  minimum per case is analytic: payload bytes exactly as a C struct-of-arrays
  would store them (e.g. arena strings: payload + one uint32 offset per row).
- Non-key string cases repeat row 0 at row 1 so `FinalizeColumn`'s uniqueness
  detection bails out on the second row instead of building a transient
  n-entry map; numeric cases use sorted values for the same reason (sortedness
  is detected in the comparison pass, no map is ever built). The map-*bearing*
  case (`key-unsorted-map-16B`) is deliberate: it measures the reverse-lookup
  map an unsorted key column retains — the largest Go-only overhead.
- Chunk-capacity waste from a partial last chunk is visible at small `-rows`
  (a 1M-row run shows ~5% on fixed-width columns: 16 chunks allocated, 15.26
  filled) and vanishes at 10^8 (1526 chunks, last one 89% full). Measure at
  full scale before reading overheads.
- **GC**: build-time stats (`NumGC`, STW pause total, `GCCPUFraction`, peak
  `HeapAlloc`/`HeapSys`/`Sys` sampled at 100 ms) plus five timed forced GCs
  over the live heap — the marginal mark cost of keeping the dataset
  resident. Pointer-free chunks (`[]int64`, byte arenas) are not scanned by
  the collector; `[]string` pays one pointer per row. `-gogc` and `-memlimit`
  apply `debug.SetGCPercent` / `debug.SetMemoryLimit` before building, to
  measure the headroom knobs that exist without leaving Go.
- **Query**: per-op wall time and allocation (mean of 3 after a warm-up call),
  and retained-after-drop, for full-universe `GroupCounts`, a substring
  `FilterSelection`, and the chained filter+group — the transient per-query
  allocations C++ would replace with manual pooling, not eliminate.
- Values are deterministic (splitmix64 / formatted counters); no `math/rand`.
  Results in `core/columns/BENCHMARKS.md` ("C++ memory evaluation") and the
  execution plan's open-question entry.
