# Phase 7 — C++ kernel evaluation (verdict: not adopted)

This directory is the measurement artifact for execution-plan phase 7. It is a
**standalone nested Go module**, deliberately excluded from both builds: the
parent module ignores nested modules, and the root `BUILD.bazel` carries a
`gazelle:exclude` for this path so no cgo `BUILD` file is ever generated. It
exists so the numbers below can be reproduced; it is not part of the library.

## Question

After phases 0–6b, profiling shows the remaining hot query path — hash
partition grouping — is *not* memory-bandwidth-bound: ~60% of cycles sit in
Go's runtime map machinery (hash + probe per row), and the effective scan rate
(~0.3 GB/s sequential) is far below DRAM bandwidth. Would a C++ kernel called
at batch granularity through cgo be worth adopting?

## Experiment

Three implementations of the identical partition kernel (dense
first-appearance codes + per-code counts over int64 keys):

1. `PartitionGoMap` — `map[int64]uint32`, the shape the repo uses today;
2. `PartitionGoOpenAddr` — pure Go, open addressing, linear probing,
   splitmix64 finalizer;
3. `PartitionCXX` — the same open-addressing algorithm in C++ (`kernel.cc`),
   one cgo call per 64k batch, table state in Go-owned memory.

All three produce byte-identical codes and counts (`TestParity`).

## Results (1M rows, i7-1185G7, medians of 3 × `-benchtime 20x`, g++ 16.1.0 -O2)

| distinct | Go map | Go open-addr | C++ via cgo (64k) |
|---|---|---|---|
| 1,000 | 8.11 ms | 6.56 ms | 5.80 ms |
| 100,000 | 18.77 ms | 9.05 ms | 8.45 ms |

Raw cgo call overhead: ~75 ns/call → ~1.2 µs per 1M rows at 64k batches
(negligible; batch granularity works as the design assumed).

## Verdict

- **C++ over identical-algorithm Go: ~1.07–1.13x.** The kernel is dominated
  by dependent loads into the probe table; the language cannot change that.
- **The real headroom is algorithmic and stays in Go:** replacing the runtime
  map with a specialized open-addressing table is 1.2x (low d) to 2.1x
  (high d), and the in-repo sequential partition path (~30 ms/1M) additionally
  pays two probing passes plus per-row closure/chunk-address dispatch over the
  ~8 ms bare loop — roughly 3–5x recoverable without cgo.
- Adopting C++ would cost: a C toolchain for every builder (MSVC/MinGW
  divergence under Bazel on Windows), cgo memory-model constraints, loss of
  `-race` coverage inside kernels, and two-language maintenance — for ≤ ~13%.

**Decision: no C++ kernels.** If the partition path matters at larger scale,
the follow-up is a pure-Go specialized partition table with a fused
counts+scatter pass.

## Reproduce

```bash
# needs gcc/g++ on PATH (winget WinLibs install, see execution-plan health notes)
cd experimental/kerneleval
CGO_ENABLED=1 go test -run TestParity -v
CGO_ENABLED=1 go test -run '^$' -bench . -benchtime 20x -count 3
```
