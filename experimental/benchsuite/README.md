# benchsuite

The scale-sweep benchmark suite: one
deterministic 20-column reference table (`full20`), generated at 10⁴ / 10⁶ /
10⁸ rows, with a fixed operation matrix run identically at every scale —
L1/L2 (load, memory), Q1–Q17 (one mechanism each), C1–C5 (combination
scenarios whose acceptance claim is decomposability into their constituent
Q-ops, reported as a ratio).

Standalone excluded module (like `experimental/memeval`): never built by
`go build ./...` from the repo root or by Bazel, and it must not import
proto-dependent packages (`web/handlers`, `datasources`, …) — those need
Bazel-generated `.pb.go`. Consequences, all documented in the op notes:

- Q10/C5 measure the request *pipeline* (URL parse → joins → computed →
  filters → grouping → aggregate sort → leaf listing), not HTTP transport,
  auth or HTML templating — mirroring `web/handlers/server.go` order.
- Computed columns use a minimal reimplementation of the handler's
  expression-to-column wiring (`addComputed` in `ops.go`).
- Q3's range filter calls the column API directly; URL filters have no
  range syntax today.

## Run

```bash
cd experimental/benchsuite
go run . -scale 1e4                 # seconds
go run . -scale 1e6                 # ~10 s build + ops
go run . -scale 1e8 -json out.json  # minutes, ~17 GB peak
```

Flags: `-scale` (rows), `-sortkey pk|drill`, `-ops Q5,C1,...` (default all),
`-seed`, `-iters` (default 50 up to 1e6, 5 above), `-json path`.

Markdown results go to stdout (BENCHMARKS.md house style); `-json` writes
the machine-readable record for cross-revision comparison. Same seed + same
scale → byte-identical data. Fast ops are auto-batched (testing.B style) so
medians stay meaningful under the platform timer granularity; `cold_s`
reports first-run costs (join memos, pre-agg summaries) or ×8-concurrency
walls where the op defines one.

Results are recorded internally, one dated section per
(rev, scale).
