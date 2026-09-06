<!--
    SPDX-License-Identifier: Apache-2.0

    Copyright 2024 The Taxinomia Authors

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# Request timing and the Performance tab

The table page's status bar shows two numbers: **Server**, the time the
server spent producing the page, and **Total**, the browser's time from
navigation to load. The **Performance** tab of the pane breaks the server
time down by phase:

| Phase | What it covers |
|---|---|
| Parse Query | URL → query state |
| Get TableView | per-user table view, cached after the first request |
| Process Joins | join resolution and memo builds |
| Computed Columns | compiling and installing computed columns |
| Apply Filters | filter evaluation over the table |
| Grouping | group construction, aggregates, group sorts |
| Build ViewModel | turning the result window into template data |
| Total Server Time | wall time from the start of the request to rendering |

A warm request on a cached view legitimately reads a fraction of a
millisecond in every phase: everything was cached. That is a real number,
not a missing one.

## "Total Server Time ms" with nothing underneath

If the tab shows the total without a value and no phase rows, your server
did not go through taxinomia's table handler. The handler is the **only**
producer of the timing data: it creates the timing collector, records the
phases as it runs them, and copies the total and the breakdown into the
view model right before rendering. A page produced by calling
`viewmodel.BuildViewModel` and the renderer yourself carries no timing,
and the template renders what it is given. (The build version can still
be present on such a page — it comes from the source revision, not from
the handler — so seeing a version proves nothing about the code path.)

Two ways to get the breakdown.

### 1. Route table requests through the handler (recommended)

This is the path the demo server uses and the golden tests verify:

```go
srv, err := handlers.NewServer(dataModel)      // github.com/google/taxinomia/web/handlers
...
http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    // product implements handlers.ProductConfig
    if res := srv.HandleTableRequestContext(r.Context(), w, r.URL, product, w.Header().Set); res != nil {
        http.Error(w, res.Message, res.StatusCode)
    }
})
```

`HandleTableRequest` (no context) is the same path without cancellation.
Everything on the page — timing, version header, info pane state — is
then produced the way it is tested here.

### 2. Keep your own pipeline and time it yourself

If you build the view model from your own query pipeline, the collector
is public. Record whatever phases you have and hand the results to the
view model before rendering:

```go
timing := handlers.NewTimingCollector()        // platform high-resolution clock

start := timing.Now()
sel := applyMyFilters(...)
timing.Record("Apply Filters", timing.Since(start))

start = timing.Now()
groupMyTable(...)
timing.Record("Grouping", timing.Since(start))

vm := viewmodel.BuildViewModel(...)
vm.RenderTimeMs = timing.TotalMs()           // the "Server" chip and the total row
vm.TimingBreakdown = timing.GetEntries()     // the phase rows
renderer.Render(w, vm)
```

Phase names are free text; the tab lists them in the order recorded.

## The clock

Phases are measured with an `hrclock.Clock` (`core/hrclock`). The default,
`hrclock.System()`, is the finest monotonic counter the platform has:

- **Linux, macOS, BSDs:** the time package's monotonic reading, already
  nanosecond-grained. Nothing to configure.
- **Windows:** `QueryPerformanceCounter` (~100 ns). The time package's
  monotonic clock advances in ~0.5 ms steps there, which would show every
  sub-millisecond phase as `0.00ms` — on a warm cache, a page of zeros.

To use another source (or an injected clock in tests, or one that also
feeds your tracing), implement the two-method interface and install it
with `srv.SetClock(...)`; `nil` restores the default. Servers that build
their own collector use `handlers.NewTimingCollectorWithClock`.

## Checking which build you run

Every page carries the build in the status bar (`r172 · e024d48`), on the
landing page, and in the `X-Taxinomia-Version` response header. Relevant
revisions:

| Revision | Change |
|---|---|
| r170 | the pane's URL / Performance tabs were under the sidebar and unreachable with the sidebar open; fixed |
| r171 | high-resolution phase clock on Windows |
| r172 | clock abstraction (`core/hrclock`, `SetClock`) |
| r173 | the Performance tab says so when no timings were recorded, and points here |

Before r170 the breakdown existed but the only way to reach it was the
`infotab=perf` URL parameter. The collapsed status bar has offered
**Show URL** / **Show perf** buttons since r168.
