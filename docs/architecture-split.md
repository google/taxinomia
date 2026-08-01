# Proposal: Splitting Taxinomia into a Data Engine and a Presentation Layer

**Status:** proposal only — nothing implemented. Written 2026-07-05, saved 2026-08-01.
**Awaiting:** the six decisions in §6.

## 1. Diagnosis — where the boundary is today (and why it's blurry)

The repo already *looks* layered — there's a `core/` tree and a `demo/` composition root — but the
labels don't match reality. `core/` currently holds both the data engine **and** the entire web
presentation stack, while `demo/` holds wiring **and** a large amount of presentation logic.

Concretely, the current packages sort into three groups:

| Package | What it really is | Web/HTML/URL coupling? |
|---|---|---|
| [core/columns/](../core/columns/) | Typed columnar storage | none — **pure core** |
| [core/tables/](../core/tables/) | `TableView`, filter/group/sort ops | none — **pure core** |
| [core/models/](../core/models/) | `DataModel`, joins, entity types, system tables | none — **pure core** |
| [core/expr/](../core/expr/) | Expression compile/eval | none — **pure core** |
| [core/aggregates/](../core/aggregates/), [core/grouping/](../core/grouping/) | Aggregation & grouping math | none — **pure core** |
| [core/csvimport/](../core/csvimport/), [core/protoloader/](../core/protoloader/) | Data ingest | none — **pure core** |
| [datasources/](../datasources/) | Loaders + `Manager` (sources, annotations, hierarchies, joins) **and** entity-URL resolution | *mostly* core, but `ResolveDefaultURL`/`GetAllURLs` are presentation |
| [core/query/](../core/query/) | Parses a `url.URL` into a `Query` struct; imports `safehtml` | **presentation** (HTTP request representation) |
| [core/views/](../core/views/) | `BuildViewModel`, builds URLs, formats display names, imports `safehtml` | **presentation** |
| [core/rendering/](../core/rendering/) | HTML templates + renderer | **presentation** |
| [core/server/](../core/server/) | `HandleTableRequest` — parses query, runs engine ops, **and** renders HTML | **mixed — the god object** |
| [demo/server.go](../demo/server.go) | Composition root **and** hierarchy navigation, detail-panel building, entity-URL generation | **mixed** |

Two things stand out from reading the code:

**a) [core/server/server.go](../core/server/server.go) is a controller that does everything.**
`HandleTableRequest` ([lines 217–370](../core/server/server.go#L217-L370)) parses the URL into a
`query.Query`, orchestrates the engine (joins, computed columns, filters, grouping via `TableView`),
builds a `views.TableViewModel`, **and** calls `s.renderer.Render(w, ...)` to emit HTML — all in one
method, writing to an `io.Writer` with a `setHeader` callback. Storage/processing and presentation
are interleaved line by line.

**b) The presentation "extension points" are bolted onto core via callbacks.** The `Server` struct
carries seven injected resolvers — `urlResolver`, `allURLsResolver`, `primaryKeyResolver`,
`entityTypeDescResolver`, `hierarchyContextBuilder`, `relatedTablesResolver`
([server.go:69–75](../core/server/server.go#L69-L75)). These are all **presentation concerns** (they
produce URLs and navigation structures), yet they're wired through the core server. The actual
implementations live in [demo/server.go:200–509](../demo/server.go#L200-L509) — ~300 lines of
hierarchy/detail-panel/URL logic sitting in the demo package as closures inside `SetupDemoServer`.
That's presentation logic that has leaked into both `core` (the interfaces) and `demo` (the bodies).

The good news: the genuine engine packages (`columns`, `tables`, `models`, `expr`, `aggregates`,
`grouping`, ingest) are already clean — **no HTTP, HTML, or URL imports**. The seam is mostly
obscured by naming and by two mixed files, not by deep entanglement. That makes this split tractable.

## 2. The proposed seam

> **Core** = everything about *storing, loading, and transforming tabular data*. It knows tables,
> columns, types, joins, filters, expressions, grouping, aggregation, hierarchies. It knows
> **nothing** about HTTP, HTML, URLs, `safehtml`, display names, or pixels.
>
> **Presentation** = everything about turning an HTTP request into a core request and a core result
> into HTML (or later: JSON/gRPC/CLI). It owns URL parsing, view models, templates, product config,
> and all navigation/URL generation.

The dependency rule is one-directional and enforceable: **presentation → core, never the reverse.**
No core package may import a presentation package or a web library.

### The interface between them

Today, presentation reaches directly into the live `TableView` object. The proposal is a narrow,
transport-agnostic contract instead — a declarative request in, a neutral result out:

```go
// package engine  (core side of the seam)

// Request is a transport-agnostic description of what to compute.
// The web layer builds this from a URL; a CLI or gRPC layer could build it too.
type Request struct {
    Table           string
    Columns         []string
    Filters         map[string]string      // column -> filter expression
    GroupBy         []string
    ComputedColumns []ComputedColumn        // name + expression
    Sort            []SortKey
    Limit           int
}

// Result is a neutral, already-computed view of the data.
// No safehtml, no URLs, no display formatting — just data + metadata.
type Result struct {
    Columns  []ColumnMeta          // name, type, entityType, isComputed, isKey...
    Rows     RowReader             // flat rows (zero-copy accessor, not materialized maps)
    Grouped  *GroupNode            // present when GroupBy is non-empty
    Stats    []ColumnStat          // "5 groups" / "100 rows" as data, not strings
    Errors   []FieldError          // per-column compile/filter errors
    Total    int
    Timing   []TimingEntry
}

type Engine interface {
    Query(req Request) (*Result, error)
    Catalog() Catalog               // tables, entity types, joins, hierarchies as *data*
}
```

And the metadata that presentation needs for navigation/URLs is exposed as **plain data**, not as
callbacks into core:

```go
// Catalog answers "what exists", so presentation can build links itself.
type Catalog interface {
    Tables() []TableMeta
    Table(name string) (TableMeta, bool)
    Hierarchies() []Hierarchy       // levels as entity-type lists
    PrimaryKeyEntityType(table string) string
    EntityTypeDescription(et string) string
    Joins() []Join
}
```

This inverts the current control flow. Instead of core calling `hierarchyContextBuilder` (a
presentation callback), **presentation asks core "what are the hierarchies?" and builds the
navigation itself.** The seven resolver setters on `Server` disappear from core; the logic in
[demo/server.go:257–509](../demo/server.go#L257-L509) moves wholesale into a presentation
`navigation` package.

The current `query.Query` (URL-coupled, `safehtml`) stays **on the presentation side** and becomes
the thing that translates URL ⇄ `engine.Request`. The current `views.TableViewModel` also stays
presentation-side and is built from `engine.Result` instead of from a live `TableView`.

## 3. Proposed directory structure

Single Go module, top-level split (recommended over two modules — see §5):

```
taxinomia/
  engine/                    # THE CORE — no HTTP/HTML/URL/safehtml anywhere
    columns/                 # ← core/columns
    tables/                  # ← core/tables
    models/                  # ← core/models
    expr/                    # ← core/expr
    aggregates/  grouping/   # ← core/aggregates, core/grouping
    ingest/                  # ← core/csvimport + core/protoloader + datasources loaders
    catalog/                 # ← datasources.Manager MINUS URL resolution
    query/                   # NEW: engine.Request/Result + the executor
                             #      (orchestration extracted from core/server, minus rendering)

  web/                       # THE PRESENTATION
    handlers/                # ← main.go routing + server.HandleTableRequest/HandleLandingRequest
    urlquery/                # ← core/query  (URL ⇄ engine.Request; owns safehtml)
    viewmodel/               # ← core/views  (engine.Result → TableViewModel)
    render/                  # ← core/rendering (templates + renderer)
    navigation/              # ← demo/server.go resolver bodies + datasources URL resolution
    product/                 # ← demo/product.go, product registry

  cmd/taxinomiad/            # ← main.go + filesystem.go (composition root)
  demo/                      # demo DATA + demo product wiring only (imports engine + web)
  experimental/              # unchanged
```

**What moves, at a glance:**

- `core/server` splits in two: the engine-orchestration half becomes `engine/query`; the
  HTTP+render half becomes `web/handlers`.
- `datasources` splits: loaders → `engine/ingest`, the `Manager`'s catalog/hierarchy/annotation
  state → `engine/catalog`, its `ResolveDefaultURL`/`GetAllURLs` → `web/navigation`.
- The ~300 lines of resolver callbacks in `demo/server.go` → `web/navigation`, driven by
  `engine.Catalog` data.
- `demo/` shrinks to just demo data generators + product textprotos.

**Open question — `core/users`:** `UserStore` does auth + domain-based table filtering. It's neither
pure data-engine nor pure rendering. Suggested placement is `web/` (or a small shared `access/`
package) since its only current use is filtering the landing page by user domain. Flagged for
decision in §6.

## 4. Migration path (incremental, each step compiles)

This does **not** need a big-bang rewrite. Suggested order, smallest risk first:

1. **Rename, don't restructure.** `core/` → `engine/` for the genuinely-pure packages; move
   `query`/`views`/`rendering` out of `core/` into `web/`. Pure import-path churn, no logic change.
   (Bazel `BUILD.bazel` deps must be rewritten too — non-trivial given the recent Bazel migration.)
2. **Split the god object.** Extract engine orchestration from `core/server` into `engine/query`
   returning a *provisional* `Result`; leave `HandleTableRequest` in `web/handlers` calling it and
   rendering. At this stage `Result` can still expose the `TableView` to avoid rewriting the
   ViewModel builder yet.
3. **Neutralize the seam.** Replace the `TableView`-leaking `Result` with the clean DTO/reader
   interface; rewrite `viewmodel` to consume `engine.Result` instead of `TableView`.
4. **Invert the resolvers.** Move the `demo/server.go` navigation logic into `web/navigation`, fed
   by `engine.Catalog`. Delete the seven `Set*Resolver` methods from the engine.
5. **Enforce the boundary.** Add a lint/Bazel-visibility rule so `engine/**` cannot depend on
   `web/**` or `safehtml`.

You can stop after any step and still have a working, cleaner tree.

## 5. Pros and cons

**Pros**

- **Correct, enforceable dependency direction.** `web → engine`, checkable in CI (or via Bazel
  `visibility`). The `core/` misnomer goes away.
- **The engine becomes reusable.** A CLI, a JSON/gRPC API (there is already
  [datasources/datasource.proto](../datasources/datasource.proto)), notebooks, or tests can drive it
  without HTTP. Today that's impossible because the only entry point renders HTML.
- **The god object dies.** `HandleTableRequest` stops being parse+compute+render in one method.
- **Presentation logic stops hiding in `demo/`.** Navigation/URL code lands in `web/`, where it
  belongs; `demo/` becomes just data.
- **Better testability on both sides.** Engine tested with `Request`/`Result` fixtures; presentation
  tested against a fake `Engine`.
- **`safehtml`/URL concerns confined** to `web/`, shrinking the surface where HTML-injection bugs
  can live.

**Cons / costs**

- **Real mechanical churn.** ~30+ files change import paths, and every `BUILD.bazel` needs its
  `deps`/`visibility` updated. This is the bulk of the effort and it's tedious.
- **Seam-design risk.** The current code passes the rich `TableView` straight through. A clean
  `Result` that materializes maps would cost memory/CPU on big tables (there are 1M-row perf
  tables). Mitigation: make `Result.Rows` a **zero-copy reader** over engine columns rather than
  copying — but that's a careful design, not a free lunch.
- **Possible duplication.** `TableViewModel` is already Result-shaped; adding `engine.Result` risks
  two parallel row structures. Needs a deliberate decision on which owns what — recommendation: keep
  `Result` minimal + neutral, `ViewModel` display-only.
- **The navigation extraction is the hard part.** Hierarchy/detail-panel logic
  ([demo/server.go:257–509](../demo/server.go#L257-L509)) is tightly wound with query-cloning and URL
  building. Inverting it to pull from `engine.Catalog` is the step most likely to surface hidden
  coupling.
- **More packages = more Bazel boilerplate** (though visibility rules are also the mechanism that
  makes the boundary *enforced* rather than merely documented — a net win).

## 6. Decisions needed before starting

1. **Module strategy:** single module with top-level `engine/` + `web/` (recommended), or two
   separate Go modules (`go.mod` each — stronger isolation and independent versioning, but more
   friction and no current consumer that needs it)?
2. **Naming:** `engine` vs `core` vs `kernel` for the data side; `web` vs `presentation` vs `app`
   for the other.
3. **Seam purity:** strict copy-out DTOs (simplest, slower on big tables) vs zero-copy reader
   interfaces (faster, more design work). Recommendation leans zero-copy.
4. **Is a non-HTML frontend a real near-term goal** (JSON/gRPC API, given `datasource.proto`)? If
   yes, it justifies investing more in the neutral seam now; if no, the seam can stay thin.
5. **Where `users`/auth lives** — `web/`, or a shared `access/` package?
6. **Enforce the boundary via Bazel visibility** from day one, or add it at the end?

## 7. Addendum — the `core/query` question

An independent pass over the package map raised one point of disagreement worth recording, because
it is the crux of the seam: whether `core/query` belongs to the data layer or to presentation. Both
readings are half-right, and that tension is exactly why the split needs the `Request`/`Result` DTO:

- `query.Query` imports `safehtml` and parses `url.URL` → that half is *transport* (belongs in
  `web/urlquery`).
- But it also carries `Filters`, `GroupedColumns`, `SortOrder`, `ComputedColumns` → that half is a
  *neutral query spec* (belongs in `engine` as `Request`).

So [core/query/query.go](../core/query/query.go) (1,150 lines) is itself a mini-example of the whole
problem: it fuses "how the request arrived over HTTP" with "what computation to run." Splitting it —
URL bits to `web/urlquery`, spec bits to `engine.Request` — is a good **litmus test** for whether the
seam is real. If that one file splits cleanly, the rest will follow.

One further confirmation that strengthens the plan:
[datasources/datasource.proto](../datasources/datasource.proto) already models
`EntityTypeDefinition`, `URLTemplate`, and `Hierarchy` — i.e. the *navigation metadata* is already
declarative config. That makes step 4 (inverting the resolver callbacks) easier than feared:
`web/navigation` can read this straight from `engine.Catalog` instead of core calling back into
presentation.
