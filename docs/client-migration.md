# Migrating off taxinomia's deprecated API

**Audience: an LLM coding agent (or a human) working in a repository that
imports `github.com/google/taxinomia`.** Work through this document top to
bottom. When you are done, the repository will compile and behave identically
while using none of the deprecated surface — which is the precondition for
taxinomia's upcoming cleanup release ("CLEAN") that deletes that surface.

Everything deprecated still works today, and every replacement already exists
in the version you are building against, so the migration can be done
incrementally and verified step by step. Old and new names are type aliases or
delegating wrappers wherever possible: mixing migrated and unmigrated code is
safe at every intermediate point.

## Step 0 — find out what you actually use

Run these searches from the repository root. Each hit maps to one section
below; sections with zero hits can be skipped entirely.

```sh
# 1. Forwarding import paths (section 1)
grep -rn --include='*.go' -E 'taxinomia/core/(query|views|rendering|server)"' .

# 2. Deprecated column filtering (section 2)
grep -rn --include='*.go' -E '\.Filter\(' .

# 3. Deprecated TableView index-list methods (section 3)
grep -rn --include='*.go' -E '\.(GetFilteredIndices|GetSortedTopK)\(' .

# 4. Deprecated grouping surface (section 4)
grep -rn --include='*.go' -E '\.GroupIndices\(|\.Indices\b' .

# 5. Deprecated resolver setters (section 5)
grep -rn --include='*.go' -E 'Set(URL|AllURLs|PrimaryKey|EntityTypeDescription|RelatedTables)Resolver|SetHierarchyContextBuilder' .

# 6. Concrete-type assertions on loader output (section 6)
grep -rn --include='*.go' -E '\*columns\.(String|Bool|Int64|Uint64|Uint32|Float64|Datetime|Duration)Column\b' .
```

If every search returns nothing, you are already migrated; jump to
"Final verification".

---

## 1. Import paths: four packages moved

Four packages moved from `core/` to `web/`; the old paths are forwarding
packages made of type aliases, so this migration is purely mechanical — same
types, same behaviour, no call-site changes beyond the package identifier.

| Old import (deprecated) | New import | Package identifier |
|---|---|---|
| `github.com/google/taxinomia/core/query` | `github.com/google/taxinomia/web/urlquery` — for `Query`, URL parsing/building | `query.` → `urlquery.` |
| `github.com/google/taxinomia/core/query` | `github.com/google/taxinomia/core/queryspec` — for the neutral vocabulary (see below) | `query.` → `queryspec.` |
| `github.com/google/taxinomia/core/views` | `github.com/google/taxinomia/web/viewmodel` | `views.` → `viewmodel.` |
| `github.com/google/taxinomia/core/rendering` | `github.com/google/taxinomia/web/rendering` | unchanged (`rendering.`) |
| `github.com/google/taxinomia/core/server` | `github.com/google/taxinomia/web/handlers` | `server.` → `handlers.` |

`core/query` splits into **two** destinations. Choose per symbol:

- **`core/queryspec`** (a permanent `core/` package) holds the neutral query
  vocabulary: `AggregateType`, `ColumnType` and their constants, `SortColumn`,
  `GroupAggSort`, `ComputedColumnDef`, `GetAvailableAggregates`,
  `AggregateSymbol`, `AggregateTitle`. If a file only uses these, import
  `queryspec` — this is the right choice for non-web code, since `urlquery`
  pulls in an HTML-safety dependency.
- **`web/urlquery`** holds `Query` and everything that parses or builds URLs.
  It also re-exports the whole `queryspec` vocabulary, so a file using both
  kinds of symbols can import `urlquery` alone.

Because all names are aliases, `query.SortColumn`, `urlquery.SortColumn` and
`queryspec.SortColumn` are the *same type* — migrated and unmigrated files
interoperate freely.

```go
// Before
import "github.com/google/taxinomia/core/query"
func handle(q *query.Query, sorts []query.SortColumn) { ... }

// After
import "github.com/google/taxinomia/web/urlquery"
func handle(q *urlquery.Query, sorts []urlquery.SortColumn) { ... }
```

## 2. Column filtering: `Filter` → `FilterSelection`

`Filter(predicate) []int` on the plain column types (`StringColumn`,
`DictStringColumn`, `BoolColumn`, `Float64Column`, `Int64Column`,
`Uint64Column`, `Uint32Column`) is deprecated: the returned `[]int` costs
eight bytes per matching row. `FilterSelection(predicate) *Selection` returns
a bitmap costing rows/8 bytes regardless of how many rows match.

```go
// Before: indices is []int
indices := col.Filter(func(v string) bool { return strings.HasPrefix(v, "x") })
for _, i := range indices { use(uint32(i)) }
n := len(indices)

// After: sel is *columns.Selection (a bitmap)
sel := col.FilterSelection(func(v string) bool { return strings.HasPrefix(v, "x") })
sel.ForEach(func(i uint32) { use(i) })   // iterate in row order
n := sel.Count()                          // popcount, no iteration
ok := sel.Contains(row)                   // membership test
```

Useful `Selection` operations: `Count()`, `Contains(i)`, `ForEach(f)`,
`And(other)` to intersect two filters, and `ToIndices() []uint32` as an escape
hatch when an index slice is genuinely required (it re-pays the per-row cost —
prefer iterating). Chunked columns additionally offer structured filters
(`FilterSelectionEqual/In/Range` and `...Context` variants) which are faster
(chunk pruning, parallel scan) — prefer them over a predicate when the filter
is an equality, a value set, or a range.

## 3. TableView: index-list methods → streaming methods

Both deprecated methods materialize a `[]uint32` with one entry per passing
row. The replacements stream over the internal filter bitmap and allocate
only what they return.

| Deprecated | Replacement |
|---|---|
| `GetFilteredIndices() []uint32` | `GetFilteredRowCount() int` if you only need the count; `GetFilteredRows(columnNames, limit)` for row data; `GetFilteredRowsSorted(columnNames, sortOrder, limit)` for sorted row data |
| `GetSortedTopK(indices, sortOrder, limit) []uint32` | `GetFilteredRowsSorted(columnNames []string, sortOrder []queryspec.SortColumn, limit int) []map[string]string` |

```go
// Before: count via the index list
n := len(view.GetFilteredIndices())
// After
n := view.GetFilteredRowCount()

// Before: top 25 rows by (score desc), then fetch values per index
idx := view.GetSortedTopK(view.GetFilteredIndices(), sorts, 25)
// ... GetValue per index per column ...
// After: one call returns the rows' values directly
rows := view.GetFilteredRowsSorted([]string{"name", "score"}, sorts, 25)
```

If your code used the raw indices for something the row-map methods do not
cover (e.g. feeding another API), iterate instead of materializing where
possible; `GetFilteredIndices` has no direct raw-index replacement by design.

## 4. Grouping: `GroupIndices` and `Group.Indices` → the three narrow operations

`GroupIndices` returns full membership lists — O(rows) memory per call — and
is deprecated on every column type. The replacement is the `IGroupOps`
interface with three operations whose output is O(distinct):

```go
ops := columns.GroupOpsFor(col, view)   // works for ANY IDataColumn:
                                        // native fast path or a correct shim

// counts[code] = selected rows in that group; firsts[code] = a representative
// row (valid where counts[code] > 0). sel is a RowSet — see below.
counts, firsts := ops.GroupCounts(sel)

// Stream (code, row) pairs to your accumulator — aggregation without lists.
ops.GroupAggregates(sel, acc)           // acc implements Add(code, row uint32)

// One page of one group's rows, only when actually needed (n < 0 = all).
rows := ops.GroupMembers(sel, code, offset, n)
```

The selection argument is a `RowSet`: pass `columns.AllRows(n)` for "all
rows", a `*Selection` bitmap from filtering, or `columns.RowIndices(list)` to
bridge an explicit index list.

Semantics to be aware of (they differ from `GroupIndices`):

- Group **codes are dense integers**, stable for a fixed (column, selection)
  across the three operations — results can be combined across calls.
- Dictionary-encoded columns use their dictionary codes and may therefore
  report **zero-count groups; skip `counts[code] == 0`**.
- `GroupIndices`' map keys were implementation-specific; if your code depended
  on them, key by the group's *value* instead: read it via
  `col.GetString(firsts[code])`.

`grouping.Group.Indices` is deprecated and is already `nil` on groups produced
by TableView grouping. Use `g.Length()` (row count), `g.First`
(representative row), `g.GetValue()` (display value), and `GroupMembers` for
actual membership. Constructing your own `Group` with `Indices` set still
works — accessors fall back to it.

## 5. Server resolvers: six setters → one catalog

The six resolver callbacks on `handlers.Server` (old path: `server.Server`)
are deprecated: `SetURLResolver`, `SetAllURLsResolver`,
`SetPrimaryKeyResolver`, `SetEntityTypeDescriptionResolver`,
`SetHierarchyContextBuilder`, `SetRelatedTablesResolver`.

The replacement is **one call**: `Server.SetCatalog(*engine.Catalog)`. The
catalog carries the data the callbacks used to compute — tables and columns,
hierarchies with descriptions, entity types with URL templates — and
`web/navigation` derives all six behaviours from it.

```go
// Before: six callbacks wiring product-specific navigation
srv.SetURLResolver(myURLResolver)
srv.SetPrimaryKeyResolver(myPKResolver)
// ... four more ...

// After: build the catalog once and install it
catalog := dsManager.BuildCatalog(tables)   // datasources.Manager does this
srv.SetCatalog(catalog)
```

If you load tables through `datasources.Manager`, `BuildCatalog` assembles the
catalog from the same configuration the resolvers read (URL templates come
from the entity-type config; the primary-key precedence matches the old
resolver's). If you construct tables programmatically, build an
`engine.Catalog` literal: `TableMeta` per table, `EntityTypeMeta` with
`URLTemplate` entries (`{value}` and `{entity_type}` placeholders,
`IsDefault` for the primary link), `Hierarchy` entries in display order.

An explicitly installed callback currently *overrides* its catalog-driven
default, so you can migrate one resolver at a time and diff the output.
**If some of your resolver logic cannot be expressed as catalog data**, do not
silently keep the setter — it is deleted in CLEAN. Report the gap to the
taxinomia maintainer so the catalog can grow the missing field first.

## 6. Not deprecated, but will bite: concrete-type assertions

Loaded tables no longer contain the column types they did historically. The
load pipeline produces chunked columns and then re-encodes strings
(dictionary / arena / front-coded). Code that type-asserts concrete storage
types on loader output — `col.(*columns.StringColumn)`,
`col.(*columns.ChunkedStringColumn)` — has already stopped matching.

Migrate to the interfaces, which every representation implements:

```go
// Value access, any column:            columns.IDataColumn
v, err := col.GetString(i)
// Typed access:                        columns.IDataColumnT[T]
if tc, ok := col.(columns.IDataColumnT[string]); ok { s, _ := tc.GetValue(i) }
// Grouping:                            columns.GroupOpsFor(col, view)
// Row comparison for sorting:          col.(columns.RowComparator)
// Chunk geometry (advanced):           col.(columns.IChunkedColumn)
```

For column *implementers* (you wrote your own `IDataColumn`): nothing is
required — the interface was never widened, and `GroupOpsFor` shims grouping.
Optionally implement `IGroupOps` (note its selections are `RowSet`, not
`[]uint32`) for O(distinct) grouping performance.

## What CLEAN will delete (the checklist you are migrating away from)

- `core/query`, `core/views`, `core/rendering`, `core/server` forwarding
  packages (section 1)
- `Filter(predicate)` on the seven plain column types (section 2)
- `TableView.GetFilteredIndices`, `TableView.GetSortedTopK` (section 3)
- `GroupIndices` on all column types; `grouping.Group.Indices` (section 4)
- the six `Set*Resolver` / `SetHierarchyContextBuilder` setters (section 5)

## Final verification

1. Re-run every Step-0 search: **all must return zero hits** (section 6's
   pattern may legitimately hit code that constructs plain columns itself —
   only assertions on *loader output* matter).
2. `go build ./...` and your test suite pass.
3. Recommended: `staticcheck -checks SA1019 ./...` (or `go vet` in toolchains
   that surface deprecations) reports no deprecated taxinomia usage.
4. Behaviour check: taxinomia's own migration of its demo app was
   byte-identical under golden tests; if you have snapshot tests, run them —
   none of the replacements above change rendered output.

When your repository passes all four, report back so CLEAN can be scheduled;
after CLEAN, the deprecated names no longer exist and unmigrated code stops
compiling rather than misbehaving.
