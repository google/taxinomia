# Grouping and aggregates

## Grouping

- **GROUP** under a header groups the rows by that column: rows with the
  same value become one group, shown once with its size in brackets,
  `[rows]`, or `[subgroups/rows]` when it is grouped further.
- **GROUP** on another column nests it: each group is grouped again by
  that column, one level deeper. Grouped columns move to the left in
  level order; drag a grouped header onto another to change the order of
  the levels.
- **UNGROUP** turns a grouped column back into a plain column.
- The **+** strip on the right of a group cell lists that group's own
  rows beneath it; **−** closes it again.
- **F** next to a group value keeps only that group's rows and ungroups
  the column (see Filtering).


## Keeping only some groups

Under a grouped column's buttons, the **keep groups where...** field keeps
only the groups whose aggregates satisfy a condition. The rows of the other
groups leave the view, so the counts and the aggregates above describe the
kept groups only; a group whose subgroups are all dropped goes too. Enter
applies it, Esc clears it.

A condition compares aggregates of the group's rows, with `and`, `or`,
`not` and arithmetic:

| You write | The group's |
|-----------|-------------|
| `count()` | number of rows |
| `subgroups()` | number of subgroups, when it is grouped further |
| `sum(col)`, `avg(col)`, `stddev(col)` | sum, average, spread of a number column |
| `min(col)`, `max(col)` | smallest, largest (numbers, text, dates) |
| `unique(col)` | number of distinct values of a text column |
| `true(col)`, `false(col)`, `ratio(col)` | yes/no column: how many true, how many false, share true (0 to 1) |
| `any(col)`, `all(col)` | yes/no column: is any value true, are all true |
| `span(col)` | time from the earliest to the latest date |

Examples:

- `count() >= 50`
- `sum(amount) > 10000 and avg(amount) < 200`
- `max(cpu_cores) - min(cpu_cores) > 32`
- `any(is_dead)` or `not all(is_active)`
- `max(order_date) > date("2024-01-01")`
- `span(order_date) > duration("30d")`

The column in an aggregate can be any column of the table, shown or not.
A value filter on the same column (its filter box) applies first, to the
rows; the condition then applies to the groups.

## Aggregates

On a grouped table, the other columns get aggregate buttons. Click one to
switch that aggregate on or off for every group: it shows in each group's
row and, for groups that are grouped further, in a summary inside the
group cell. Which aggregates a column offers depends on its type; hover a
button for its name.

| Column type | Aggregates |
|-------------|------------|
| Numbers | # rows, Σ sum, μ average, σ spread (standard deviation), ↓ smallest, ↑ largest |
| Text | # rows, ◇ distinct values, ↓ first and ↑ last alphabetically |
| Dates and times | # rows, ↓ earliest, ↑ latest, μ average, σ spread, Δ time span |
| Yes/no | # rows, ✓ how many true, ✗ how many false, % share true |

Rows whose value could not be computed are left out of the aggregates and
counted: `failed 2` next to them means two rows were left out.

## Sorting

- The table is always sorted by its columns, left to right: the first
  column decides, the next breaks ties, and so on. Each column sorts
  ascending unless flipped with its ▲/▼ button.
- To change which column sorts first, drag its header left or right.
- Groups are sorted by their value. The **⟳** button of a grouped column
  sorts its groups by a number instead: each click steps to the next
  choice, rows per group, subgroups per group (except on the last grouped
  column, which has none), then every aggregate
  switched on in the other columns. The number in use is shown in bold
  (an aggregate also in blue); the ▲/▼ button then flips between
  smallest and largest first. Keep clicking ⟳ to sort by value again.
- Rows whose value could not be computed sort as the largest value: last
  when ascending, first when descending.
