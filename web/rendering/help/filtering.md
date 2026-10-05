# Filtering

Every column has a filter box under its header. Type in it and press
Enter to apply; Esc, or the × next to it, clears it. Filters on several
columns combine: a row is kept only when it matches all of them.

While you type, the **Syntax** button beside the box (or F1) opens this
help under the row, with a line saying what your filter will do. A
computed column's formula has the same button: it shows the expression
syntax and the table's columns, which insert their name when clicked.

## What you can type

| You type | Keeps the rows whose value |
|----------|----------------------------|
| `east` | contains "east" anywhere, in any case: East, Northeast, EAST |
| `"East"` | is exactly East (upper and lower case count) |
| `North\|South` | is exactly North or exactly South |
| `[error]` | could not be computed (a computed column whose expression fails on the row) |
| `[unmatched]` | has no match in the joined table (a joined column) |

- A plain word looks for the text anywhere in the value and ignores
  upper and lower case.
- Quotes ask for the whole value, exactly.
- A vertical bar lists several exact values: any of them is kept.
- `[error]` and `[unmatched]` match only when typed in full; a part of
  them, such as `e`, never matches those rows.

## Filtering from a grouped table

- **F** next to a group value keeps only that group's rows and ungroups
  the column, to look inside the group.
- The tick-box button next to a grouped column's filter box picks
  several groups at once: click it, tick the groups to keep, then click
  it again. The ticked values become one filter (several exact values).

## How a filtered column looks

- Its header turns pale amber, and it moves to the left: filtered
  columns come first, then grouped columns, then the others.
- The counts row under the headers shows how many rows the filters keep
  out of all rows.
- In the column pane, a filtered column carries a funnel mark.
