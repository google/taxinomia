# Filtering

Every column has a filter box under its header. Type in it and press
Enter to apply; Esc, or the × next to it, clears it. Filters on several
columns combine: a row is kept only when it matches all of them.

While you type, the **Syntax** button beside the box (or F1) opens this
help under the row, with a line saying what your filter will do; Esc or
a click elsewhere closes it. A
computed column's formula has the same button: it shows the expression
syntax and the table's columns, which insert their name when clicked.

## What you can type

| You type | Keeps the rows whose value |
|----------|----------------------------|
| `east` | contains "east" anywhere, in any case: East, Northeast, EAST |
| `"East"` | is exactly East (upper and lower case count) |
| `North\|South` | is exactly North or exactly South |
| `10..100` | is from 10 up to, not including, 100 (number and date columns) |
| `<10`, `<=10`, `>100`, `>=100` | is below, up to, above, or from the number |
| `2024-01..2024-07`, `>=2025` | on a date column: a year, month or day stands for its whole period |
| `east \|\| west \|\|` | groups the rows instead (see Groups from a filter) |
| `[error]` | could not be computed (a computed column whose expression fails on the row) |
| `[unmatched]` | has no match in the joined table (a joined column) |

- A plain word looks for the text anywhere in the value and ignores
  upper and lower case.
- Quotes ask for the whole value, exactly.
- A vertical bar lists several exact values: any of them is kept.
- `[error]` and `[unmatched]` match only when typed in full; a part of
  them, such as `e`, never matches those rows.

## Ranges

On number and date columns, a filter can compare instead of matching text:
`<10`, `<=10`, `>100`, `>=100`, or a range `10..100`, which includes 10 and
stops before 100, so `..10`, `10..100` and `100..` fit together without
overlap. On a date column a year, a month or a day stands for its whole
period: `2024` is the year, `<=2024-06` is up to the end of June,
`2024-01..2024-07` is January to June. Ranges combine with `|`:
`<10|>100`. On a text column the same characters are plain text.

## Groups from a filter

Two bars, `||`, turn a filter into groups: each part is written exactly
like a filter, and each becomes one group of the column, instead of one
group per value.

- `east || west` groups the rows containing east and those containing
  west; rows in neither leave the view.
- A trailing `||` keeps the rest as a last group, **[other]**:
  `"North"|"South" || west ||` makes three groups.
- A row goes to the first group it matches; groups keep the order typed.
- Ranges make bins: `..10 || 10..100 || 100..`.
- Typing groups into a column that is not grouped groups it; UNGROUP
  removes them. **F** on a group filters to its part; [other] has none.

## Filtering from a grouped table

- **F** next to a group value keeps only that group's rows: the value and
  those of the groups around it become filters. While levels are grouped
  below it, its levels stay grouped and show once; on the last grouped
  level they are ungrouped, to show the rows.
- The tick-box button next to a grouped column's filter box picks
  several groups at once: click it, tick the groups to keep, then click
  it again. The ticked values become one filter (several exact values).

## How a filtered column looks

- Its header turns pale amber, and it moves to the left: filtered
  columns come first, then grouped columns, then the others.
- The counts row under the headers shows how many rows the filters keep
  out of all rows.
- In the column pane, a filtered column carries a funnel mark.
