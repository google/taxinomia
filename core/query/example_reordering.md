# Column Reordering for Grouping

This document demonstrates how column ordering works when grouping is toggled.

## Rules

1. **Grouped columns always appear first** in the table, in their grouping order
2. **When a column is grouped**, it moves to the appropriate position based on grouping order
3. **When a column is ungrouped**, it moves to the right of the last grouped column
4. **Ungrouped columns maintain their relative order**

## Examples

### Example 1: Group a middle column

**Initial state:**
- Columns: `status`, `region`, `category`, `amount`
- Grouped: none

**User clicks group toggle on "region":**
- Columns: `region`, `status`, `category`, `amount`
- Grouped: `region`

Result: `region` moves to first position

### Example 2: Group multiple columns

**Initial state:**
- Columns: `status`, `region`, `category`, `amount`
- Grouped: none

**User clicks group toggle on "status":**
- Columns: `status`, `region`, `category`, `amount` (no change, already first)
- Grouped: `status`

**User then clicks group toggle on "category":**
- Columns: `status`, `category`, `region`, `amount`
- Grouped: `status`, `category`

Result: Grouped columns (`status`, `category`) appear first in grouping order

### Example 3: Ungroup a middle grouped column

**Initial state:**
- Columns: `status`, `region`, `category`, `amount`
- Grouped: `status`, `region`, `category`

**User clicks group toggle on "region" (to ungroup it):**
- Columns: `status`, `category`, `region`, `amount`
- Grouped: `status`, `category`

Result: `region` moves to the right of the last grouped column (`category`), before `amount`

## Implementation

The column reordering is handled automatically by the `WithGroupedColumnToggled()` method in [query.go](query.go#L318-L343), which calls `reorderColumnsForGrouping()` to ensure proper ordering.

The logic:
1. Identify which columns are grouped
2. Separate columns into grouped and ungrouped lists
3. Sort grouped columns by their grouping order
4. Concatenate: grouped columns first, then ungrouped columns
