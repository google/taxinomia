# Column Ordering Rules

## Overview

Columns in the table view are automatically reordered based on their state to provide a logical and consistent display order.

## Ordering Priority

Columns are displayed in the following order from left to right:

1. **Filtered Columns** (leftmost)
   - Columns with active filters that are NOT grouped appear first
   - Filter parameters: `filter:columnName=value`
   - Example: `filter:status=active`

2. **Grouped Columns** (middle)
   - Columns used for grouping appear after filtered-only columns
   - Grouped parameter: `grouped=col1,col2`
   - **Important**: Grouped columns appear in the order specified by the `grouped` parameter (the grouping hierarchy), not their original position in `columns`
   - Note: Columns that are both filtered and grouped stay in the grouped section
   - This allows multi-select filtering while preserving the grouping structure

3. **Other Columns** (rightmost)
   - All remaining columns appear last
   - Maintain their relative order from the URL

## Examples

### Example 1: Simple Filter
**URL**: `/table?table=orders&columns=status,region,category,amount&filter:region=north`

**Result**: `region, status, category, amount`
- `region` moves to first position (filtered)
- Others maintain their order

### Example 2: Multiple Filters
**URL**: `/table?table=orders&columns=status,region,category,amount&filter:region=north&filter:amount=100`

**Result**: `region, amount, status, category`
- `region` and `amount` move to first positions (filtered)
- Their relative order is preserved

### Example 3: Filtered + Grouped
**URL**: `/table?table=orders&columns=status,region,category,amount&grouped=status&filter:amount=100`

**Result**: `amount, status, region, category`
- `amount` is first (filtered)
- `status` is second (grouped)
- `region` and `category` remain at the end

### Example 4: Filtered Column Stays in Grouped Position
**URL**: `/table?table=orders&columns=status,region,category,amount&grouped=status,category&filter:status=active`

**Result**:
- Columns: `status, category, region, amount`
- Grouped: `status, category` (both remain grouped)
- `status` stays in the grouped section (not moved to filtered section)
- This allows multi-select filtering while maintaining column position and grouping structure

### Example 5: Complex Scenario
**URL**: `/table?table=test&columns=a,b,c,d,e,f&grouped=c,d&filter:e=val1&filter:f=val2`

**Result**: `e, f, c, d, a, b`
- `e, f` are filtered (leftmost)
- `c, d` are grouped (middle)
- `a, b` are regular columns (rightmost)

## Implementation

The reordering happens automatically in `NewQuery()` when parsing URL parameters. The `Columns` field is reordered in place to maintain the logical display order:

```go
// Extract filters
for key, values := range q {
    if strings.HasPrefix(key, "filter:") && len(values) > 0 {
        columnName := strings.TrimPrefix(key, "filter:")
        state.Filters[columnName] = values[0]
    }
}

// Reorder Columns based on filtered/grouped status
state.reorderColumns()
```

**Important**: The `Columns` field is modified directly to reflect the display order (filtered → grouped → others). This means:
- The URL reflects the actual displayed column order
- URLs automatically update to show the logical ordering when filters/grouping change
- Consistent with how grouping already works in the system

## Benefits

1. **Filtered columns are immediately visible** - Users can see which columns they're filtering on without scrolling
2. **Grouped columns stay prominent** - Grouping hierarchy is clear and consistent
3. **Automatic reordering** - No manual column dragging needed when applying filters
4. **Clear visual hierarchy** - The column order reflects the operation priority (filter > group > display)

## Filter and Grouping Interaction

### When a Filter is Applied to a Grouped Column
- The column is **automatically removed** from the grouped list
- The column moves to the filtered section (leftmost)
- Grouping behavior is disabled for that column
- The grouping toggle button is disabled in the UI

### When a Filter is Removed
- The column moves from the filtered section back to its appropriate position
- If the column was previously grouped (before filtering), it does NOT automatically return to the grouped state
- If the column is neither filtered nor grouped, it moves to the rightmost section
- Relative order among columns in the same category is preserved

### UI Behavior
- Filtered columns show a **disabled grouping button** (⊞ grayed out)
- Tooltip indicates: "Grouping is disabled for filtered columns"
- The button cannot be clicked while a filter is active
