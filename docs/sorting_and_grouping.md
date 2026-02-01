# Sorting and Grouping Reference

Taxinomia provides powerful sorting and grouping capabilities for analyzing tabular data. This document describes how to sort columns, group data hierarchically, and sort groups by aggregate values.

## Overview

| Feature | Description | Use Case |
|---------|-------------|----------|
| Column Sort | Sort rows by column values | Order data by date, amount, name |
| Multi-column Sort | Sort by multiple columns with priority | Sort by category, then by date |
| Grouping | Hierarchically group rows by column values | Group orders by status, then by region |
| Aggregate Sort | Sort groups by computed aggregate values | Show highest-revenue regions first |

## Column Sorting

### Basic Sorting

Click the sort button (⇅) in a column header to toggle sorting:
- **First click**: Sort ascending (▲)
- **Second click**: Sort descending (▼)
- **Third click**: Remove sort

### Multi-column Sort

When multiple columns are sorted, a priority number appears next to each sort indicator:
- **1** = Primary sort column
- **2** = Secondary sort (ties broken by this column)
- And so on...

Sort priority is determined by the order in which columns were sorted.

### URL Parameters

Sort order is encoded in the `sort` URL parameter:
```
?sort=+column1,-column2
```
- `+` prefix = ascending
- `-` prefix = descending

## Grouping

### Basic Grouping

Click the group button (⊞) on a column to group rows by that column's values. Each unique value creates a group showing:

```
value [rows]
```

Where `rows` is the number of data rows in that group.

### Hierarchical Grouping

Group by multiple columns to create a hierarchy. For nested groups, the display shows:

```
value [subgroups/rows]
```

Where:
- `subgroups` = number of child groups
- `rows` = total number of data rows (across all descendants)

### URL Parameters

Grouped columns are specified with `grouped:` prefix:
```
?grouped:status&grouped:region
```

The order determines grouping hierarchy (first = outermost).

## Aggregates

When grouping is active, aggregates are computed for leaf columns (non-grouped visible columns).

### Available Aggregates by Column Type

#### Numeric Columns
| Symbol | Name | Description |
|--------|------|-------------|
| Σ | Sum | Total of all values |
| μ | Average | Arithmetic mean |
| σ | Std Dev | Standard deviation |
| ↓ | Min | Minimum value |
| ↑ | Max | Maximum value |
| # | Count | Number of values |

#### String Columns
| Symbol | Name | Description |
|--------|------|-------------|
| ∪ | Unique | Count of distinct values |
| # | Count | Number of values |

#### Boolean Columns
| Symbol | Name | Description |
|--------|------|-------------|
| ✓ | True | Count of true values |
| ✗ | False | Count of false values |
| % | Ratio | Percentage of true values |
| # | Count | Total count |

#### Datetime Columns
| Symbol | Name | Description |
|--------|------|-------------|
| ↓ | Min | Earliest datetime |
| ↑ | Max | Latest datetime |
| μ | Average | Mean datetime |
| σ | Std Dev | Standard deviation of times |
| Δ | Span | Time span (max - min) |
| # | Count | Number of values |

### Enabling/Disabling Aggregates

Toggle individual aggregates using the aggregate buttons in the column header when grouping is active. Enabled aggregates appear in each group's cell.

### URL Parameters

Aggregate settings are encoded with `agg:` prefix:
```
?agg:amount=sum,avg&agg:name=unique
```

## Sorting Groups by Aggregates

When data is grouped, you can sort groups by their aggregate values instead of the grouped column's natural value order.

### Sort Options

Click the cycle button (⟳) next to a grouped column to cycle through sort options:

1. **Row Count** (≡) - Sort by total number of rows in each group
2. **Subgroup Count** (⊞) - Sort by number of direct child groups
3. **Leaf Column Aggregates** - Sort by any enabled aggregate on leaf columns

### Sort Direction

When an aggregate sort is active, the sort direction button (▲/▼) toggles between ascending and descending order for that aggregate.

### Visual Indicators

The currently sorted value is displayed in **bold**:
- When sorting by row count → row count number is bold
- When sorting by subgroup count → subgroup count number is bold
- When sorting by grouped value → value text is bold
- When sorting by aggregate → aggregate value is highlighted

### URL Parameters

Group aggregate sort is encoded with `groupsort:` prefix:
```
?groupsort:status=+amount:sum
```

Format: `groupsort:groupedColumn=±leafColumn:aggregateType`

- `+` = ascending, `-` = descending
- For row count: `groupsort:status=+:rows`
- For subgroup count: `groupsort:status=-:subgroups`

### Examples

**Sort regions by total revenue (descending):**
```
?grouped:region&agg:amount=sum&groupsort:region=-amount:sum
```

**Sort status by row count (ascending):**
```
?grouped:status&groupsort:status=+:rows
```

**Sort categories by number of unique products:**
```
?grouped:category&agg:product=unique&groupsort:category=-product:unique
```

## Interaction Between Sorting and Grouping

### Column Value Sort vs Aggregate Sort

- **Column value sort**: Groups are ordered by their grouping value (e.g., alphabetically by status)
- **Aggregate sort**: Groups are ordered by a computed metric (e.g., by total amount)

Only one can be active at a time per grouped column. Enabling aggregate sort disables column value sort, and vice versa.

### Leaf Column Sorting

When grouping is active, sorting is disabled for leaf columns (non-grouped visible columns) because rows are already organized into groups. The sort controls for leaf columns appear grayed out.

### Aggregate Removal

If an aggregate used for sorting is disabled, the aggregate sort is automatically removed for any grouped columns that were sorting by that aggregate.

## Performance Considerations

- Grouping computation is cached and only recomputed when grouping columns, filters, or sort direction changes
- Aggregates are computed bottom-up: leaf groups compute from data, parent groups combine children
- Large datasets with many unique values may have slower grouping performance
