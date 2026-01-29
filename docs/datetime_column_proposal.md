# Datetime Column Support - Implementation Summary

## Overview

Native datetime support has been added to Taxinomia columns, enabling efficient storage, filtering, grouping, and display of temporal data.

## What Was Implemented

### 1. DatetimeColumn (`core/columns/datetime_column.go`)

Storage using Go's native `time.Time`:
```go
type DatetimeColumn struct {
    columnDef     *ColumnDef
    data          []time.Time
    isKey         bool
    valueIndex    map[int64]int
    displayFormat string
    location      *time.Location
}
```

**Key Features:**
- Stores datetime values as `time.Time` (internally UTC)
- Multiple append methods: `Append(time.Time)`, `AppendUnix(int64)`, `AppendString(string)`
- Configurable display format via `SetDisplayFormat()`
- Full interface implementation: `IDataColumn`, `GroupIndices`, `CreateJoinedColumn`

**Supported Input Formats:**
- ISO 8601: `2006-01-02T15:04:05Z07:00`
- Date only: `2006-01-02`
- Date and time: `2006-01-02 15:04:05`
- Unix timestamps (auto-detected: seconds, milliseconds, nanoseconds)

### 2. Epoch-Based Extraction Functions

Functions on DatetimeColumn:
```go
Seconds(i uint32) (int64, error)   // Unix seconds
Minutes(i uint32) (int64, error)   // seconds / 60
Hours(i uint32) (int64, error)     // seconds / 3600
Days(i uint32) (int64, error)      // seconds / 86400
Weeks(i uint32) (int64, error)     // days / 7
Months(i uint32) (int64, error)    // Exact: (year-1970)*12 + (month-1)
Quarters(i uint32) (int64, error)  // Exact: (year-1970)*4 + (month-1)/3
Years(i uint32) (int64, error)     // year - 1970
```

### 3. Expression Evaluator Functions (`core/expr/eval.go`)

Added datetime functions for computed columns:
```
seconds(dt)   → Unix seconds
minutes(dt)   → minutes since epoch
hours(dt)     → hours since epoch
days(dt)      → days since epoch
weeks(dt)     → weeks since epoch
months(dt)    → exact months since epoch
quarters(dt)  → exact quarters since epoch
years(dt)     → years since epoch (1970)
```

**Usage in expressions:**
```
months(order_date)                 // Group by month
quarters(created_at)               // Group by quarter
years(created_at)                  // Group by year
```

### 4. JoinedDatetimeColumn (`core/columns/joined_columns.go`)

Supports datetime columns in join operations.

### 5. ComputedDatetimeColumn (`core/columns/computed_columns.go`)

Supports computed datetime columns with lazy evaluation.

### 6. Protoloader Timestamp Support (`core/protoloader/loader.go`)

- Detects `google.protobuf.Timestamp` fields
- Automatically formats as ISO 8601 strings
- Treats Timestamp messages as scalar fields (not nested hierarchies)

### 7. Demo Data (`demo/demo_data.go`, `demo/server.go`)

New "events" table demonstrating datetime columns:
- `event_id` (uint32)
- `event_name` (string)
- `event_type` (string)
- `created_at` (datetime)
- `scheduled_at` (datetime)
- `status` (string)

50 sample events spanning 2023-2024.

## Usage Examples

### Creating a DatetimeColumn programmatically:
```go
colDef := columns.NewColumnDef("order_date", "Order Date", "")
col := columns.NewDatetimeColumn(colDef)

col.AppendString("2024-01-15")
col.AppendString("2024-01-16T14:30:00Z")
col.Append(time.Now())

col.FinalizeColumn()
table.AddColumn(col)
```

### Using epoch functions in expressions:
```go
// Create computed column for month grouping
compiled, _ := expr.Compile("months(created_at)")
// ... bind and evaluate
```

### Accessing epoch values from DatetimeColumn:
```go
col := table.GetColumn("created_at").(*columns.DatetimeColumn)
months, _ := col.Months(0)  // exact months since epoch
quarters, _ := col.Quarters(0)  // exact quarters since epoch
```

## Files Created/Modified

### New Files:
1. `core/columns/datetime_column.go` - DatetimeColumn implementation
2. `core/columns/datetime_column_test.go` - Unit tests

### Modified Files:
1. `core/columns/joined_columns.go` - Added JoinedDatetimeColumn
2. `core/columns/computed_columns.go` - Added ComputedDatetimeColumn
3. `core/expr/eval.go` - Added datetime epoch functions
4. `core/protoloader/loader.go` - Timestamp field detection
5. `demo/demo_data.go` - CreateEventsTable function
6. `demo/server.go` - Register events table

## Design Decisions

1. **Native `time.Time` storage**: Simpler API, built-in timezone handling. Memory overhead acceptable for typical use cases.

2. **Exact month/quarter calculations**: `months()` and `quarters()` use calendar-based math `(year-1970)*12 + (month-1)` rather than approximations like days/30.

3. **Grouping via computed columns**: Instead of special grouping modes, use epoch functions to create computed columns that can be grouped normally. Example: `months(order_date)` creates an int column groupable by standard grouping.

4. **Filtering deferred**: Range filters (`>`, `<`, `..`) not yet implemented. Can be added to `ApplyFilters` in table_view.go when needed.

## Future Enhancements (Deferred)

1. **Duration type**: For date arithmetic (`date_diff`, `date_add`)
2. **Range filtering**: `filter:created_at=>2024-01-01`, `filter:created_at=2024-01..2024-03`
3. **Relative time filters**: `last 7 days`, `this month`
4. **Date picker UI**: Replace text input with date picker for datetime columns
