# Column Types Reference

Taxinomia supports several column types for storing and processing data. This document describes each type, its storage characteristics, and supported operations.

## Overview

| Type | Description | Storage | Example Values |
|------|-------------|---------|----------------|
| `string` | Text values | Interned strings | `"hello"`, `"New York"` |
| `uint32` | Unsigned integers | 32-bit unsigned | `0`, `42`, `4294967295` |
| `bool` | Boolean values | Native boolean | `True`, `False` |
| `datetime` | Date and time | Unix nanoseconds | `2024-01-15 10:30:00` |
| `duration` | Time duration | Nanoseconds | `2h30m`, `3d4h15m` |

## String Columns

String columns store text values. Values are interned for memory efficiency when the same string appears multiple times.

### Features
- Efficient storage through string interning
- Full Unicode support
- Can serve as key columns for joins when values are unique

### Display
String values are displayed as-is.

## Uint32 Columns

Unsigned 32-bit integer columns store non-negative whole numbers.

### Value Range
- Minimum: `0`
- Maximum: `4,294,967,295` (2^32 - 1)

### Features
- Fixed 4-byte storage per value
- Efficient comparison and arithmetic
- Can serve as key columns for joins when values are unique

### Display
Integer values are displayed as decimal numbers without separators.

## Bool Columns

Boolean columns store true/false values.

### Parsing
When importing from CSV, the following values are recognized (case-insensitive):

| True | False |
|------|-------|
| `true` | `false` |
| `1` | `0` |
| `yes` | `no` |
| `t` | `f` |
| `y` | `n` |
| | (empty string) |

### Display
- `True` for true values
- `False` for false values

### Features
- Memory-efficient storage
- Grouping produces two groups: false (key 0) and true (key 1)
- Statistics: `CountTrue()`, `CountFalse()`

## Datetime Columns

Datetime columns store date and time values with nanosecond precision.

### Parsing
When importing from CSV, multiple formats are automatically detected:

| Format | Example |
|--------|---------|
| RFC3339 with nanoseconds | `2024-01-15T10:30:00.123456789Z` |
| RFC3339 | `2024-01-15T10:30:00Z` |
| ISO without timezone | `2024-01-15T10:30:00` |
| Space separator | `2024-01-15 10:30:00` |
| Date only | `2024-01-15` |
| YYYY/MM/DD | `2024/01/15` |
| DD-Mon-YYYY | `15-Jan-2024` |
| Natural format | `Jan 15, 2024` |
| Full month | `January 15, 2024` |
| With milliseconds | `2024-01-15T10:30:00.000` |

Unix timestamps are also supported:
- **Seconds**: values up to ~3×10¹¹
- **Milliseconds**: values from ~1×10¹¹ to ~1×10¹⁶
- **Nanoseconds**: values > 1×10¹⁶

### Display
Default format: `2006-01-02 15:04:05` (Go format)

Configurable formats:
- `DatetimeFormatISO`: `2006-01-02T15:04:05Z07:00`
- `DatetimeFormatDate`: `2006-01-02`
- `DatetimeFormatDateTime`: `2006-01-02 15:04:05`
- `DatetimeFormatTime`: `15:04:05`

### Expression Functions
See [Expression Language](expression_language.md#datetime-functions) for datetime manipulation functions.

## Duration Columns

Duration columns store time durations with nanosecond precision.

### Parsing
When importing from CSV, Go-style duration strings are supported with an extension for days:

| Component | Unit | Example |
|-----------|------|---------|
| Days | `d` | `3d` |
| Hours | `h` | `2h` |
| Minutes | `m` | `30m` |
| Seconds | `s` | `45s` |
| Milliseconds | `ms` | `500ms` |
| Microseconds | `us` or `µs` | `100us` |
| Nanoseconds | `ns` | `1000ns` |

Combined examples:
- `2h30m` - 2 hours 30 minutes
- `3d4h15m` - 3 days 4 hours 15 minutes
- `-1h30m` - negative 1 hour 30 minutes
- `500ms` - 500 milliseconds

### Display Formats

**Compact format** (default):
- `0s` for zero
- `2h30m` for 2 hours 30 minutes
- `3d4h15m` for 3 days 4 hours 15 minutes

**Verbose format**:
- `0 seconds` for zero
- `2 hours 30 minutes` for 2 hours 30 minutes
- `3 days 4 hours 15 minutes` for 3 days 4 hours 15 minutes

### Expression Functions
See [Expression Language](expression_language.md#duration-functions) for duration manipulation functions.

## Proto Configuration

Column types can be explicitly specified in table source proto configuration:

```protobuf
enum ColumnType {
  COLUMN_TYPE_AUTO = 0;    // Auto-detect from data
  COLUMN_TYPE_STRING = 1;  // Force string type
  COLUMN_TYPE_UINT32 = 2;  // Force uint32 type
  COLUMN_TYPE_BOOL = 3;    // Force bool type
}
```

Example textproto configuration:

```textproto
tables {
  name: "orders"
  csv_file: "orders.csv"
  columns {
    name: "order_id"
    type: COLUMN_TYPE_UINT32
  }
  columns {
    name: "customer_name"
    type: COLUMN_TYPE_STRING
  }
  columns {
    name: "is_priority"
    type: COLUMN_TYPE_BOOL
  }
}
```

## Key Columns and Joins

Any column type can serve as a key column when all values are unique. Key columns enable:

1. **Reverse lookup**: Find row index by value
2. **Table joins**: Connect tables through matching entity types

To use a column for joins:
1. Set the `entity_type` in the column configuration
2. Ensure all values are unique
3. Use matching `entity_type` across tables

Example:

```textproto
# In orders.csv config
columns {
  name: "region_id"
  entity_type: "region"
}

# In regions.csv config
columns {
  name: "id"
  entity_type: "region"
}
```

## Computed Columns

Computed columns calculate values at query time using expressions. The expression's return type determines the column type:

| Expression Result | Column Type |
|-------------------|-------------|
| Number | `ComputedUint32Column` |
| String | `ComputedStringColumn` |
| Boolean | `ComputedBoolColumn` |
| Datetime | `ComputedDatetimeColumn` |
| Duration | `ComputedDurationColumn` |

See [Expression Language](expression_language.md) for expression syntax and functions.
