# Filter Performance Benchmarks

## Summary

The optimized ApplyFilters implementation processes column data column-by-column with filter type determined once per column, achieving excellent throughput across different scenarios.

**Key Results:**
- **Peak Performance**: 21.77 million rows/second (exact match on 1M rows)
- **Typical String Filtering**: 5-8 million rows/second (substring match)
- **Multiple Filters**: 3-4 million rows/second (3 filters applied)
- **Uint32 Filtering**: 5-6 million rows/second

## Test Environment
- **CPU**: Intel Core i7-8550U @ 1.80GHz (4 cores, 8 threads)
- **OS**: Windows
- **Go**: amd64 architecture

## String Column Filtering

### Substring Match (Case-Insensitive)
Filters values using case-insensitive substring matching (e.g., "active" matches "Active", "Inactive").

| Rows      | Time/Op (ms) | Throughput (Mrows/sec) | Memory/Op | Allocs/Op |
|-----------|--------------|------------------------|-----------|-----------|
| 1,000     | 0.11         | 8.70                   | 9 KB      | 1,001     |
| 10,000    | 1.13         | 8.82                   | 90 KB     | 10,001    |
| 100,000   | 12.14        | 8.24                   | 906 KB    | 100,001   |
| 1,000,000 | 187.27       | 5.34                   | 9 MB      | 1,000,001 |

**Analysis**: Consistent 5-9 Mrows/sec throughput. Memory scales linearly with row count due to string allocations for case conversion.

### Exact Match (Case-Sensitive)
Filters using case-sensitive exact matching (e.g., "Active" matches only "Active").

| Rows      | Time/Op (ms) | Throughput (Mrows/sec) | Memory/Op | Allocs/Op |
|-----------|--------------|------------------------|-----------|-----------|
| 1,000     | 0.05         | 20.82                  | 1 KB      | 1         |
| 10,000    | 0.54         | 18.36                  | 10 KB     | 1         |
| 100,000   | 5.96         | 16.78                  | 106 KB    | 1         |
| 1,000,000 | 45.93        | **21.77**              | 1 MB      | 1         |

**Analysis**: 2-4x faster than substring match! Minimal allocations (only filter mask). Avoids string case conversion overhead.

### Multiple Filters (3 columns)
Three filters applied sequentially (status, category, region).

| Rows      | Time/Op (ms) | Throughput (Mrows/sec) | Memory/Op | Allocs/Op |
|-----------|--------------|------------------------|-----------|-----------|
| 1,000     | 0.25         | 3.96                   | 14 KB     | 1,791     |
| 10,000    | 2.63         | 3.81                   | 146 KB    | 17,964    |
| 100,000   | 24.04        | 4.16                   | 1.4 MB    | 176,001   |
| 1,000,000 | 241.13       | 4.15                   | 14 MB     | 1,722,223 |

**Analysis**: 3-4 Mrows/sec with multiple filters. Progressive filtering allows early exit for rows that fail first filter.

## Uint32 Column Filtering

Numeric columns require string conversion for filtering (GetString() interface).

### Exact Match
| Rows      | Time/Op (ms) | Throughput (Mrows/sec) | Memory/Op | Allocs/Op |
|-----------|--------------|------------------------|-----------|-----------|
| 1,000     | 0.16         | 6.24                   | 7.6 KB    | 1,737     |
| 10,000    | 1.63         | 6.14                   | 76 KB     | 17,361    |
| 100,000   | 16.06        | 6.23                   | 770 KB    | 173,602   |
| 1,000,000 | 160.59       | 6.23                   | 7.6 MB    | 1,736,008 |

**Analysis**: Very consistent 6.2 Mrows/sec across all sizes. String conversion from uint32 dominates performance.

### Substring Match
| Rows      | Time/Op (ms) | Throughput (Mrows/sec) |
|-----------|--------------|------------------------|
| 1,000     | 0.19         | 5.36                   |
| 10,000    | 1.74         | 5.76                   |
| 100,000   | 27.88        | 3.59                   |
| 1,000,000 | 170.42       | 5.87                   |

**Analysis**: Similar to exact match, as both require string conversion from uint32.

## Mixed Column Filtering

String + Uint32 column filtering (2 filters: status + amount).

| Rows      | Time/Op (ms) | Throughput (Mrows/sec) | Memory/Op | Allocs/Op |
|-----------|--------------|------------------------|-----------|-----------|
| 1,000     | 0.26         | 3.87                   | 12 KB     | 2,106     |
| 10,000    | 2.53         | 3.96                   | 127 KB    | 21,080    |
| 100,000   | 25.51        | 3.92                   | 1.2 MB    | 209,447   |
| 1,000,000 | 264.83       | 3.78                   | 12 MB     | 2,052,505 |

**Analysis**: Consistent ~4 Mrows/sec. Combined overhead of string operations and numeric conversion.

## Filter Selectivity Impact

Tests on 100,000 rows with different selectivity levels (how many rows pass the filter).

| Selectivity | Match Rate | Time/Op (ms) | Throughput (Mrows/sec) |
|-------------|------------|--------------|------------------------|
| High (rare) | 1%         | 6.88         | 14.54                  |
| Moderate    | 50%        | 6.44         | **15.53**              |
| Low (common)| 90%        | 6.64         | 15.07                  |

**Analysis**: Minimal impact from selectivity! The `if !mask[i] { continue }` optimization is effective at skipping already-filtered rows, but since we're using exact match (minimal allocations), the overall performance is dominated by column access time rather than filter evaluation. Throughput is very consistent at ~15 Mrows/sec regardless of selectivity.

## Key Optimizations

1. **Filter Type Determined Once Per Column**: The `isExactMatch` check happens once per column, not once per row. For 1M rows, this eliminates 999,999 redundant condition checks.

2. **Column-by-Column Processing**: Processes all rows for one column before moving to the next, improving cache locality.

3. **Early Exit on Filtered Rows**: `if !mask[i] { continue }` skips rows that already failed previous filters.

4. **Minimal Allocations for Exact Match**: Only allocates the filter mask (1 bit per row), avoiding string allocations.

5. **Progressive Narrowing**: Each filter narrows the result set, reducing work for subsequent filters.

## Memory Efficiency

- **String substring match**: ~9 bytes/row (string allocations for case conversion)
- **String exact match**: ~1 byte/row (only filter mask)
- **Uint32**: ~7.6 bytes/row (string conversion from numbers)
- **Multiple filters**: Cumulative memory across all filters

## Real-World Performance Examples

### Example 1: Single String Filter on 1M Rows
```go
filters := map[string]string{"status": "active"}
tableView.ApplyFilters(filters) // ~187ms, 5.34 Mrows/sec
```

### Example 2: Exact Match on 1M Rows
```go
filters := map[string]string{"status": "\"Active\""}
tableView.ApplyFilters(filters) // ~46ms, 21.77 Mrows/sec ⚡
```

### Example 3: Three Filters on 1M Rows
```go
filters := map[string]string{
    "status": "active",
    "category": "electronics",
    "region": "north",
}
tableView.ApplyFilters(filters) // ~241ms, 4.15 Mrows/sec
```

## Recommendations

1. **Use Exact Match When Possible**: 2-4x faster than substring match, use quoted values ("\"value\"") when you know the exact string.

2. **Order Filters by Selectivity**: Place highly selective filters first to maximize early exit benefits (though current results show minimal impact).

3. **String Columns Perform Best**: String columns with exact match achieve 21+ Mrows/sec.

4. **Uint32 Columns**: Consistent 6 Mrows/sec due to string conversion overhead.

5. **Multiple Filters**: Budget ~3-4 Mrows/sec per additional filter.

## Comparison to Pre-Optimization

The optimization eliminated:
- **N × M redundant condition checks** (N rows, M filters)
- **Nested filter type evaluation** inside row loops
- **Redundant string case conversions**

For 1M rows with 3 filters, this eliminated **3 million** unnecessary `isExactMatch` condition checks!
