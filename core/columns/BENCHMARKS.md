# Column Benchmarks

Benchmark results for column operations on an Intel Core i7-8550U @ 1.80GHz (Windows/amd64).

Run benchmarks with:
```bash
go test -bench=. -benchmem ./core/columns/
```

## Results Summary

### GroupIndices Comparison (1M rows, 100 groups)

| Column Type | Time (ms) | Memory (MB) | Allocs  |
|-------------|-----------|-------------|---------|
| Bool        |        18 |          21 |      66 |
| Uint32      |        22 |          14 |   1,709 |
| Duration    |        37 |          14 |   1,818 |
| Datetime    |        41 |          14 |   1,820 |
| String      |        48 |          14 |   1,818 |

### GroupIndices Edge Cases (1M rows)

| Groups      | Column Type | Time (ms) | Memory (MB) |   Allocs |
|-------------|-------------|-----------|-------------|----------|
| 1 (best)    | Uint32      |        18 |          21 |       36 |
| 1 (best)    | String      |        32 |          21 |       37 |
| 100         | Uint32      |        22 |          14 |    1,709 |
| 100         | String      |        48 |          14 |    1,818 |
| 1,000       | Uint32      |        24 |          13 |   11,020 |
| 1,000       | String      |        62 |          13 |   12,040 |
| 1M (worst)  | Uint32      |       372 |         176 |1,008,199 |
| 1M (worst)  | String      |       668 |         283 |1,016,390 |

### StringColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) |   Allocs |
|----------------|------|--------|-----------|-------------|----------|
| Append         |   1M |      - |       196 |          96 |1,000,063 |
| FinalizeColumn |   1M |      - |      0.01 |        0.01 |       11 |
| GetString      |   1M |      - |       3.5 |           0 |        0 |
| GroupIndices   |   1M |    100 |        49 |          14 |    1,818 |
| GroupIndices   |   1M |  1,000 |        62 |          13 |   12,040 |
| GroupIndices   |  10M |    100 |       547 |         197 |    2,718 |

### Uint32Column

| Operation      | Rows | Groups | Time (ms) | Memory (MB) |   Allocs |
|----------------|------|--------|-----------|-------------|----------|
| Append         |   1M |      - |        10 |          21 |       38 |
| FinalizeColumn |   1M |      - |      0.01 |        0.01 |       11 |
| GetString      |   1M |      - |        85 |         1.8 |  900,001 |
| GroupIndices   |   1M |    100 |        22 |          14 |    1,709 |
| GroupIndices   |   1M |  1,000 |        24 |          13 |   11,020 |
| GroupIndices   |  10M |    100 |       217 |         197 |    2,609 |

### BoolColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) | Allocs |
|----------------|------|--------|-----------|-------------|--------|
| Append         |   1M |      - |       4.7 |         5.2 |     34 |
| FinalizeColumn |   1M |      - |         0 |           0 |      0 |
| GetString      |   1M |      - |       3.4 |           0 |      0 |
| GroupIndices   |   1M |      2 |        22 |          21 |     66 |

### DatetimeColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) |   Allocs |
|----------------|------|--------|-----------|-------------|----------|
| Append         |   1M |      - |        65 |         128 |       39 |
| FinalizeColumn |   1M |      - |       175 |          76 |    8,200 |
| GetString      |   1M |      - |       224 |          24 |1,000,000 |
| GroupIndices   |   1M |    100 |        40 |          14 |    1,820 |

### DurationColumn

| Operation      | Rows | Groups | Time (ms) | Memory (MB) | Allocs  |
|----------------|------|--------|-----------|-------------|---------|
| Append         |   1M |      - |        14 |          42 |      40 |
| FinalizeColumn |   1M |      - |         0 |           0 |       0 |
| GetString      |   1M |      - |        70 |           8 | 999,000 |
| GroupIndices   |   1M |    100 |        36 |          14 |   1,818 |

## Key Observations

1. **GroupIndices Performance**: Uint32 is ~2.2x faster than String due to no string hashing overhead.

2. **Append Performance**: Uint32 is ~19x faster than String (no string allocation/copying).

3. **GetString Performance**: String is fastest (direct return), Uint32/Duration/Datetime require formatting.

4. **FinalizeColumn**: Bool and Duration are essentially free (no index building needed).

5. **Memory**: GroupIndices allocates ~14MB for result maps with 100 groups.

6. **Edge Cases**:
   - **1 group (best case)**: ~32ms String, ~18ms Uint32 - dominated by slice append operations
   - **1M groups (worst case)**: ~668ms String, ~372ms Uint32 - 14x slower than 100 groups due to map overhead and 1M allocations

## Raw Output

```
goos: windows
goarch: amd64
pkg: github.com/google/taxinomia/core/columns
cpu: Intel(R) Core(TM) i7-8550U CPU @ 1.80GHz
BenchmarkStringColumn_Append_1M-8                              6    196075267 ns/op   96026166 B/op  1000063 allocs/op
BenchmarkStringColumn_FinalizeColumn_1M-8                 106592        11496 ns/op       6952 B/op       11 allocs/op
BenchmarkStringColumn_GetString_1M-8                         306      3463480 ns/op          0 B/op        0 allocs/op
BenchmarkStringColumn_GroupIndices_1M_100Groups-8             24     49146425 ns/op   14133728 B/op     1818 allocs/op
BenchmarkStringColumn_GroupIndices_1M_1000Groups-8            19     62409058 ns/op   13192752 B/op    12040 allocs/op
BenchmarkStringColumn_GroupIndices_10M_100Groups-8             2    547266850 ns/op  196815328 B/op     2718 allocs/op
BenchmarkUint32Column_Append_1M-8                            100     10145750 ns/op   21096470 B/op       38 allocs/op
BenchmarkUint32Column_FinalizeColumn_1M-8                 142903         8173 ns/op       4648 B/op       11 allocs/op
BenchmarkUint32Column_GetString_1M-8                          13     85260477 ns/op    1800468 B/op   900001 allocs/op
BenchmarkUint32Column_GroupIndices_1M_100Groups-8             56     21842030 ns/op   14126650 B/op     1709 allocs/op
BenchmarkUint32Column_GroupIndices_1M_1000Groups-8            49     23941876 ns/op   13079999 B/op    11020 allocs/op
BenchmarkUint32Column_GroupIndices_10M_100Groups-8             5    217398940 ns/op  196808251 B/op     2609 allocs/op
BenchmarkBoolColumn_Append_1M-8                              332      4651922 ns/op    5241669 B/op       34 allocs/op
BenchmarkBoolColumn_FinalizeColumn_1M-8                1000000000         0.36 ns/op          0 B/op        0 allocs/op
BenchmarkBoolColumn_GetString_1M-8                           387      3372294 ns/op          0 B/op        0 allocs/op
BenchmarkBoolColumn_GroupIndices_1M-8                         52     22037290 ns/op   21155608 B/op       66 allocs/op
BenchmarkDatetimeColumn_Append_1M-8                           19     65173526 ns/op  127920172 B/op       39 allocs/op
BenchmarkDatetimeColumn_FinalizeColumn_1M-8                    6    175094533 ns/op   75518664 B/op     8200 allocs/op
BenchmarkDatetimeColumn_GetString_1M-8                         5    223960760 ns/op   24000000 B/op  1000000 allocs/op
BenchmarkDatetimeColumn_GroupIndices_1M_100Groups-8           26     40472381 ns/op   14131824 B/op     1820 allocs/op
BenchmarkDurationColumn_Append_1M-8                          100     14222418 ns/op   41678243 B/op       40 allocs/op
BenchmarkDurationColumn_FinalizeColumn_1M-8            1000000000         0.80 ns/op          0 B/op        0 allocs/op
BenchmarkDurationColumn_GetString_1M-8                        16     70451281 ns/op    7992023 B/op   999000 allocs/op
BenchmarkDurationColumn_GroupIndices_1M_100Groups-8           30     36459350 ns/op   14131491 B/op     1818 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/String-8        25     47984520 ns/op   14133728 B/op     1818 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Uint32-8        49     22219524 ns/op   14126635 B/op     1709 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Bool-8          64     17995416 ns/op   21155592 B/op       66 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Duration-8      31     37231994 ns/op   14131497 B/op     1818 allocs/op
BenchmarkGroupIndices_Comparison_1M_100Groups/Datetime-8      32     41358834 ns/op   14131824 B/op     1820 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1Group/String-8             36     31780539 ns/op   21096321 B/op       37 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1Group/Uint32-8             68     17845099 ns/op   21096429 B/op       36 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1MGroups/String-8            2    668237400 ns/op  283135920 B/op  1016390 allocs/op
BenchmarkGroupIndices_EdgeCase_1M_1MGroups/Uint32-8            3    372349467 ns/op  175635949 B/op  1008199 allocs/op
PASS
ok  github.com/google/taxinomia/core/columns  52.095s
```
