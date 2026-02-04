package columns

import (
	"fmt"
	"testing"
	"time"
)

// ============================================================================
// Test data sizes
// ============================================================================

const (
	smallSize  = 10_000
	mediumSize = 100_000
	largeSize  = 1_000_000
	hugeSize   = 10_000_000
)

// ============================================================================
// StringColumn Benchmarks
// ============================================================================

func BenchmarkStringColumn_Append_1M(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := NewStringColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(fmt.Sprintf("value_%d", j%100))
		}
	}
}

func BenchmarkStringColumn_FinalizeColumn_1M(b *testing.B) {
	col := NewStringColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(fmt.Sprintf("value_%d", j%100))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.valueIndex = nil // Reset to force re-finalization
		col.FinalizeColumn()
	}
}

func BenchmarkStringColumn_GetString_1M(b *testing.B) {
	col := NewStringColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(fmt.Sprintf("value_%d", j%100))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, idx := range indices {
			col.GetString(idx)
		}
	}
}

func BenchmarkStringColumn_GroupIndices_1M_100Groups(b *testing.B) {
	col := NewStringColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(fmt.Sprintf("value_%d", j%100))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

func BenchmarkStringColumn_GroupIndices_1M_1000Groups(b *testing.B) {
	col := NewStringColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(fmt.Sprintf("value_%d", j%1000))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

func BenchmarkStringColumn_GroupIndices_10M_100Groups(b *testing.B) {
	col := NewStringColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < hugeSize; j++ {
		col.Append(fmt.Sprintf("value_%d", j%100))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(hugeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

// ============================================================================
// Uint32Column Benchmarks
// ============================================================================

func BenchmarkUint32Column_Append_1M(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := NewUint32Column(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(uint32(j % 100))
		}
	}
}

func BenchmarkUint32Column_FinalizeColumn_1M(b *testing.B) {
	col := NewUint32Column(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(uint32(j % 100))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.valueIndex = nil
		col.FinalizeColumn()
	}
}

func BenchmarkUint32Column_GetString_1M(b *testing.B) {
	col := NewUint32Column(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(uint32(j % 100))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, idx := range indices {
			col.GetString(idx)
		}
	}
}

func BenchmarkUint32Column_GroupIndices_1M_100Groups(b *testing.B) {
	col := NewUint32Column(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(uint32(j % 100))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

func BenchmarkUint32Column_GroupIndices_1M_1000Groups(b *testing.B) {
	col := NewUint32Column(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(uint32(j % 1000))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

func BenchmarkUint32Column_GroupIndices_10M_100Groups(b *testing.B) {
	col := NewUint32Column(NewColumnDef("test", "Test", ""))
	for j := 0; j < hugeSize; j++ {
		col.Append(uint32(j % 100))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(hugeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

// ============================================================================
// BoolColumn Benchmarks
// ============================================================================

func BenchmarkBoolColumn_Append_1M(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := NewBoolColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(j%2 == 0)
		}
	}
}

func BenchmarkBoolColumn_FinalizeColumn_1M(b *testing.B) {
	col := NewBoolColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(j%2 == 0)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.FinalizeColumn()
	}
}

func BenchmarkBoolColumn_GetString_1M(b *testing.B) {
	col := NewBoolColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(j%2 == 0)
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, idx := range indices {
			col.GetString(idx)
		}
	}
}

func BenchmarkBoolColumn_GroupIndices_1M(b *testing.B) {
	col := NewBoolColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(j%2 == 0)
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

// ============================================================================
// DatetimeColumn Benchmarks
// ============================================================================

func BenchmarkDatetimeColumn_Append_1M(b *testing.B) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := NewDatetimeColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(baseTime.Add(time.Duration(j) * time.Hour))
		}
	}
}

func BenchmarkDatetimeColumn_FinalizeColumn_1M(b *testing.B) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	col := NewDatetimeColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(baseTime.Add(time.Duration(j) * time.Hour))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.FinalizeColumn()
	}
}

func BenchmarkDatetimeColumn_GetString_1M(b *testing.B) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	col := NewDatetimeColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(baseTime.Add(time.Duration(j) * time.Hour))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, idx := range indices {
			col.GetString(idx)
		}
	}
}

func BenchmarkDatetimeColumn_GroupIndices_1M_100Groups(b *testing.B) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	col := NewDatetimeColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		// Create 100 unique dates (one per day for 100 days, cycling)
		col.Append(baseTime.Add(time.Duration(j%100) * 24 * time.Hour))
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

// ============================================================================
// DurationColumn Benchmarks
// ============================================================================

func BenchmarkDurationColumn_Append_1M(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col := NewDurationColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(time.Duration(j%1000) * time.Millisecond)
		}
	}
}

func BenchmarkDurationColumn_FinalizeColumn_1M(b *testing.B) {
	col := NewDurationColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(time.Duration(j%1000) * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.FinalizeColumn()
	}
}

func BenchmarkDurationColumn_GetString_1M(b *testing.B) {
	col := NewDurationColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(time.Duration(j%1000) * time.Millisecond)
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, idx := range indices {
			col.GetString(idx)
		}
	}
}

func BenchmarkDurationColumn_GroupIndices_1M_100Groups(b *testing.B) {
	col := NewDurationColumn(NewColumnDef("test", "Test", ""))
	for j := 0; j < largeSize; j++ {
		col.Append(time.Duration(j%100) * time.Second)
	}
	col.FinalizeColumn()
	indices := createBenchIndices(largeSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		col.GroupIndices(indices, nil)
	}
}

// ============================================================================
// Cross-column comparison benchmarks (same operation, different types)
// ============================================================================

func BenchmarkGroupIndices_Comparison_1M_100Groups(b *testing.B) {
	indices := createBenchIndices(largeSize)

	b.Run("String", func(b *testing.B) {
		col := NewStringColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(fmt.Sprintf("val_%d", j%100))
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Uint32", func(b *testing.B) {
		col := NewUint32Column(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(uint32(j % 100))
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Bool", func(b *testing.B) {
		col := NewBoolColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(j%2 == 0)
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Duration", func(b *testing.B) {
		col := NewDurationColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(time.Duration(j%100) * time.Second)
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Datetime", func(b *testing.B) {
		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		col := NewDatetimeColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(baseTime.Add(time.Duration(j%100) * 24 * time.Hour))
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})
}

// ============================================================================
// Edge case benchmarks: 1 group (best case) and 1M groups (worst case)
// ============================================================================

func BenchmarkGroupIndices_EdgeCase_1M_1Group(b *testing.B) {
	indices := createBenchIndices(largeSize)

	b.Run("String", func(b *testing.B) {
		col := NewStringColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append("same_value") // All same value = 1 group
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Uint32", func(b *testing.B) {
		col := NewUint32Column(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(42) // All same value = 1 group
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})
}

func BenchmarkGroupIndices_EdgeCase_1M_1MGroups(b *testing.B) {
	indices := createBenchIndices(largeSize)

	b.Run("String", func(b *testing.B) {
		col := NewStringColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(fmt.Sprintf("unique_%d", j)) // All unique = 1M groups
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Uint32", func(b *testing.B) {
		col := NewUint32Column(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(uint32(j)) // All unique = 1M groups
		}
		col.FinalizeColumn()

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})
}

// ============================================================================
// Helper functions
// ============================================================================

func createBenchIndices(n int) []uint32 {
	indices := make([]uint32, n)
	for i := 0; i < n; i++ {
		indices[i] = uint32(i)
	}
	return indices
}
