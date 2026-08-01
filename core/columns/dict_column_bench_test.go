package columns

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// Benchmarks comparing StringColumn against dictionary encoding on repetitive
// data. Sizes and shapes mirror column_bench_test.go so the numbers line up.

func benchValues(n, distinct int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("value_%d", i%distinct)
	}
	return values
}

func newBenchStringColumn(n, distinct int) *StringColumn {
	col := NewStringColumn(NewColumnDef("test", "Test", ""))
	for _, v := range benchValues(n, distinct) {
		col.Append(v)
	}
	col.FinalizeColumn()
	return col
}

func newBenchDictColumn[K Unsigned](n, distinct int) *DictStringColumn[K] {
	col := NewDictStringColumn[K](NewColumnDef("test", "Test", ""))
	for _, v := range benchValues(n, distinct) {
		col.Append(v)
	}
	col.FinalizeColumn()
	return col
}

// ============================================================================
// GroupIndices - the hot path
// ============================================================================

func BenchmarkDict_GroupIndices_1M_100Groups(b *testing.B) {
	indices := createBenchIndices(largeSize)

	b.Run("String", func(b *testing.B) {
		col := newBenchStringColumn(largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Dict8", func(b *testing.B) {
		col := newBenchDictColumn[uint8](largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})
}

func BenchmarkDict_GroupIndices_1M_1000Groups(b *testing.B) {
	indices := createBenchIndices(largeSize)

	b.Run("String", func(b *testing.B) {
		col := newBenchStringColumn(largeSize, 1000)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("Dict16", func(b *testing.B) {
		col := newBenchDictColumn[uint16](largeSize, 1000)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})
}

// Nested grouping regroups a parent group's indices, so the input is a sparse
// subset rather than the full dense range.
func BenchmarkDict_GroupIndices_1M_Subset(b *testing.B) {
	subset := make([]uint32, 0, largeSize/10)
	for i := 0; i < largeSize; i += 10 {
		subset = append(subset, uint32(i))
	}

	b.Run("String", func(b *testing.B) {
		col := newBenchStringColumn(largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(subset, nil)
		}
	})

	b.Run("Dict8", func(b *testing.B) {
		col := newBenchDictColumn[uint8](largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(subset, nil)
		}
	})
}

// ============================================================================
// Filter - predicate per distinct value vs per row
// ============================================================================

func BenchmarkDict_Filter_1M_100Groups(b *testing.B) {
	predicate := func(v string) bool { return strings.HasSuffix(v, "7") }

	b.Run("String", func(b *testing.B) {
		col := newBenchStringColumn(largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.Filter(predicate)
		}
	})

	b.Run("Dict8", func(b *testing.B) {
		col := newBenchDictColumn[uint8](largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.Filter(predicate)
		}
	})
}

// With a cheap predicate the scan itself dominates and encoding barely helps.
// The saving only shows up once the predicate costs more than a code lookup -
// case-insensitive matching, regex, expression evaluation.
func BenchmarkDict_Filter_1M_ExpensivePredicate(b *testing.B) {
	predicate := func(v string) bool {
		return strings.Contains(strings.ToUpper(v), "VALUE_7")
	}

	b.Run("String", func(b *testing.B) {
		col := newBenchStringColumn(largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.Filter(predicate)
		}
	})

	b.Run("Dict8", func(b *testing.B) {
		col := newBenchDictColumn[uint8](largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.Filter(predicate)
		}
	})
}

// ============================================================================
// Scan / read path
// ============================================================================

func BenchmarkDict_GetString_1M(b *testing.B) {
	indices := createBenchIndices(largeSize)

	b.Run("String", func(b *testing.B) {
		col := newBenchStringColumn(largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, idx := range indices {
				col.GetString(idx)
			}
		}
	})

	b.Run("Dict8", func(b *testing.B) {
		col := newBenchDictColumn[uint8](largeSize, 100)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, idx := range indices {
				col.GetString(idx)
			}
		}
	})
}

// ============================================================================
// Build cost
// ============================================================================

func BenchmarkDict_Append_1M_100Groups(b *testing.B) {
	values := benchValues(largeSize, 100)

	b.Run("String", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col := NewStringColumn(NewColumnDef("test", "Test", ""))
			for _, v := range values {
				col.Append(v)
			}
		}
	})

	b.Run("Dict8", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col := NewDictStringColumn[uint8](NewColumnDef("test", "Test", ""))
			for _, v := range values {
				col.Append(v)
			}
		}
	})
}

func BenchmarkDict_Compact_1M_100Groups(b *testing.B) {
	col := newBenchStringColumn(largeSize, 100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		CompactStringColumn(col)
	}
}

// ============================================================================
// Resident memory - not a timing benchmark, reported via -run
// ============================================================================

// TestMemoryFootprint reports the heap retained by each representation of the
// same 1M-row, 100-distinct-value column. Run with:
//
//	go test ./core/columns -run TestMemoryFootprint -v
//
// Values are generated inside each build so that the column owns its strings,
// as it does in production - the loader hands over freshly parsed strings and
// then drops them. That is what makes the difference visible: the plain column
// retains one string allocation per row, the dictionary retains one per
// distinct value.
func TestMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~100MB")
	}

	// retained measures the heap still held after build() returns and the
	// garbage from building has been collected.
	retained := func(build func() any) (uint64, any) {
		runtime.GC()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)

		obj := build()

		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(obj)

		if after.HeapAlloc < before.HeapAlloc {
			t.Fatalf("heap shrank during build: %d -> %d", before.HeapAlloc, after.HeapAlloc)
		}
		return after.HeapAlloc - before.HeapAlloc, obj
	}

	plainBytes, plain := retained(func() any {
		col := NewStringColumn(NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(fmt.Sprintf("value_%d", j%100))
		}
		col.FinalizeColumn()
		return col
	})
	plainOnly := plain.(*StringColumn)
	plain = nil
	_ = plain

	dictBytes, dict := retained(func() any {
		col := NewDictStringColumn[uint8](NewColumnDef("test", "Test", ""))
		for j := 0; j < largeSize; j++ {
			col.Append(fmt.Sprintf("value_%d", j%100))
		}
		col.FinalizeColumn()
		return col
	})

	t.Logf("rows: %d, distinct: %d", largeSize, 100)
	t.Logf("StringColumn:     %7.2f MB", float64(plainBytes)/(1<<20))
	t.Logf("DictStringColumn: %7.2f MB", float64(dictBytes)/(1<<20))
	if dictBytes > 0 {
		t.Logf("ratio:            %7.1fx", float64(plainBytes)/float64(dictBytes))
	}

	runtime.KeepAlive(plainOnly)
	runtime.KeepAlive(dict)
}
