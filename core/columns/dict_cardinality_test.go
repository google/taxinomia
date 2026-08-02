package columns

import (
	"fmt"
	"runtime"
	"testing"
)

// Sweeps the distinct-value ratio to locate the point where dictionary encoding
// stops paying for itself. uint32 codes throughout so the sweep measures
// cardinality alone; narrower codes do strictly better at the low end.

var sweepCardinalities = []int{10, 100, 1_000, 10_000, 100_000, 250_000, 500_000, 1_000_000}

func BenchmarkDict_Sweep_GroupIndices(b *testing.B) {
	indices := createBenchIndices(largeSize)
	for _, d := range sweepCardinalities {
		b.Run(fmt.Sprintf("d=%d/String", d), func(b *testing.B) {
			col := newBenchStringColumn(largeSize, d)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				col.GroupIndices(indices, nil)
			}
		})
		b.Run(fmt.Sprintf("d=%d/Dict32", d), func(b *testing.B) {
			col := newBenchDictColumn[uint32](largeSize, d)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				col.GroupIndices(indices, nil)
			}
		})
	}
}

func BenchmarkDict_Sweep_Build(b *testing.B) {
	for _, d := range sweepCardinalities {
		values := benchValues(largeSize, d)
		b.Run(fmt.Sprintf("d=%d/String", d), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				col := NewStringColumn(NewColumnDef("test", "Test", ""))
				for _, v := range values {
					col.Append(v)
				}
				col.FinalizeColumn()
			}
		})
		b.Run(fmt.Sprintf("d=%d/Dict32", d), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				col := NewDictStringColumn[uint32](NewColumnDef("test", "Test", ""))
				for _, v := range values {
					col.Append(v)
				}
				col.FinalizeColumn()
			}
		})
	}
}

// Grouping a small subset of a high-cardinality column: the dense counting-sort
// path allocates arrays sized by the dictionary, not by the input, so
// GroupIndices falls back to map grouping when the subset is small relative to
// the dictionary. This sweep covers both sides of that switch.
func BenchmarkDict_Sweep_SmallSubset(b *testing.B) {
	subset := createBenchIndices(1000)
	for _, d := range []int{100, 10_000, 500_000} {
		b.Run(fmt.Sprintf("d=%d/String", d), func(b *testing.B) {
			col := newBenchStringColumn(largeSize, d)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				col.GroupIndices(subset, nil)
			}
		})
		b.Run(fmt.Sprintf("d=%d/Dict32", d), func(b *testing.B) {
			col := newBenchDictColumn[uint32](largeSize, d)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				col.GroupIndices(subset, nil)
			}
		})
	}
}

// TestCardinalityMemorySweep reports retained memory across the same sweep,
// with and without the interning map kept after finalize.
//
//	go test ./core/columns -run TestCardinalityMemorySweep -v
func TestCardinalityMemorySweep(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~100MB per point")
	}

	retained := func(build func() any) uint64 {
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
			return 0
		}
		return after.HeapAlloc - before.HeapAlloc
	}

	t.Logf("%10s %6s %12s %12s %12s %8s", "distinct", "d/n", "String MB", "Dict MB", "Dict-noidx", "ratio")
	for _, d := range sweepCardinalities {
		plainBytes := retained(func() any {
			col := NewStringColumn(NewColumnDef("test", "Test", ""))
			for j := 0; j < largeSize; j++ {
				col.Append(fmt.Sprintf("value_%d", j%d))
			}
			col.FinalizeColumn()
			return col
		})
		dictBytes := retained(func() any {
			col := NewDictStringColumn[uint32](NewColumnDef("test", "Test", ""))
			for j := 0; j < largeSize; j++ {
				col.Append(fmt.Sprintf("value_%d", j%d))
			}
			col.FinalizeColumn()
			return col
		})
		// FinalizeColumn now releases the interning map for non-key columns
		// itself; the explicit nil is kept so the comparison stays valid even
		// at the d=n point where the column is a key.
		dictNoIdxBytes := retained(func() any {
			col := NewDictStringColumn[uint32](NewColumnDef("test", "Test", ""))
			for j := 0; j < largeSize; j++ {
				col.Append(fmt.Sprintf("value_%d", j%d))
			}
			col.FinalizeColumn()
			col.index = nil
			return col
		})

		t.Logf("%10d %6.3f %12.2f %12.2f %12.2f %8.1fx",
			d, float64(d)/float64(largeSize),
			float64(plainBytes)/(1<<20),
			float64(dictBytes)/(1<<20),
			float64(dictNoIdxBytes)/(1<<20),
			float64(plainBytes)/float64(max(dictNoIdxBytes, 1)))
	}
}

// Cost of deciding NOT to compact. The absolute cardinality cap bounds the
// counting map at 64k entries regardless of n (the old n/2 rule grew it to
// n/2, most expensive exactly for the columns that gain nothing).
func BenchmarkDict_Compact_BailOut(b *testing.B) {
	for _, d := range []int{600_000, 1_000_000} {
		b.Run(fmt.Sprintf("d=%d", d), func(b *testing.B) {
			col := newBenchStringColumn(largeSize, d)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, ok := CompactStringColumn(col); ok {
					b.Fatal("expected bail-out")
				}
			}
		})
	}
}
