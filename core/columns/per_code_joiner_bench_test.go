package columns

import (
	"fmt"
	"testing"
)

// Benchmarks for the per-code joiner (phase 6a): an FK -> PK join swept over
// every row, per-row Joiner[string] against PerCodeJoiner. The target is a
// front-coded sorted PK — the representation encoding selection gives loaded
// string primary keys — so the per-row path pays the §6 O(log n) key lookup
// on every row and the per-code path pays it once per distinct FK value.

const (
	joinBenchRows    = 1_000_000
	joinBenchTargets = 100_000
	joinBenchDist    = 1000
)

// newJoinBenchColumns builds a 1M-row dict FK column whose values hit 1000
// distinct keys of a 100k-row front-coded sorted PK column.
func newJoinBenchColumns(b *testing.B) (*ChunkedDictStringColumn[uint16], *ChunkedFrontCodedStringColumn) {
	b.Helper()
	pkSrc := NewChunkedStringColumn(NewColumnDef("pk", "PK", "thing"))
	for i := 0; i < joinBenchTargets; i++ {
		pkSrc.Append(fmt.Sprintf("k%06d", i))
	}
	pkSrc.FinalizeColumn()
	pk, ok := FrontCodeChunkedStringColumn(pkSrc)
	if !ok {
		b.Fatal("front coding declined the sorted key column")
	}

	fk := NewChunkedDictStringColumn[uint16](NewColumnDef("fk", "FK", "thing"))
	stride := joinBenchTargets / joinBenchDist
	for i := 0; i < joinBenchRows; i++ {
		fk.Append(fmt.Sprintf("k%06d", (i%joinBenchDist)*stride))
	}
	fk.FinalizeColumn()
	return fk, pk
}

func BenchmarkJoin_LookupSweep_1M(b *testing.B) {
	fk, pk := newJoinBenchColumns(b)

	sweep := func(b *testing.B, j IJoiner) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for row := uint32(0); row < joinBenchRows; row++ {
				if _, err := j.Lookup(row); err != nil {
					b.Fatal(err)
				}
			}
		}
	}

	b.Run("PerRow", func(b *testing.B) {
		sweep(b, &Joiner[string]{FromColumn: fk, ToColumn: pk})
	})
	b.Run("PerCode", func(b *testing.B) {
		sweep(b, PerCodeJoinerFor(fk, pk))
	})
}

// BenchmarkJoin_GroupCounts_1M measures the query shape the joiner sits in:
// grouping a joined column over the full table, where every row resolves
// through Lookup before its group key is read.
func BenchmarkJoin_GroupCounts_1M(b *testing.B) {
	fk, pk := newJoinBenchColumns(b)

	// The displayed column of the target table: 10 distinct notes.
	note := NewChunkedDictStringColumn[uint8](NewColumnDef("note", "Note", ""))
	for i := 0; i < joinBenchTargets; i++ {
		note.Append(fmt.Sprintf("note_%d", i%10))
	}
	note.FinalizeColumn()

	sel := AllRows(joinBenchRows)
	run := func(b *testing.B, j IJoiner) {
		col := NewJoinedStringColumn(NewColumnDef("joined", "Joined", ""), j, note)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupCounts(sel)
		}
	}

	b.Run("PerRow", func(b *testing.B) {
		run(b, &Joiner[string]{FromColumn: fk, ToColumn: pk})
	})
	b.Run("PerCode", func(b *testing.B) {
		run(b, PerCodeJoinerFor(fk, pk))
	})
}
