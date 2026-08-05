/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tables

import (
	"fmt"
	"sort"
	"testing"

	"github.com/google/taxinomia/core/columns"
)

// buildSortableTable builds a chunked-column table shaped like loader output:
// a low-cardinality dimension, a unique primary key, and a measure.
func buildSortableTable(n int) *DataTable {
	dim := columns.NewChunkedStringColumn(columns.NewColumnDef("dim", "Dim", ""))
	pk := columns.NewChunkedInt64Column(columns.NewColumnDef("pk", "PK", "test.pk"))
	val := columns.NewChunkedFloat64Column(columns.NewColumnDef("val", "Val", ""))
	dims := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for i := 0; i < n; i++ {
		dim.Append(dims[i%len(dims)])
		pk.Append(int64(n - i)) // unique, descending
		val.Append(float64(i))
	}
	dim.FinalizeColumn()
	pk.FinalizeColumn()
	val.FinalizeColumn()
	dt := NewDataTable()
	dt.AddColumn(dim)
	dt.AddColumn(pk)
	dt.AddColumn(val)
	return dt
}

// rowTuples snapshots every row of the table as one string, for row-alignment
// checks across a sort.
func rowTuples(t *testing.T, dt *DataTable, colNames []string) []string {
	t.Helper()
	rows := make([]string, dt.Length())
	for i := range rows {
		tuple := ""
		for _, name := range colNames {
			s, err := dt.GetColumn(name).GetString(uint32(i))
			if err != nil {
				t.Fatalf("GetString(%s, %d): %v", name, i, err)
			}
			tuple += s + "|"
		}
		rows[i] = tuple
	}
	return rows
}

func TestSortByKeyRowsAlignedAndOrdered(t *testing.T) {
	const n = 1000
	dt := buildSortableTable(n)
	cols := []string{"dim", "pk", "val"}
	before := rowTuples(t, dt, cols)

	if dt.SortKey() != nil {
		t.Fatal("SortKey should be nil before sorting")
	}
	if err := dt.SortByKey([]string{"dim", "pk"}); err != nil {
		t.Fatal(err)
	}
	if got := dt.SortKey(); len(got) != 2 || got[0] != "dim" || got[1] != "pk" {
		t.Errorf("SortKey() = %v, want [dim pk]", got)
	}

	after := rowTuples(t, dt, cols)

	// Same multiset of rows: sorting permutes, never invents or loses.
	sortedBefore := append([]string(nil), before...)
	sortedAfter := append([]string(nil), after...)
	sort.Strings(sortedBefore)
	sort.Strings(sortedAfter)
	for i := range sortedBefore {
		if sortedBefore[i] != sortedAfter[i] {
			t.Fatalf("row multiset changed at %d: %q vs %q", i, sortedBefore[i], sortedAfter[i])
		}
	}

	// Ordered by (dim, pk): dim ascending; within a dim run, pk ascending.
	dim := dt.GetColumn("dim")
	pk := dt.GetColumn("pk").(*columns.ChunkedInt64Column)
	for i := 1; i < n; i++ {
		if c := columns.CompareAtIndex(dim, uint32(i-1), uint32(i)); c > 0 {
			t.Fatalf("dim out of order at row %d", i)
		} else if c == 0 {
			a, _ := pk.GetValue(uint32(i - 1))
			b, _ := pk.GetValue(uint32(i))
			if a >= b {
				t.Fatalf("pk tie-break out of order at row %d: %d >= %d", i, a, b)
			}
		}
	}
}

// Sorting by the primary key of a multi-chunk table must leave the leading
// column's chunk zone maps non-overlapping — that is what zone-map pruning
// and (later) the sparse PK index build on.
func TestSortByKeyChunkBoundsMonotone(t *testing.T) {
	const n = 200_000 // > 3 chunks at the default chunk size
	dt := buildSortableTable(n)
	if err := dt.SortByKey([]string{"pk"}); err != nil {
		t.Fatal(err)
	}
	pk := dt.GetColumn("pk").(*columns.ChunkedInt64Column)
	if pk.NumChunks() < 3 {
		t.Fatalf("want >= 3 chunks, got %d", pk.NumChunks())
	}
	for ci := 1; ci < pk.NumChunks(); ci++ {
		_, prevMax, ok1 := pk.ChunkBounds(ci - 1)
		curMin, _, ok2 := pk.ChunkBounds(ci)
		if !ok1 || !ok2 {
			t.Fatalf("missing zone maps on sorted column (chunk %d)", ci)
		}
		if prevMax >= curMin {
			t.Fatalf("chunk bounds overlap at %d: prevMax=%d curMin=%d", ci, prevMax, curMin)
		}
	}
	if !pk.IsKey() {
		t.Error("pk should still be a key after sorting")
	}
}

func TestSortByKeyStableForEqualRows(t *testing.T) {
	dim := columns.NewChunkedStringColumn(columns.NewColumnDef("dim", "Dim", ""))
	seq := columns.NewChunkedInt64Column(columns.NewColumnDef("seq", "Seq", ""))
	// dim alone does not order rows totally; load order must break ties.
	input := []string{"b", "a", "b", "a", "b"}
	for i, v := range input {
		dim.Append(v)
		seq.Append(int64(i))
	}
	dt := NewDataTable()
	dt.AddColumn(dim)
	dt.AddColumn(seq)
	if err := dt.SortByKey([]string{"dim"}); err != nil {
		t.Fatal(err)
	}
	got := ""
	for i := 0; i < dt.Length(); i++ {
		s, _ := dt.GetColumn("seq").GetString(uint32(i))
		got += s
	}
	if got != "13024" {
		t.Errorf("stable sort order = %q, want %q", got, "13024")
	}
}

// BenchmarkSortByKey1M measures the load-time cost of sorting a 1M-row
// table (dimension + unique pk + measure) into its declared physical order:
// permutation sort plus the reorder-rebuild of all three columns.
func BenchmarkSortByKey1M(b *testing.B) {
	for _, key := range [][]string{{"pk"}, {"dim", "pk"}} {
		b.Run(fmt.Sprintf("key=%v", key), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dt := buildSortableTable(1_000_000)
				b.StartTimer()
				if err := dt.SortByKey(key); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestSortByKeyErrors(t *testing.T) {
	t.Run("unknown column", func(t *testing.T) {
		dt := buildSortableTable(10)
		if err := dt.SortByKey([]string{"nope"}); err == nil {
			t.Error("expected error for unknown sort key column")
		}
		if dt.SortKey() != nil {
			t.Error("failed sort must not record a sort key")
		}
	})

	t.Run("empty key", func(t *testing.T) {
		dt := buildSortableTable(10)
		if err := dt.SortByKey(nil); err == nil {
			t.Error("expected error for empty sort key")
		}
	})

	t.Run("non-reorderable column", func(t *testing.T) {
		dt := buildSortableTable(10)
		plain := columns.NewStringColumn(columns.NewColumnDef("plain", "Plain", ""))
		for i := 0; i < 10; i++ {
			plain.Append(fmt.Sprintf("v%d", i))
		}
		dt.AddColumn(plain)
		before := rowTuples(t, dt, []string{"dim", "pk", "val", "plain"})
		if err := dt.SortByKey([]string{"pk"}); err == nil {
			t.Fatal("expected error when a column is not Reorderable")
		}
		after := rowTuples(t, dt, []string{"dim", "pk", "val", "plain"})
		for i := range before {
			if before[i] != after[i] {
				t.Fatalf("failed sort modified the table at row %d", i)
			}
		}
		if dt.SortKey() != nil {
			t.Error("failed sort must not record a sort key")
		}
	})
}
