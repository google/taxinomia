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
	"runtime"
	"testing"

	"github.com/google/taxinomia/core/columns"
)

// TestSelectEncodingsDeclaredDimension: a sort-key column is a declared
// dimension and is dictionary-encoded regardless of row count; the key column
// and non-string columns pass through; row content and the recorded sort key
// are unchanged.
func TestSelectEncodingsDeclaredDimension(t *testing.T) {
	const n = 1000 // below the size threshold: only the role declaration compacts
	dt := buildSortableTable(n)
	if err := dt.SortByKey([]string{"dim", "pk"}); err != nil {
		t.Fatal(err)
	}
	cols := []string{"dim", "pk", "val"}
	before := rowTuples(t, dt, cols)

	dt.SelectEncodings()

	dim, ok := dt.GetColumn("dim").(*columns.ChunkedDictStringColumn[uint8])
	if !ok {
		t.Fatalf("declared sort-key dimension not dictionary-encoded; got %T", dt.GetColumn("dim"))
	}
	if dim.Cardinality() != 5 {
		t.Errorf("dictionary cardinality = %d, want 5", dim.Cardinality())
	}
	if !dim.SortedBySelf() {
		t.Error("leading sort-key dimension should be recorded as sorted by itself")
	}
	if _, ok := dt.GetColumn("pk").(*columns.ChunkedInt64Column); !ok {
		t.Errorf("pk column type changed: %T", dt.GetColumn("pk"))
	}
	if _, ok := dt.GetColumn("val").(*columns.ChunkedFloat64Column); !ok {
		t.Errorf("val column type changed: %T", dt.GetColumn("val"))
	}

	after := rowTuples(t, dt, cols)
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("row %d changed across SelectEncodings: %q vs %q", i, before[i], after[i])
		}
	}
	if got := dt.SortKey(); len(got) != 2 || got[0] != "dim" || got[1] != "pk" {
		t.Errorf("SortKey() = %v, want [dim pk] unchanged", got)
	}
}

// TestSelectEncodingsKeyStringColumn: a string primary key is never
// dictionary-encoded (d = n is the pathological case), sorted or not.
func TestSelectEncodingsKeyStringColumn(t *testing.T) {
	pk := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "test.id"))
	dim := columns.NewChunkedStringColumn(columns.NewColumnDef("dim", "Dim", ""))
	for i := 0; i < 500; i++ {
		pk.Append(fmt.Sprintf("id-%04d", i))
		dim.Append([]string{"x", "y"}[i%2])
	}
	pk.FinalizeColumn()
	dim.FinalizeColumn()
	dt := NewDataTable()
	dt.AddColumn(pk)
	dt.AddColumn(dim)
	if err := dt.SortByKey([]string{"dim", "id"}); err != nil {
		t.Fatal(err)
	}

	dt.SelectEncodings()

	if _, ok := dt.GetColumn("id").(*columns.ChunkedStringColumn); !ok {
		t.Errorf("string key column re-encoded to %T; keys must stay plain until arena storage", dt.GetColumn("id"))
	}
	if _, ok := dt.GetColumn("dim").(*columns.ChunkedDictStringColumn[uint8]); !ok {
		t.Errorf("declared dim not compacted; got %T", dt.GetColumn("dim"))
	}
}

// TestSelectEncodingsThresholdPath: without a role declaration the size
// thresholds decide — a large repetitive column compacts, a small one and an
// all-distinct one do not (the latter even though it was never finalized, so
// IsKey cannot flag it).
func TestSelectEncodingsThresholdPath(t *testing.T) {
	big := columns.NewChunkedStringColumn(columns.NewColumnDef("big", "Big", ""))
	small := columns.NewChunkedStringColumn(columns.NewColumnDef("small", "Small", ""))
	distinct := columns.NewChunkedStringColumn(columns.NewColumnDef("distinct", "Distinct", ""))
	for i := 0; i < 5000; i++ {
		big.Append([]string{"a", "b", "c"}[i%3])
		distinct.Append(fmt.Sprintf("v-%06d", i))
	}
	for i := 0; i < 100; i++ {
		small.Append([]string{"a", "b"}[i%2])
	}
	// None finalized: the datasources csv loaders never finalize, and
	// SelectEncodings must cope.
	dt := NewDataTable()
	dt.AddColumn(big)

	dtSmall := NewDataTable()
	dtSmall.AddColumn(small)

	dtDistinct := NewDataTable()
	dtDistinct.AddColumn(distinct)

	dt.SelectEncodings()
	dtSmall.SelectEncodings()
	dtDistinct.SelectEncodings()

	if _, ok := dt.GetColumn("big").(*columns.ChunkedDictStringColumn[uint8]); !ok {
		t.Errorf("5000-row 3-distinct column not compacted; got %T", dt.GetColumn("big"))
	}
	if _, ok := dtSmall.GetColumn("small").(*columns.ChunkedStringColumn); !ok {
		t.Errorf("100-row column compacted without a role declaration; got %T", dtSmall.GetColumn("small"))
	}
	if _, ok := dtDistinct.GetColumn("distinct").(*columns.ChunkedStringColumn); !ok {
		t.Errorf("all-distinct column was compacted; got %T", dtDistinct.GetColumn("distinct"))
	}
}

// TestSelectEncodingsRenderingParity: sorting plus re-encoding must not
// change what a query renders. The rows are built already in (dim, pk) order
// so the sort is the identity permutation and the grouped output is
// comparable byte for byte.
func TestSelectEncodingsRenderingParity(t *testing.T) {
	build := func() *DataTable {
		dim := columns.NewChunkedStringColumn(columns.NewColumnDef("dim", "Dim", ""))
		pk := columns.NewChunkedInt64Column(columns.NewColumnDef("pk", "PK", "test.pk"))
		val := columns.NewChunkedFloat64Column(columns.NewColumnDef("val", "Val", ""))
		row := 0
		for _, d := range []string{"alpha", "bravo", "charlie"} {
			for k := 0; k < 40; k++ {
				dim.Append(d)
				pk.Append(int64(row))
				val.Append(float64((row * 7) % 23))
				row++
			}
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

	render := func(dt *DataTable) string {
		tv := NewTableView(dt, "parity")
		tv.VisibleColumns = []string{"dim", "pk", "val"}
		tv.ApplyFilters(map[string]string{"dim": "alpha|bravo"})
		tv.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
		return dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	}

	plain := build()
	encoded := build()
	if err := encoded.SortByKey([]string{"dim", "pk"}); err != nil {
		t.Fatal(err)
	}
	encoded.SelectEncodings()
	if _, ok := encoded.GetColumn("dim").(*columns.ChunkedDictStringColumn[uint8]); !ok {
		t.Fatalf("precondition: dim should be dictionary-encoded, got %T", encoded.GetColumn("dim"))
	}

	if got, want := render(encoded), render(plain); got != want {
		t.Errorf("rendered output changed across sort+encode.\n--- encoded ---\n%s\n--- plain ---\n%s", got, want)
	}
}

// TestSortedKeyFinalizeRetainedMemory pins the phase-4b acceptance criterion
// (docs/scaling-to-1b-rows.md §6): finalizing a key column on sorted storage
// retains no reverse-lookup map — only zone maps — while the same data
// shuffled retains a map that scales with the rows. 1M int64 keys: the map
// costs tens of MB; the sparse path must stay under 1 MB.
func TestSortedKeyFinalizeRetainedMemory(t *testing.T) {
	if raceEnabled {
		t.Skip("memory measurement is not meaningful under the race detector")
	}
	const rows = 1_000_000

	measure := func(sorted bool) int64 {
		col := columns.NewChunkedInt64Column(columns.NewColumnDef("pk", "PK", "test.pk"))
		for i := 0; i < rows; i++ {
			v := int64(i)
			if !sorted {
				// A fixed-stride permutation: unique, thoroughly unsorted.
				v = int64((i*2_654_435_761 + 13) % rows)
			}
			col.Append(v)
		}
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		col.FinalizeColumn()

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(col)
		if !col.IsKey() {
			t.Fatalf("column (sorted=%v) not detected as key", sorted)
		}
		if _, err := col.GetIndex(500_000); err != nil {
			t.Fatalf("lookup on %v column failed: %v", map[bool]string{true: "sorted", false: "shuffled"}[sorted], err)
		}
		return int64(after.HeapAlloc) - int64(before.HeapAlloc)
	}

	sortedRetained := measure(true)
	unsortedRetained := measure(false)
	t.Logf("finalize retained: sorted %d bytes, shuffled %d bytes", sortedRetained, unsortedRetained)

	if sortedRetained > 1<<20 {
		t.Errorf("sorted key finalize retained %d bytes; want < 1 MiB (no reverse-lookup map)", sortedRetained)
	}
	if unsortedRetained < 10<<20 {
		t.Errorf("shuffled key finalize retained %d bytes; expected the map (> 10 MiB) — did the measurement break?", unsortedRetained)
	}
}
