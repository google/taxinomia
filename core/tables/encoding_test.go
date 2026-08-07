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
// dictionary-encoded (d = n is the pathological case). Sorted by itself it
// becomes front-coded; behind a leading dimension (not in value order) it
// takes the plain arena and keeps serving reverse lookups.
func TestSelectEncodingsKeyStringColumn(t *testing.T) {
	pk := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "test.id"))
	dim := columns.NewChunkedStringColumn(columns.NewColumnDef("dim", "Dim", ""))
	for i := 0; i < 500; i++ {
		pk.Append(fmt.Sprintf("id-%04d", (i*7)%500))
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

	id, ok := dt.GetColumn("id").(*columns.ChunkedArenaStringColumn)
	if !ok {
		t.Fatalf("string key behind a leading dimension should take arena storage; got %T", dt.GetColumn("id"))
	}
	if id.SortedBySelf() {
		t.Error("id sorted under (dim, id) should not be in its own value order")
	}
	for _, want := range []string{"id-0000", "id-0250", "id-0499"} {
		row, err := id.GetIndex(want)
		if err != nil {
			t.Fatalf("GetIndex(%q): %v", want, err)
		}
		if got, _ := id.GetValue(row); got != want {
			t.Errorf("GetIndex(%q) = row %d holding %q", want, row, got)
		}
	}
	if _, ok := dt.GetColumn("dim").(*columns.ChunkedDictStringColumn[uint8]); !ok {
		t.Errorf("declared dim not compacted; got %T", dt.GetColumn("dim"))
	}
}

// TestSelectEncodingsStringPKFrontCoded: a string primary key on storage
// sorted by itself becomes front-coded (docs/scaling-to-1b-rows.md §5,
// "String PK") and keeps serving reverse lookups from the sparse index.
func TestSelectEncodingsStringPKFrontCoded(t *testing.T) {
	pk := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "test.id"))
	val := columns.NewChunkedInt64Column(columns.NewColumnDef("val", "Val", ""))
	for i := 0; i < 500; i++ {
		pk.Append(fmt.Sprintf("id-%04d", (i*7)%500))
		val.Append(int64(i))
	}
	pk.FinalizeColumn()
	val.FinalizeColumn()
	dt := NewDataTable()
	dt.AddColumn(pk)
	dt.AddColumn(val)
	if err := dt.SortByKey([]string{"id"}); err != nil {
		t.Fatal(err)
	}

	dt.SelectEncodings()

	id, ok := dt.GetColumn("id").(*columns.ChunkedFrontCodedStringColumn)
	if !ok {
		t.Fatalf("sorted string PK not front-coded; got %T", dt.GetColumn("id"))
	}
	for i := 0; i < 500; i++ {
		want := fmt.Sprintf("id-%04d", i)
		row, err := id.GetIndex(want)
		if err != nil || row != uint32(i) {
			t.Fatalf("GetIndex(%q) = (%d, %v), want (%d, nil)", want, row, err, i)
		}
	}
	if _, err := id.GetIndex("id-9999"); err == nil {
		t.Error("GetIndex on an absent key should error")
	}
	// The rows stay aligned: val was permuted with the pk.
	valCol := dt.GetColumn("val").(*columns.ChunkedInt64Column)
	for i := 0; i < 500; i++ {
		idv, _ := id.GetString(uint32(i))
		v, _ := valCol.GetValue(uint32(i))
		if want := fmt.Sprintf("id-%04d", (v*7)%500); idv != want {
			t.Fatalf("row %d misaligned: id %q, val %d (expect id %q)", i, idv, v, want)
		}
	}
}

// TestSelectEncodingsThresholdPath: without a role declaration the size
// thresholds decide — a large repetitive column compacts; a small one and an
// all-distinct one decline the dictionary (the latter even though it was
// never finalized, so IsKey cannot flag it) and take the plain arena instead:
// no loaded string column keeps []string storage.
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
	smallCol, ok := dtSmall.GetColumn("small").(*columns.ChunkedArenaStringColumn)
	if !ok {
		t.Errorf("100-row column should decline the dictionary and take the arena; got %T", dtSmall.GetColumn("small"))
	}
	distinctCol, ok := dtDistinct.GetColumn("distinct").(*columns.ChunkedArenaStringColumn)
	if !ok {
		t.Errorf("all-distinct column should decline the dictionary and take the arena; got %T", dtDistinct.GetColumn("distinct"))
	}
	for i := 0; i < 100; i++ {
		if got, _ := smallCol.GetString(uint32(i)); got != []string{"a", "b"}[i%2] {
			t.Fatalf("small row %d = %q after arena encoding", i, got)
		}
	}
	for i := 0; i < 5000; i += 617 {
		if got, _ := distinctCol.GetString(uint32(i)); got != fmt.Sprintf("v-%06d", i) {
			t.Fatalf("distinct row %d = %q after arena encoding", i, got)
		}
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

// TestSelectEncodingsRenderingParityStringPK: with a string primary key
// (front-coded after encoding) and a high-cardinality note column (arena
// after encoding), the rendered output is identical before and after
// sort+encode. Rows are built already in pk order so the sort is the identity
// permutation and the outputs are comparable byte for byte.
func TestSelectEncodingsRenderingParityStringPK(t *testing.T) {
	build := func() *DataTable {
		pk := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "test.id"))
		dim := columns.NewChunkedStringColumn(columns.NewColumnDef("dim", "Dim", ""))
		note := columns.NewChunkedStringColumn(columns.NewColumnDef("note", "Note", ""))
		for i := 0; i < 120; i++ {
			pk.Append(fmt.Sprintf("id-%04d", i))
			dim.Append([]string{"alpha", "bravo", "charlie"}[i%3])
			note.Append(fmt.Sprintf("note text %04d", (i*7)%120))
		}
		pk.FinalizeColumn()
		dim.FinalizeColumn()
		note.FinalizeColumn()
		dt := NewDataTable()
		dt.AddColumn(pk)
		dt.AddColumn(dim)
		dt.AddColumn(note)
		return dt
	}

	render := func(dt *DataTable) string {
		tv := NewTableView(dt, "parity")
		tv.VisibleColumns = []string{"dim", "id", "note"}
		tv.ApplyFilters(map[string]string{"dim": "alpha|bravo", "id": "\"id-0042\""})
		tv.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
		out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
		tv2 := NewTableView(dt, "parity2")
		tv2.VisibleColumns = []string{"dim", "id", "note"}
		tv2.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
		return out + "\n=== grouped ===\n" + dumpGroupTree(tv2, tv2.GetLeafColumns()) + "\n=== ascii ===\n" + tv2.ToAscii()
	}

	plain := build()
	encoded := build()
	if err := encoded.SortByKey([]string{"id"}); err != nil {
		t.Fatal(err)
	}
	encoded.SelectEncodings()
	if _, ok := encoded.GetColumn("id").(*columns.ChunkedFrontCodedStringColumn); !ok {
		t.Fatalf("precondition: id should be front-coded, got %T", encoded.GetColumn("id"))
	}
	if _, ok := encoded.GetColumn("note").(*columns.ChunkedArenaStringColumn); !ok {
		t.Fatalf("precondition: note should be arena-encoded, got %T", encoded.GetColumn("note"))
	}
	// dim is below the dictionary size threshold and not a declared sort-key
	// dimension here, so it takes the arena too.
	if _, ok := encoded.GetColumn("dim").(*columns.ChunkedArenaStringColumn); !ok {
		t.Fatalf("precondition: dim should be arena-encoded, got %T", encoded.GetColumn("dim"))
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

// TestStringStorageRetainedMemory pins the phase-4c acceptance criterion
// (docs/scaling-to-1b-rows.md §5): arena storage drops the per-row string
// header ([]string retains a 16-byte header plus an individual content
// allocation per row; the arena retains a 4-byte offset plus packed bytes),
// and front coding shrinks a sorted key well below even the arena, because
// consecutive keys share most of their bytes.
func TestStringStorageRetainedMemory(t *testing.T) {
	if raceEnabled {
		t.Skip("memory measurement is not meaningful under the race detector")
	}
	const rows = 1_000_000

	retained := func(build func() columns.IDataColumn) (columns.IDataColumn, int64) {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		col := build()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(col)
		return col, int64(after.HeapAlloc) - int64(before.HeapAlloc)
	}

	// Sorted 12-byte keys, generated per column so each column owns its
	// string contents.
	buildPlain := func() *columns.ChunkedStringColumn {
		col := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "test.id"))
		for i := 0; i < rows; i++ {
			col.Append(fmt.Sprintf("cust_%07d", i))
		}
		col.FinalizeColumn()
		return col
	}

	plainAny, plainBytes := retained(func() columns.IDataColumn { return buildPlain() })
	plain := plainAny.(*columns.ChunkedStringColumn)
	arenaAny, arenaBytes := retained(func() columns.IDataColumn { return columns.ArenaEncodeChunkedStringColumn(buildPlain()) })
	arena := arenaAny.(*columns.ChunkedArenaStringColumn)
	fcAny, fcBytes := retained(func() columns.IDataColumn {
		fc, ok := columns.FrontCodeChunkedStringColumn(buildPlain())
		if !ok {
			t.Fatal("front coding declined the sorted key")
		}
		return fc
	})
	fc := fcAny.(*columns.ChunkedFrontCodedStringColumn)
	t.Logf("retained for %d rows: []string %d bytes, arena %d bytes, front-coded %d bytes", rows, plainBytes, arenaBytes, fcBytes)

	if arenaBytes > plainBytes*6/10 {
		t.Errorf("arena retained %d bytes vs []string %d; want the header elimination to save at least 40%%", arenaBytes, plainBytes)
	}
	if fcBytes > arenaBytes*7/10 {
		t.Errorf("front-coded retained %d bytes vs arena %d; want prefix sharing to save at least 30%%", fcBytes, arenaBytes)
	}
	for _, i := range []uint32{0, 1, 65535, 65536, 999_999} {
		want, _ := plain.GetString(i)
		if got, _ := arena.GetString(i); got != want {
			t.Fatalf("arena row %d = %q, want %q", i, got, want)
		}
		if got, _ := fc.GetString(i); got != want {
			t.Fatalf("front-coded row %d = %q, want %q", i, got, want)
		}
		if row, err := fc.GetIndex(want); err != nil || row != i {
			t.Fatalf("front-coded GetIndex(%q) = (%d, %v), want (%d, nil)", want, row, err, i)
		}
	}
}
