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

package columns

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// checkKeyLookups asserts that every value resolves to its row index and that
// every absent value errors, regardless of which lookup path (map or sparse
// binary search) the column uses.
func checkKeyLookups[T any](t *testing.T, col IDataColumnT[T], values []T, absent []T) {
	t.Helper()
	for i, v := range values {
		got, err := col.GetIndex(v)
		if err != nil {
			t.Fatalf("GetIndex(%v): unexpected error: %v", v, err)
		}
		if got != uint32(i) {
			t.Errorf("GetIndex(%v) = %d, want %d", v, got, i)
		}
	}
	for _, v := range absent {
		if got, err := col.GetIndex(v); err == nil {
			t.Errorf("GetIndex(%v) = %d, want error for absent value", v, got)
		}
	}
}

// TestSortedKeyColumnSparseIndex pins the phase-4b core: a key column whose
// storage is sorted by itself records that at finalize, builds NO
// reverse-lookup map, and serves GetIndex by binary search — including values
// at chunk boundaries and absent values below, between, and above the stored
// range. testChunkSize is 8, so 21 values span 3 chunks with a partial last.
func TestSortedKeyColumnSparseIndex(t *testing.T) {
	const n = 21

	t.Run("int64", func(t *testing.T) {
		col := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), testChunkSize)
		values := make([]int64, n)
		for i := range values {
			values[i] = int64(i * 10) // gaps so absent-between exists
			col.Append(values[i])
		}
		col.FinalizeColumn()
		if !col.IsKey() {
			t.Fatal("sorted unique column not detected as key")
		}
		if !col.SortedBySelf() {
			t.Fatal("sorted column not recorded as SortedBySelf")
		}
		if col.valueIndex != nil {
			t.Fatal("sorted key column built a reverse-lookup map; the sparse index should replace it")
		}
		checkKeyLookups[int64](t, col, values, []int64{-5, 5, 15, 205, 1000})
	})

	t.Run("string", func(t *testing.T) {
		col := newChunkedStringColumn(NewColumnDef("k", "K", "thing"), testChunkSize)
		values := make([]string, n)
		for i := range values {
			values[i] = fmt.Sprintf("id-%03d", i*10)
			col.Append(values[i])
		}
		col.FinalizeColumn()
		if !col.IsKey() || !col.SortedBySelf() || col.valueIndex != nil {
			t.Fatalf("want sorted map-free key column; got isKey=%v sorted=%v map=%v",
				col.IsKey(), col.SortedBySelf(), col.valueIndex != nil)
		}
		checkKeyLookups[string](t, col, values, []string{"", "id-005", "id-105", "zzz"})
	})

	t.Run("uint64", func(t *testing.T) {
		col := newChunkedUint64Column(NewColumnDef("k", "K", "thing"), testChunkSize)
		values := make([]uint64, n)
		for i := range values {
			values[i] = uint64(i)*3 + 1
			col.Append(values[i])
		}
		col.FinalizeColumn()
		if !col.IsKey() || !col.SortedBySelf() || col.valueIndex != nil {
			t.Fatal("want sorted map-free key column")
		}
		checkKeyLookups[uint64](t, col, values, []uint64{0, 2, 999})
	})

	t.Run("uint32", func(t *testing.T) {
		col := newChunkedUint32Column(NewColumnDef("k", "K", "thing"), testChunkSize)
		values := make([]uint32, n)
		for i := range values {
			values[i] = uint32(i) * 2
			col.Append(values[i])
		}
		col.FinalizeColumn()
		if !col.IsKey() || !col.SortedBySelf() || col.valueIndex != nil {
			t.Fatal("want sorted map-free key column")
		}
		checkKeyLookups[uint32](t, col, values, []uint32{1, 41, 1 << 30})
	})

	t.Run("float64", func(t *testing.T) {
		col := newChunkedFloat64Column(NewColumnDef("k", "K", "thing"), testChunkSize)
		values := make([]float64, n)
		for i := range values {
			values[i] = float64(i) + 0.5
			col.Append(values[i])
		}
		col.FinalizeColumn()
		if !col.IsKey() || !col.SortedBySelf() || col.valueIndex != nil {
			t.Fatal("want sorted map-free key column")
		}
		checkKeyLookups[float64](t, col, values, []float64{0, 1.25, 99, math.NaN()})
	})

	t.Run("datetime", func(t *testing.T) {
		col := newChunkedDatetimeColumn(NewColumnDef("k", "K", "thing"), testChunkSize)
		base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		values := make([]time.Time, n)
		for i := range values {
			values[i] = base.Add(time.Duration(i) * time.Hour)
			col.Append(values[i])
		}
		col.FinalizeColumn()
		if !col.IsKey() || !col.SortedBySelf() || col.valueIndex != nil {
			t.Fatal("want sorted map-free key column")
		}
		checkKeyLookups[time.Time](t, col, values,
			[]time.Time{base.Add(-time.Hour), base.Add(30 * time.Minute), base.Add(999 * time.Hour)})
	})
}

// TestSortedKeyLookupZeroSigns pins canonical-key equality on the sparse
// path: -0 and +0 are the same key, so a stored -0 is found when looking up
// +0 (and vice versa), exactly as the map path behaved.
func TestSortedKeyLookupZeroSigns(t *testing.T) {
	col := newChunkedFloat64Column(NewColumnDef("k", "K", "thing"), 4)
	values := []float64{-10, math.Copysign(0, -1), 10, 20, 30}
	for _, v := range values {
		col.Append(v)
	}
	col.FinalizeColumn()
	if !col.IsKey() || !col.SortedBySelf() || col.valueIndex != nil {
		t.Fatal("want sorted map-free key column")
	}
	for _, probe := range []float64{0, math.Copysign(0, -1)} {
		got, err := col.GetIndex(probe)
		if err != nil || got != 1 {
			t.Errorf("GetIndex(%v) = %d, %v; want row 1 (stored -0)", probe, got, err)
		}
	}
}

// TestSortedColumnWithNaNIsNotKey: NaN disqualifies keys on the sorted
// detection path exactly as on the map path, even though cmp.Compare orders
// NaN first (the data can still read as sorted).
func TestSortedColumnWithNaNIsNotKey(t *testing.T) {
	col := newChunkedFloat64Column(NewColumnDef("k", "K", "thing"), 4)
	for _, v := range []float64{math.NaN(), 1, 2, 3} {
		col.Append(v)
	}
	col.FinalizeColumn()
	if col.IsKey() {
		t.Fatal("column containing NaN must not be a key")
	}
	if _, err := col.GetIndex(2); err == nil {
		t.Error("GetIndex on a non-key column should error")
	}
}

// TestUnsortedKeyColumnKeepsMap: the reverse-lookup map remains the lookup
// path for key columns whose storage is not sorted by them — no behavior or
// memory change for that case.
func TestUnsortedKeyColumnKeepsMap(t *testing.T) {
	col := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), testChunkSize)
	rng := rand.New(rand.NewSource(1))
	perm := rng.Perm(50)
	values := make([]int64, len(perm))
	for i, p := range perm {
		values[i] = int64(p * 7)
		col.Append(values[i])
	}
	col.FinalizeColumn()
	if !col.IsKey() {
		t.Fatal("unique column not detected as key")
	}
	if col.SortedBySelf() {
		t.Fatal("shuffled column recorded as SortedBySelf")
	}
	if col.valueIndex == nil {
		t.Fatal("unsorted key column must keep its reverse-lookup map")
	}
	checkKeyLookups[int64](t, col, values, []int64{-1, 1, 1000})
}

// TestSortedNonUniqueColumnIsNotKey: adjacent duplicates on sorted storage
// are detected without a map; the column is not a key and reverse lookup
// errors.
func TestSortedNonUniqueColumnIsNotKey(t *testing.T) {
	col := newChunkedInt64Column(NewColumnDef("d", "D", "thing"), 4)
	for _, v := range []int64{1, 2, 2, 3, 4} {
		col.Append(v)
	}
	col.FinalizeColumn()
	if col.IsKey() {
		t.Fatal("column with duplicates detected as key")
	}
	if !col.SortedBySelf() {
		t.Fatal("sorted non-unique column should still record SortedBySelf")
	}
	if _, err := col.GetIndex(2); err == nil {
		t.Error("GetIndex on a non-key column should error")
	}
}

// TestSortedUniqueNonEntityColumn: without an entity type there is no reverse
// lookup, sorted or not — parity with the map path, which never kept the map
// for entity-less columns.
func TestSortedUniqueNonEntityColumn(t *testing.T) {
	col := newChunkedInt64Column(NewColumnDef("k", "K", ""), 4)
	for i := int64(0); i < 10; i++ {
		col.Append(i)
	}
	col.FinalizeColumn()
	if !col.IsKey() || !col.SortedBySelf() {
		t.Fatal("want sorted unique column")
	}
	if _, err := col.GetIndex(3); err == nil {
		t.Error("GetIndex without an entity type should error, as it always has")
	}
}

// TestSparseIndexEdgeSizes: empty, single-value, and exactly-one-chunk
// columns behave like their map-backed equivalents.
func TestSparseIndexEdgeSizes(t *testing.T) {
	empty := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), 4)
	empty.FinalizeColumn()
	if _, err := empty.GetIndex(1); err == nil {
		t.Error("GetIndex on empty column should error")
	}

	one := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), 4)
	one.Append(42)
	one.FinalizeColumn()
	if !one.IsKey() || !one.SortedBySelf() || one.valueIndex != nil {
		t.Fatal("single-value column should be a sorted map-free key")
	}
	checkKeyLookups[int64](t, one, []int64{42}, []int64{41, 43})

	full := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), 4)
	vals := []int64{1, 2, 3, 4}
	for _, v := range vals {
		full.Append(v)
	}
	full.FinalizeColumn()
	checkKeyLookups[int64](t, full, vals, []int64{0, 5})
}

// TestReorderYieldsSparseKey: sorting a shuffled unique column through the 4a
// Reorder path produces a copy that is detected as sorted at its finalize —
// the load-time sort is exactly where the map is supposed to disappear.
func TestReorderYieldsSparseKey(t *testing.T) {
	col := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), testChunkSize)
	rng := rand.New(rand.NewSource(7))
	perm := rng.Perm(100)
	for _, p := range perm {
		col.Append(int64(p))
	}
	col.FinalizeColumn()
	if col.SortedBySelf() || col.valueIndex == nil {
		t.Fatal("precondition: shuffled column should be map-backed")
	}

	// Build the sorting permutation: sortPerm[i] = row holding value i.
	sortPerm := make([]uint32, len(perm))
	for row, p := range perm {
		sortPerm[p] = uint32(row)
	}
	sorted := col.Reorder(sortPerm).(*ChunkedInt64Column)
	if !sorted.IsKey() || !sorted.SortedBySelf() || sorted.valueIndex != nil {
		t.Fatal("reordered-to-sorted column should be a map-free key")
	}
	values := make([]int64, len(perm))
	for i := range values {
		values[i] = int64(i)
	}
	checkKeyLookups[int64](t, sorted, values, []int64{-1, 100})
}

// TestSortedDictKeyColumn: a dictionary key column on sorted storage releases
// its interning map; GetIndex and the structured filters look values up by
// binary search over the (sorted) dictionary.
func TestSortedDictKeyColumn(t *testing.T) {
	dict := newChunkedDictStringColumn[uint16](NewColumnDef("k", "K", "thing"), 4)
	plain := newChunkedStringColumn(NewColumnDef("k", "K", "thing"), 4)
	values := make([]string, 21)
	for i := range values {
		values[i] = fmt.Sprintf("id-%03d", i*10)
		dict.Append(values[i])
		plain.Append(values[i])
	}
	dict.FinalizeColumn()
	plain.FinalizeColumn()

	if !dict.IsKey() || !dict.SortedBySelf() {
		t.Fatal("want sorted dict key column")
	}
	if dict.index != nil {
		t.Fatal("sorted dict key column should release its interning map")
	}
	checkKeyLookups[string](t, dict, values, []string{"", "id-005", "zzz"})

	// Structured filters route through lookupCode's binary-search branch.
	for _, probe := range []string{"id-000", "id-100", "id-200", "absent"} {
		want := plain.FilterSelectionEqual(probe).ToIndices()
		got := dict.FilterSelectionEqual(probe).ToIndices()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("FilterSelectionEqual(%q): dict %v, plain %v", probe, got, want)
		}
	}
	wantIn := plain.FilterSelectionIn([]string{"id-010", "id-190", "absent"}).ToIndices()
	gotIn := dict.FilterSelectionIn([]string{"id-010", "id-190", "absent"}).ToIndices()
	if fmt.Sprint(gotIn) != fmt.Sprint(wantIn) {
		t.Errorf("FilterSelectionIn: dict %v, plain %v", gotIn, wantIn)
	}
}

// TestUnsortedDictKeyColumnKeepsMap: an unsorted dict key column keeps its
// interning map and its lookups, unchanged.
func TestUnsortedDictKeyColumnKeepsMap(t *testing.T) {
	dict := newChunkedDictStringColumn[uint16](NewColumnDef("k", "K", "thing"), 4)
	values := []string{"delta", "alpha", "echo", "bravo", "charlie"}
	for _, v := range values {
		dict.Append(v)
	}
	dict.FinalizeColumn()
	if !dict.IsKey() || dict.SortedBySelf() {
		t.Fatal("want unsorted dict key column")
	}
	if dict.index == nil {
		t.Fatal("unsorted dict key column must keep its interning map")
	}
	checkKeyLookups[string](t, dict, values, []string{"foxtrot", ""})
}

// TestSortedNonKeyDictColumn: sorted storage is recorded on non-key dict
// columns too, and value lookups (filters) binary-search the dictionary
// rather than scanning it.
func TestSortedNonKeyDictColumn(t *testing.T) {
	dict := newChunkedDictStringColumn[uint8](NewColumnDef("d", "D", ""), 4)
	for _, v := range []string{"a", "a", "b", "b", "b", "c", "c"} {
		dict.Append(v)
	}
	dict.FinalizeColumn()
	if dict.IsKey() {
		t.Fatal("repetitive column detected as key")
	}
	if !dict.SortedBySelf() {
		t.Fatal("sorted dict column not recorded as SortedBySelf")
	}
	got := dict.FilterSelectionEqual("b").ToIndices()
	if fmt.Sprint(got) != fmt.Sprint([]uint32{2, 3, 4}) {
		t.Errorf("FilterSelectionEqual(b) = %v, want [2 3 4]", got)
	}
	if n := dict.FilterSelectionEqual("absent").Count(); n != 0 {
		t.Errorf("FilterSelectionEqual(absent) matched %d rows", n)
	}
}
