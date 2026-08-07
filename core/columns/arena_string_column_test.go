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
	"reflect"
	"sort"
	"strings"
	"testing"
)

// arenaTestValues exercises empty strings, duplicates, multi-byte runes and
// values spanning chunk boundaries at the test chunk size of 8.
var arenaTestValues = []string{
	"delta", "alpha", "", "charlie", "alpha", "Ĝamma", "bravo", "echo",
	"foxtrot", "", "golf", "hotel", "alpha", "india", "juliett", "kilo",
	"lima", // partial last chunk
}

func buildArenaPlainPair(values []string, chunkSize int, entityType string, finalize bool) (*ChunkedArenaStringColumn, *ChunkedStringColumn) {
	a := newChunkedArenaStringColumn(NewColumnDef("col", "Col", entityType), chunkSize)
	p := newChunkedStringColumn(NewColumnDef("col", "Col", entityType), chunkSize)
	for _, v := range values {
		a.Append(v)
		p.Append(v)
	}
	if finalize {
		a.FinalizeColumn()
		p.FinalizeColumn()
	}
	return a, p
}

func TestArenaStringAccessParity(t *testing.T) {
	a, p := buildArenaPlainPair(arenaTestValues, 8, "", true)
	if a.Length() != p.Length() {
		t.Fatalf("Length: arena %d, plain %d", a.Length(), p.Length())
	}
	for i := uint32(0); i < uint32(p.Length()); i++ {
		av, aerr := a.GetString(i)
		pv, perr := p.GetString(i)
		if av != pv || (aerr == nil) != (perr == nil) {
			t.Errorf("GetString(%d): arena (%q, %v), plain (%q, %v)", i, av, aerr, pv, perr)
		}
	}
	if _, err := a.GetValue(uint32(a.Length())); err == nil {
		t.Error("GetValue past the end should error")
	}
	if a.NumChunks() != p.NumChunks() || a.ChunkSize() != p.ChunkSize() || a.ChunkLen(a.NumChunks()-1) != p.ChunkLen(p.NumChunks()-1) {
		t.Errorf("chunk shape mismatch: arena (%d chunks of %d, last %d), plain (%d of %d, last %d)",
			a.NumChunks(), a.ChunkSize(), a.ChunkLen(a.NumChunks()-1), p.NumChunks(), p.ChunkSize(), p.ChunkLen(p.NumChunks()-1))
	}
}

func TestArenaFinalizeKeyDetectionParity(t *testing.T) {
	distinct := make([]string, 100)
	for i := range distinct {
		distinct[i] = fmt.Sprintf("v-%03d", (i*37)%100) // shuffled, unique
	}
	sorted := make([]string, 100)
	for i := range sorted {
		sorted[i] = fmt.Sprintf("v-%03d", i)
	}

	cases := []struct {
		name       string
		values     []string
		entity     string
		wantKey    bool
		wantSorted bool
		wantMap    bool
	}{
		{"unsorted key entity", distinct, "test.e", true, false, true},
		{"unsorted key no entity", distinct, "", true, false, false},
		{"sorted key entity", sorted, "test.e", true, true, false},
		{"duplicates", arenaTestValues, "test.e", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, p := buildArenaPlainPair(tc.values, 8, tc.entity, true)
			if a.IsKey() != tc.wantKey || p.IsKey() != tc.wantKey {
				t.Errorf("IsKey: arena %v, plain %v, want %v", a.IsKey(), p.IsKey(), tc.wantKey)
			}
			if a.SortedBySelf() != tc.wantSorted || p.SortedBySelf() != tc.wantSorted {
				t.Errorf("SortedBySelf: arena %v, plain %v, want %v", a.SortedBySelf(), p.SortedBySelf(), tc.wantSorted)
			}
			if (a.valueIndex != nil) != tc.wantMap {
				t.Errorf("valueIndex present = %v, want %v", a.valueIndex != nil, tc.wantMap)
			}
			for i := uint32(0); i < uint32(len(tc.values)); i++ {
				v := tc.values[i]
				ai, aerr := a.GetIndex(v)
				pi, perr := p.GetIndex(v)
				if (aerr == nil) != (perr == nil) || (aerr == nil && ai != pi) {
					t.Fatalf("GetIndex(%q): arena (%d, %v), plain (%d, %v)", v, ai, aerr, pi, perr)
				}
			}
			for _, absent := range []string{"", "a", "v-", "v-100", "zzz"} {
				if sort.SearchStrings(tc.values, absent) < len(tc.values) && tc.values[sort.SearchStrings(tc.values, absent)] == absent {
					continue
				}
				_, aerr := a.GetIndex(absent)
				_, perr := p.GetIndex(absent)
				if (aerr == nil) != (perr == nil) {
					t.Errorf("GetIndex(%q) absent: arena err %v, plain err %v", absent, aerr, perr)
				}
			}
		})
	}
}

func TestArenaFilterParity(t *testing.T) {
	for _, finalize := range []bool{true, false} {
		a, p := buildArenaPlainPair(arenaTestValues, 8, "", finalize)
		preds := map[string]func(string) bool{
			"contains-a": func(s string) bool { return strings.Contains(s, "a") },
			"empty":      func(s string) bool { return s == "" },
			"none":       func(string) bool { return false },
		}
		for name, pred := range preds {
			if got, want := a.FilterSelection(pred).ToIndices(), p.FilterSelection(pred).ToIndices(); !reflect.DeepEqual(got, want) {
				t.Errorf("finalize=%v FilterSelection(%s): arena %v, plain %v", finalize, name, got, want)
			}
		}
		lo, hi := "b", "f"
		structured := map[string]*Selection{
			"Equal(alpha)":   a.FilterSelectionEqual("alpha"),
			"Equal(absent)":  a.FilterSelectionEqual("zulu"),
			"Equal(empty)":   a.FilterSelectionEqual(""),
			"In(a,gv,miss)":  a.FilterSelectionIn([]string{"alpha", "golf", "zulu"}),
			"In(empty list)": a.FilterSelectionIn(nil),
			"Range[b,f]":     a.FilterSelectionRange(&lo, &hi),
			"Range[nil,f]":   a.FilterSelectionRange(nil, &hi),
			"Range[b,nil]":   a.FilterSelectionRange(&lo, nil),
			"Range[nil,nil]": a.FilterSelectionRange(nil, nil),
		}
		naive := map[string]func(string) bool{
			"Equal(alpha)":   func(s string) bool { return s == "alpha" },
			"Equal(absent)":  func(s string) bool { return s == "zulu" },
			"Equal(empty)":   func(s string) bool { return s == "" },
			"In(a,gv,miss)":  func(s string) bool { return s == "alpha" || s == "golf" || s == "zulu" },
			"In(empty list)": func(string) bool { return false },
			"Range[b,f]":     func(s string) bool { return s >= lo && s <= hi },
			"Range[nil,f]":   func(s string) bool { return s <= hi },
			"Range[b,nil]":   func(s string) bool { return s >= lo },
			"Range[nil,nil]": func(string) bool { return true },
		}
		for name, sel := range structured {
			if got, want := sel.ToIndices(), p.FilterSelection(naive[name]).ToIndices(); !reflect.DeepEqual(got, want) {
				t.Errorf("finalize=%v %s: arena %v, plain naive %v", finalize, name, got, want)
			}
		}
	}
}

// TestArenaZoneSkipWhiteBox proves chunks are genuinely pruned: a matching
// value planted into a finalized chunk (by overwriting same-length blob
// bytes) is invisible to the structured filters, whose zone maps exclude the
// chunk, but visible to the naive scan.
func TestArenaZoneSkipWhiteBox(t *testing.T) {
	a := newChunkedArenaStringColumn(NewColumnDef("col", "Col", ""), 8)
	for i := 0; i < 16; i++ {
		a.Append(fmt.Sprintf("m-%02d", i)) // chunk 0: m-00..m-07, chunk 1: m-08..m-15
	}
	a.FinalizeColumn()
	copy(a.chunks[0].blob[0:4], "z-99") // plant into chunk 0; bounds still say m-00..m-07
	if got := a.FilterSelection(func(s string) bool { return s == "z-99" }).Count(); got != 1 {
		t.Fatalf("naive scan found %d planted rows, want 1", got)
	}
	if got := a.FilterSelectionEqual("z-99").Count(); got != 0 {
		t.Errorf("pruned equality found the planted row; chunk was not skipped")
	}
	if got := a.FilterSelectionIn([]string{"z-99"}).Count(); got != 0 {
		t.Errorf("pruned In found the planted row; chunk was not skipped")
	}
	lo := "z"
	if got := a.FilterSelectionRange(&lo, nil).Count(); got != 0 {
		t.Errorf("pruned range found the planted row; chunk was not skipped")
	}
}

func collectAggPairs(ops IGroupOps, sel RowSet) [][2]uint32 {
	rec := &aggPairRecorder{}
	ops.GroupAggregates(sel, rec)
	return rec.pairs
}

func groupOpsParity(t *testing.T, name string, a, p IGroupOps, sel RowSet) {
	t.Helper()
	ac, af := a.GroupCounts(sel)
	pc, pf := p.GroupCounts(sel)
	if !reflect.DeepEqual(ac, pc) || !reflect.DeepEqual(af, pf) {
		t.Errorf("%s GroupCounts: arena (%v, %v), plain (%v, %v)", name, ac, af, pc, pf)
		return
	}
	if got, want := collectAggPairs(a, sel), collectAggPairs(p, sel); !reflect.DeepEqual(got, want) {
		t.Errorf("%s GroupAggregates: arena %v, plain %v", name, got, want)
	}
	for code := range ac {
		for _, page := range [][2]int{{0, -1}, {1, 2}, {0, 1}, {0, 0}} {
			got := a.GroupMembers(sel, uint32(code), page[0], page[1])
			want := p.GroupMembers(sel, uint32(code), page[0], page[1])
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s GroupMembers(code %d, off %d, n %d): arena %v, plain %v", name, code, page[0], page[1], got, want)
			}
		}
	}
}

func TestArenaGroupOpsParity(t *testing.T) {
	a, p := buildArenaPlainPair(arenaTestValues, 8, "", true)
	n := len(arenaTestValues)
	bitmap := NewSelection(n)
	for i := 0; i < n; i += 2 {
		bitmap.Add(uint32(i))
	}
	sels := map[string]RowSet{
		"AllRows":    AllRows(n),
		"RowIndices": RowIndices{3, 1, 8, 12, 4, 1, 40}, // out of order, repeated, out of range
		"Selection":  bitmap,
	}
	for name, sel := range sels {
		groupOpsParity(t, name, a, p, sel)
	}
}

func TestArenaGroupIndicesParity(t *testing.T) {
	a, p := buildArenaPlainPair(arenaTestValues, 8, "", true)
	indices := []uint32{0, 3, 1, 4, 2, 12, 15, 40}
	ag, au := a.GroupIndices(indices, nil)
	pg, pu := p.GroupIndices(indices, nil)
	if !reflect.DeepEqual(ag, pg) || !reflect.DeepEqual(au, pu) {
		t.Errorf("GroupIndices: arena (%v, %v), plain (%v, %v)", ag, au, pg, pu)
	}
}

func TestArenaCompareRowsParity(t *testing.T) {
	a, p := buildArenaPlainPair(arenaTestValues, 8, "", true)
	for i := uint32(0); i < uint32(a.Length()); i++ {
		for j := uint32(0); j < uint32(a.Length()); j++ {
			if got, want := sign(a.CompareRows(i, j)), sign(p.CompareRows(i, j)); got != want {
				t.Fatalf("CompareRows(%d, %d): arena %d, plain %d", i, j, got, want)
			}
		}
	}
}

func TestArenaReorder(t *testing.T) {
	values := make([]string, 20)
	for i := range values {
		values[i] = fmt.Sprintf("k-%02d", i)
	}
	shuffled := make([]string, len(values))
	for i := range values {
		shuffled[i] = values[(i*7)%len(values)]
	}
	a, p := buildArenaPlainPair(shuffled, 8, "test.e", true)

	// The permutation that sorts the shuffled values.
	perm := make([]uint32, len(values))
	for i := range perm {
		perm[i] = uint32(i)
	}
	sort.Slice(perm, func(x, y int) bool { return shuffled[perm[x]] < shuffled[perm[y]] })

	ra := a.Reorder(perm).(*ChunkedArenaStringColumn)
	rp := p.Reorder(perm).(*ChunkedStringColumn)
	for i := uint32(0); i < uint32(len(values)); i++ {
		av, _ := ra.GetString(i)
		pv, _ := rp.GetString(i)
		if av != pv || av != values[i] {
			t.Fatalf("row %d after reorder: arena %q, plain %q, want %q", i, av, pv, values[i])
		}
	}
	if !ra.IsKey() || !ra.SortedBySelf() {
		t.Errorf("reordered-to-sorted arena column: IsKey %v, SortedBySelf %v, want true, true", ra.IsKey(), ra.SortedBySelf())
	}
	if ra.valueIndex != nil {
		t.Error("sorted arena column retains a reverse-lookup map; the sparse search should serve lookups")
	}
	if idx, err := ra.GetIndex("k-13"); err != nil || idx != 13 {
		t.Errorf("GetIndex on reordered column = (%d, %v), want (13, nil)", idx, err)
	}
}

func TestArenaEncodePreservesState(t *testing.T) {
	t.Run("unfinalized stays unfinalized", func(t *testing.T) {
		src := newChunkedStringColumn(NewColumnDef("c", "C", "test.e"), 8)
		for i := 0; i < 20; i++ {
			src.Append(fmt.Sprintf("u-%02d", i))
		}
		a := ArenaEncodeChunkedStringColumn(src)
		if a.zones != nil || a.IsKey() || a.valueIndex != nil {
			t.Errorf("unfinalized source produced finalized arena state: zones=%v key=%v map=%v", a.zones != nil, a.IsKey(), a.valueIndex != nil)
		}
		for i := uint32(0); i < 20; i++ {
			want, _ := src.GetString(i)
			if got, _ := a.GetString(i); got != want {
				t.Fatalf("row %d: %q, want %q", i, got, want)
			}
		}
	})
	t.Run("finalized unsorted key keeps lookups", func(t *testing.T) {
		src := newChunkedStringColumn(NewColumnDef("c", "C", "test.e"), 8)
		for i := 0; i < 20; i++ {
			src.Append(fmt.Sprintf("u-%02d", (i*7)%20))
		}
		src.FinalizeColumn()
		a := ArenaEncodeChunkedStringColumn(src)
		if !a.IsKey() || a.SortedBySelf() || a.valueIndex == nil {
			t.Fatalf("state: key=%v sorted=%v map=%v, want key, unsorted, map", a.IsKey(), a.SortedBySelf(), a.valueIndex != nil)
		}
		for i := 0; i < 20; i++ {
			v := fmt.Sprintf("u-%02d", i)
			ai, aerr := a.GetIndex(v)
			si, serr := src.GetIndex(v)
			if aerr != nil || serr != nil || ai != si {
				t.Fatalf("GetIndex(%q): arena (%d, %v), src (%d, %v)", v, ai, aerr, si, serr)
			}
		}
	})
	t.Run("chunk size preserved and empty ok", func(t *testing.T) {
		src := newChunkedStringColumn(NewColumnDef("c", "C", ""), 32)
		a := ArenaEncodeChunkedStringColumn(src)
		if a.ChunkSize() != 32 || a.Length() != 0 || a.NumChunks() != 0 {
			t.Errorf("empty encode: chunkSize %d, length %d, chunks %d", a.ChunkSize(), a.Length(), a.NumChunks())
		}
	})
}
