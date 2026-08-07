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

// buildFrontCoded builds a plain sorted key column from the given
// already-sorted distinct values and front-codes it. Chunk size 32 puts two
// restart spans in every full chunk.
func buildFrontCoded(t *testing.T, values []string, entityType string) (*ChunkedFrontCodedStringColumn, *ChunkedStringColumn) {
	t.Helper()
	p := newChunkedStringColumn(NewColumnDef("pk", "PK", entityType), 32)
	for _, v := range values {
		p.Append(v)
	}
	p.FinalizeColumn()
	if !p.IsKey() || !p.SortedBySelf() {
		t.Fatalf("test data is not a sorted key: IsKey %v, SortedBySelf %v", p.IsKey(), p.SortedBySelf())
	}
	fc, ok := FrontCodeChunkedStringColumn(p)
	if !ok {
		t.Fatal("FrontCodeChunkedStringColumn declined a sorted key column")
	}
	return fc, p
}

func sortedIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("cust_%06d", i*3)
	}
	return ids
}

// mixedSortedValues has an empty first string, varied shared prefixes,
// prefix-of-successor pairs and multi-byte runes — sorted and distinct.
var mixedSortedValues = []string{
	"", "a", "aa", "aab", "ab", "abc", "abcd", "b",
	"ba", "bb", "bb0", "bb1", "bb10", "bb2", "cheese", "cheesecake",
	"d", "dd", "ddd, the longest value in the column by a margin", "de",
	"z", "za", "É", "Éclair",
}

func TestFrontCodedRoundTrip(t *testing.T) {
	cases := map[string][]string{
		"sequential ids": sortedIDs(100), // 3 chunks of 32 + partial
		"mixed":          mixedSortedValues,
		"single":         {"only"},
		"restart edges":  sortedIDs(33), // 32 = exactly one chunk, +1 spills
		"one span":       sortedIDs(15),
		"span boundary":  sortedIDs(16),
		"span plus one":  sortedIDs(17),
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			fc, _ := buildFrontCoded(t, values, "test.e")
			if fc.Length() != len(values) {
				t.Fatalf("Length = %d, want %d", fc.Length(), len(values))
			}
			for i, want := range values {
				if got, err := fc.GetString(uint32(i)); err != nil || got != want {
					t.Fatalf("GetString(%d) = (%q, %v), want %q", i, got, err, want)
				}
			}
			if _, err := fc.GetValue(uint32(len(values))); err == nil {
				t.Error("GetValue past the end should error")
			}
			if !fc.IsKey() || !fc.SortedBySelf() {
				t.Errorf("IsKey %v, SortedBySelf %v, want true, true", fc.IsKey(), fc.SortedBySelf())
			}
		})
	}
}

func TestFrontCodeRefusal(t *testing.T) {
	unsorted := newChunkedStringColumn(NewColumnDef("c", "C", "test.e"), 32)
	for i := 0; i < 40; i++ {
		unsorted.Append(fmt.Sprintf("v-%02d", (i*7)%40))
	}
	unsorted.FinalizeColumn()

	dups := newChunkedStringColumn(NewColumnDef("c", "C", "test.e"), 32)
	for i := 0; i < 40; i++ {
		dups.Append(fmt.Sprintf("v-%02d", i/2))
	}
	dups.FinalizeColumn()

	unfinalized := newChunkedStringColumn(NewColumnDef("c", "C", "test.e"), 32)
	for i := 0; i < 40; i++ {
		unfinalized.Append(fmt.Sprintf("v-%02d", i))
	}

	for name, src := range map[string]*ChunkedStringColumn{
		"unsorted key": unsorted, "sorted duplicates": dups, "unfinalized": unfinalized,
	} {
		if _, ok := FrontCodeChunkedStringColumn(src); ok {
			t.Errorf("%s: front coding accepted a column outside the string-PK role", name)
		}
	}
}

func TestFrontCodedGetIndex(t *testing.T) {
	for name, values := range map[string][]string{"ids": sortedIDs(200), "mixed": mixedSortedValues} {
		t.Run(name, func(t *testing.T) {
			fc, p := buildFrontCoded(t, values, "test.e")
			for i, v := range values {
				got, err := fc.GetIndex(v)
				if err != nil || got != uint32(i) {
					t.Fatalf("GetIndex(%q) = (%d, %v), want (%d, nil)", v, got, err, i)
				}
				want, perr := p.GetIndex(v)
				if perr != nil || want != got {
					t.Fatalf("GetIndex(%q) parity: fc %d, plain (%d, %v)", v, got, want, perr)
				}
			}
			absents := []string{"cust_000001", "cust_999999", "aA", "bb11", "cheesecak", "cheesecakes", "zz", "\x00"}
			for _, v := range absents {
				if i := sort.SearchStrings(values, v); i < len(values) && values[i] == v {
					continue
				}
				if _, err := fc.GetIndex(v); err == nil {
					t.Errorf("GetIndex(%q) found an absent value", v)
				}
			}
		})
	}
	t.Run("entityless errors", func(t *testing.T) {
		fc, _ := buildFrontCoded(t, sortedIDs(40), "")
		if _, err := fc.GetIndex("cust_000000"); err == nil {
			t.Error("GetIndex on an entityless column should error, mirroring the other storage columns")
		}
	})
}

func TestFrontCodedStructuredFilters(t *testing.T) {
	values := mixedSortedValues
	fc, p := buildFrontCoded(t, values, "test.e")

	naiveEqual := func(v string) func(string) bool {
		return func(s string) bool { return s == v }
	}
	for _, v := range []string{"", "a", "bb10", "Éclair", "absent", "aaa"} {
		if got, want := fc.FilterSelectionEqual(v).ToIndices(), p.FilterSelection(naiveEqual(v)).ToIndices(); !reflect.DeepEqual(got, want) {
			t.Errorf("Equal(%q): fc %v, naive %v", v, got, want)
		}
	}
	in := []string{"a", "cheese", "zzz", ""}
	inSet := map[string]bool{"a": true, "cheese": true, "zzz": true, "": true}
	if got, want := fc.FilterSelectionIn(in).ToIndices(), p.FilterSelection(func(s string) bool { return inSet[s] }).ToIndices(); !reflect.DeepEqual(got, want) {
		t.Errorf("In(%v): fc %v, naive %v", in, got, want)
	}
	if got := fc.FilterSelectionIn(nil).Count(); got != 0 {
		t.Errorf("In(nil) selected %d rows, want 0", got)
	}

	bounds := []string{"", "a", "ab", "b", "bb1", "bb10", "cheese", "zz", "É", "\x00", "~"}
	for _, lo := range bounds {
		for _, hi := range bounds {
			lo, hi := lo, hi
			cases := map[string]struct {
				lo, hi *string
				pred   func(string) bool
			}{
				"both": {&lo, &hi, func(s string) bool { return s >= lo && s <= hi }},
				"lo":   {&lo, nil, func(s string) bool { return s >= lo }},
				"hi":   {nil, &hi, func(s string) bool { return s <= hi }},
			}
			for name, tc := range cases {
				got := fc.FilterSelectionRange(tc.lo, tc.hi).ToIndices()
				want := p.FilterSelection(tc.pred).ToIndices()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("Range(%s, lo=%q hi=%q): fc %v, naive %v", name, lo, hi, got, want)
				}
			}
		}
	}
	if got, want := fc.FilterSelectionRange(nil, nil).Count(), len(values); got != want {
		t.Errorf("Range(nil, nil) = %d rows, want %d", got, want)
	}
}

func TestFrontCodedFilterSelection(t *testing.T) {
	fc, p := buildFrontCoded(t, mixedSortedValues, "test.e")
	preds := map[string]func(string) bool{
		"contains-b": func(s string) bool { return strings.Contains(s, "b") },
		"empty":      func(s string) bool { return s == "" },
		"len>3":      func(s string) bool { return len(s) > 3 },
	}
	for name, pred := range preds {
		if got, want := fc.FilterSelection(pred).ToIndices(), p.FilterSelection(pred).ToIndices(); !reflect.DeepEqual(got, want) {
			t.Errorf("FilterSelection(%s): fc %v, plain %v", name, got, want)
		}
	}
}

func TestFrontCodedGroupOps(t *testing.T) {
	values := sortedIDs(70)
	fc, p := buildFrontCoded(t, values, "test.e")
	n := len(values)
	bitmap := NewSelection(n)
	for i := 0; i < n; i += 3 {
		bitmap.Add(uint32(i))
	}
	sels := map[string]RowSet{
		"AllRows":    AllRows(n),
		"RowIndices": RowIndices{5, 40, 33, 0, 69, 200}, // out of order, out of range
		"Selection":  bitmap,
	}
	for name, sel := range sels {
		groupOpsParity(t, name, fc, p, sel)
	}
	// Paging edge cases on the single-member groups.
	if got := fc.GroupMembers(AllRows(n), 3, 1, -1); got != nil {
		t.Errorf("GroupMembers(offset 1) = %v, want nil — groups have one member", got)
	}
	if got := fc.GroupMembers(AllRows(n), 3, 0, 0); got != nil {
		t.Errorf("GroupMembers(n=0) = %v, want nil", got)
	}
}

func TestFrontCodedCompareRows(t *testing.T) {
	fc, _ := buildFrontCoded(t, mixedSortedValues, "test.e")
	for i := uint32(0); i < uint32(fc.Length()); i++ {
		for j := uint32(0); j < uint32(fc.Length()); j++ {
			want := sign(strings.Compare(mixedSortedValues[i], mixedSortedValues[j]))
			if got := sign(fc.CompareRows(i, j)); got != want {
				t.Fatalf("CompareRows(%d, %d) = %d, want %d", i, j, got, want)
			}
		}
	}
}

func TestFrontCodedChunkBounds(t *testing.T) {
	values := sortedIDs(70) // chunks of 32: [0,32), [32,64), [64,70)
	fc, _ := buildFrontCoded(t, values, "test.e")
	for ci := 0; ci < fc.NumChunks(); ci++ {
		lo := ci * 32
		hi := lo + fc.ChunkLen(ci) - 1
		min, max, ok := fc.ChunkBounds(ci)
		if !ok || min != values[lo] || max != values[hi] {
			t.Errorf("ChunkBounds(%d) = (%q, %q, %v), want (%q, %q, true)", ci, min, max, ok, values[lo], values[hi])
		}
	}
}

func TestFrontCodedGroupIndicesParity(t *testing.T) {
	values := sortedIDs(40)
	fc, p := buildFrontCoded(t, values, "test.e")
	indices := []uint32{0, 7, 3, 39, 12, 100}
	fg, fu := fc.GroupIndices(indices, nil)
	pg, pu := p.GroupIndices(indices, nil)
	if !reflect.DeepEqual(fg, pg) || !reflect.DeepEqual(fu, pu) {
		t.Errorf("GroupIndices: fc (%v, %v), plain (%v, %v)", fg, fu, pg, pu)
	}
}

func TestFrontCodedReorder(t *testing.T) {
	values := sortedIDs(40)
	fc, _ := buildFrontCoded(t, values, "test.e")

	reverse := make([]uint32, len(values))
	for i := range reverse {
		reverse[i] = uint32(len(values) - 1 - i)
	}
	r := fc.Reorder(reverse).(*ChunkedArenaStringColumn)
	for i := range values {
		if got, _ := r.GetString(uint32(i)); got != values[len(values)-1-i] {
			t.Fatalf("row %d after reverse = %q, want %q", i, got, values[len(values)-1-i])
		}
	}
	if !r.IsKey() || r.SortedBySelf() {
		t.Errorf("reversed copy: IsKey %v, SortedBySelf %v, want true, false", r.IsKey(), r.SortedBySelf())
	}

	identity := make([]uint32, len(values))
	for i := range identity {
		identity[i] = uint32(i)
	}
	s := fc.Reorder(identity).(*ChunkedArenaStringColumn)
	if !s.IsKey() || !s.SortedBySelf() {
		t.Errorf("identity copy: IsKey %v, SortedBySelf %v, want true, true", s.IsKey(), s.SortedBySelf())
	}
}
