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
	"sort"
	"testing"
)

func TestCompareNatural(t *testing.T) {
	less := [][2]string{
		{"k9", "k10"}, {"item-2", "item-10"}, {"2", "10"}, {"a", "a1"}, {"a1", "a1b"},
		{"7", "007"}, {"x7a", "x007"}, {"1.9", "1.10"}, {"", "0"}, {"abc", "abd"},
		{"file9.txt", "file10.txt"}, {"v1.2.9", "v1.2.10"}, {"00", "000"},
	}
	for _, p := range less {
		if c := CompareStrings(CollationNatural, p[0], p[1]); c >= 0 {
			t.Errorf("natural: %q should order before %q (got %d)", p[0], p[1], c)
		}
		if c := CompareStrings(CollationNatural, p[1], p[0]); c <= 0 {
			t.Errorf("natural: %q should order after %q (got %d)", p[1], p[0], c)
		}
	}
	for _, s := range []string{"", "k9", "007", "abc123def"} {
		if c := CompareStrings(CollationNatural, s, s); c != 0 {
			t.Errorf("natural: %q vs itself = %d", s, c)
		}
	}
	// Default stays bytewise.
	if CompareStrings(CollationDefault, "k9", "k10") <= 0 {
		t.Error("default collation must be bytewise (k10 < k9)")
	}
	// Total order: sorting a shuffled list under the comparator is stable
	// and yields the expected sequence.
	want := []string{"a1", "a2", "a10", "a10b", "a11", "b0", "b00", "b1"}
	got := []string{"b1", "a10b", "a2", "b00", "a11", "a1", "b0", "a10"}
	sort.Slice(got, func(i, j int) bool { return CompareStrings(CollationNatural, got[i], got[j]) < 0 })
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted = %v, want %v", got, want)
		}
	}
}

// TestCollationFlowsThroughCompareAtIndex: every string column shape
// honours its definition's collation in CompareAtIndex.
func TestCollationFlowsThroughCompareAtIndex(t *testing.T) {
	vals := []string{"k10", "k9", "k100", "k9"}
	def := func() *ColumnDef {
		d := NewColumnDef("c", "c", "")
		d.SetCollation(CollationNatural)
		return d
	}
	plain := NewStringColumn(def())
	chunked := NewChunkedStringColumn(def())
	for _, v := range vals {
		plain.Append(v)
		chunked.Append(v)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()
	dict, ok := CompactChunkedStringColumn(chunked)
	if !ok {
		// Below the size threshold the column stays raw; build the dict form directly.
		d := NewChunkedDictStringColumn[uint8](def())
		for _, v := range vals {
			d.Append(v)
		}
		d.FinalizeColumn()
		dict = d
	}
	arena := ArenaEncodeChunkedStringColumn(chunked)
	for name, col := range map[string]IDataColumn{"StringColumn": plain, "ChunkedStringColumn": chunked, "dict": dict, "arena": arena} {
		// row1 "k9" < row0 "k10" < row2 "k100"; row1 == row3
		if CompareAtIndex(col, 1, 0) >= 0 || CompareAtIndex(col, 0, 2) >= 0 || CompareAtIndex(col, 1, 3) != 0 {
			t.Errorf("%s (%T) ignores natural collation", name, col)
		}
	}
	// Without the setting the same data orders bytewise.
	raw := NewStringColumn(NewColumnDef("c", "c", ""))
	for _, v := range vals {
		raw.Append(v)
	}
	if CompareAtIndex(raw, 1, 0) <= 0 {
		t.Error("default collation must stay bytewise")
	}
}
