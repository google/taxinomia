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
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
)

// newFilterTestView builds a small table exercising every filter form:
// regions repeat (substring/exact matches), notes are near-unique
// (multi-value matches).
func newFilterTestView(t *testing.T, rows int) (*TableView, []string, []string) {
	t.Helper()
	regions := []string{"North", "south", "east", "west", "northeast"}
	table := NewDataTable()
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	noteCol := columns.NewStringColumn(columns.NewColumnDef("note", "Note", ""))
	regionValues := make([]string, rows)
	noteValues := make([]string, rows)
	for i := 0; i < rows; i++ {
		regionValues[i] = regions[i%len(regions)]
		noteValues[i] = fmt.Sprintf("note-%d", i%7)
		regionCol.Append(regionValues[i])
		noteCol.Append(noteValues[i])
	}
	regionCol.FinalizeColumn()
	noteCol.FinalizeColumn()
	table.AddColumn(regionCol)
	table.AddColumn(noteCol)
	tv := NewTableView(table, "filtertest")
	tv.VisibleColumns = []string{"region", "note"}
	return tv, regionValues, noteValues
}

// TestApplyFiltersSelectionParity pins that the Selection-bitmap filter path
// selects exactly the rows the documented matching rules describe, for every
// filter form.
func TestApplyFiltersSelectionParity(t *testing.T) {
	const rows = 1000

	cases := []struct {
		name    string
		filters map[string]string
		match   func(region, note string) bool
	}{
		{
			name:    "substring case-insensitive",
			filters: map[string]string{"region": "orth"},
			match: func(region, note string) bool {
				return strings.Contains(strings.ToLower(region), "orth")
			},
		},
		{
			name:    "exact case-sensitive",
			filters: map[string]string{"region": `"North"`},
			match:   func(region, note string) bool { return region == "North" },
		},
		{
			name:    "multi-value exact",
			filters: map[string]string{"note": "note-1|note-3"},
			match:   func(region, note string) bool { return note == "note-1" || note == "note-3" },
		},
		{
			name:    "two columns AND",
			filters: map[string]string{"region": "orth", "note": "note-1|note-3"},
			match: func(region, note string) bool {
				return strings.Contains(strings.ToLower(region), "orth") &&
					(note == "note-1" || note == "note-3")
			},
		},
		{
			name:    "missing column selects nothing",
			filters: map[string]string{"nosuch": "x"},
			match:   func(region, note string) bool { return false },
		},
		{
			name:    "no filters select everything",
			filters: map[string]string{},
			match:   func(region, note string) bool { return true },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tv, regionValues, noteValues := newFilterTestView(t, rows)
			tv.ApplyFilters(tc.filters)

			want := []uint32{}
			for i := 0; i < rows; i++ {
				if tc.match(regionValues[i], noteValues[i]) {
					want = append(want, uint32(i))
				}
			}

			got := tv.GetFilteredIndices()
			if len(got) != len(want) {
				t.Fatalf("filtered %d rows, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("index %d: got row %d, want row %d", i, got[i], want[i])
				}
			}
			if count := tv.GetFilteredRowCount(); count != len(want) {
				t.Errorf("GetFilteredRowCount() = %d, want %d", count, len(want))
			}
		})
	}
}

// TestApplyFiltersRetainedMemory pins the phase-2a acceptance criterion: the
// cached selection for an unselective filter on 1M rows is a bitmap (~128 KB),
// not the ~1 MB []bool mask it replaced, and nothing O(4 bytes x rows) is
// retained. (The []uint32 form still exists behind the GetFilteredIndices
// adapter for unmigrated callers; it is transient and removed in phase 2b.)
func TestApplyFiltersRetainedMemory(t *testing.T) {
	if raceEnabled {
		t.Skip("memory measurement is not meaningful under the race detector")
	}
	const rows = 1_000_000

	table := NewDataTable()
	col := columns.NewStringColumn(columns.NewColumnDef("value", "Value", ""))
	for i := 0; i < rows; i++ {
		col.Append(fmt.Sprintf("row-%06d", i))
	}
	col.FinalizeColumn()
	table.AddColumn(col)
	tv := NewTableView(table, "bench")
	tv.VisibleColumns = []string{"value"}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Substring match present in every value: unselective, all rows pass.
	tv.ApplyFilters(map[string]string{"value": "row"})

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(tv)

	if count := tv.GetFilteredRowCount(); count != rows {
		t.Fatalf("unselective filter passed %d rows, want %d", count, rows)
	}

	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("retained by unselective filter on %d rows: %d bytes", rows, retained)
	// The bitmap is rows/8 = 125 KB. 256 KiB leaves headroom for the filter
	// bookkeeping while failing loudly on the old []bool mask (1 MB), let
	// alone a retained []uint32 index list (4 MB).
	if retained > 256<<10 {
		t.Errorf("filter retained %d bytes; want a bitmap selection (< 256 KiB)", retained)
	}
}
