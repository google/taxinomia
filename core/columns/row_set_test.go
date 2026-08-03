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
	"reflect"
	"testing"
)

func collectRowSet(sel RowSet) []uint32 {
	var out []uint32
	sel.ForEachRow(func(i uint32) bool {
		out = append(out, i)
		return true
	})
	return out
}

func TestAllRows(t *testing.T) {
	if got := AllRows(0).NumRows(); got != 0 {
		t.Errorf("AllRows(0).NumRows() = %d, want 0", got)
	}
	if got := collectRowSet(AllRows(0)); len(got) != 0 {
		t.Errorf("AllRows(0) visited %v, want nothing", got)
	}

	want := []uint32{0, 1, 2, 3, 4}
	if got := collectRowSet(AllRows(5)); !reflect.DeepEqual(got, want) {
		t.Errorf("AllRows(5) visited %v, want %v", got, want)
	}
	if got := AllRows(5).NumRows(); got != 5 {
		t.Errorf("AllRows(5).NumRows() = %d, want 5", got)
	}

	// Early stop after the second row.
	var visited []uint32
	AllRows(5).ForEachRow(func(i uint32) bool {
		visited = append(visited, i)
		return len(visited) < 2
	})
	if !reflect.DeepEqual(visited, []uint32{0, 1}) {
		t.Errorf("early-stopped AllRows visited %v, want [0 1]", visited)
	}
}

func TestRowIndices(t *testing.T) {
	// Order is preserved verbatim, including non-ascending lists.
	sel := RowIndices{7, 3, 3, 100}
	if got := sel.NumRows(); got != 4 {
		t.Errorf("NumRows() = %d, want 4", got)
	}
	if got := collectRowSet(sel); !reflect.DeepEqual(got, []uint32{7, 3, 3, 100}) {
		t.Errorf("visited %v, want [7 3 3 100]", got)
	}

	var visited []uint32
	sel.ForEachRow(func(i uint32) bool {
		visited = append(visited, i)
		return false
	})
	if !reflect.DeepEqual(visited, []uint32{7}) {
		t.Errorf("early-stopped RowIndices visited %v, want [7]", visited)
	}
}

func TestSelectionForEachRow(t *testing.T) {
	// Rows straddling word boundaries, matching the Selection unit tests.
	rows := []uint32{0, 5, 63, 64, 65, 127, 128, 199}
	sel := SelectionFromIndices(200, rows)
	if got := collectRowSet(sel); !reflect.DeepEqual(got, rows) {
		t.Errorf("ForEachRow visited %v, want %v", got, rows)
	}
	if got := sel.NumRows(); got != len(rows) {
		t.Errorf("NumRows() = %d, want %d", got, len(rows))
	}

	// Early stop mid-word and across words.
	for _, stopAfter := range []int{1, 3, 5} {
		var visited []uint32
		sel.ForEachRow(func(i uint32) bool {
			visited = append(visited, i)
			return len(visited) < stopAfter
		})
		if !reflect.DeepEqual(visited, rows[:stopAfter]) {
			t.Errorf("stop after %d: visited %v, want %v", stopAfter, visited, rows[:stopAfter])
		}
	}
}

// TestGroupOpsSelectionMatchesIndices pins that a Selection bitmap drives the
// grouping operations identically to the equivalent ascending index list —
// the property phase 2b relies on when it feeds filter bitmaps straight into
// grouping.
func TestGroupOpsSelectionMatchesIndices(t *testing.T) {
	col := NewStringColumn(NewColumnDef("s", "S", ""))
	values := []string{"b", "a", "b", "c", "a", "a", "d", "b", "c", "a"}
	for _, v := range values {
		col.Append(v)
	}
	col.FinalizeColumn()

	indices := []uint32{0, 2, 3, 5, 6, 8, 9}
	fromList := RowIndices(indices)
	fromBitmap := SelectionFromIndices(len(values), indices)

	ops := GroupOpsFor(col, nil)

	listCounts, listFirsts := ops.GroupCounts(fromList)
	bmpCounts, bmpFirsts := ops.GroupCounts(fromBitmap)
	if !reflect.DeepEqual(listCounts, bmpCounts) || !reflect.DeepEqual(listFirsts, bmpFirsts) {
		t.Errorf("GroupCounts differ: list (%v, %v) vs bitmap (%v, %v)",
			listCounts, listFirsts, bmpCounts, bmpFirsts)
	}

	listAcc, bmpAcc := newCollectAcc(), newCollectAcc()
	ops.GroupAggregates(fromList, listAcc)
	ops.GroupAggregates(fromBitmap, bmpAcc)
	if !reflect.DeepEqual(listAcc.byCode, bmpAcc.byCode) {
		t.Errorf("GroupAggregates differ: list %v vs bitmap %v", listAcc.byCode, bmpAcc.byCode)
	}

	for code := range listCounts {
		listMembers := ops.GroupMembers(fromList, uint32(code), 0, -1)
		bmpMembers := ops.GroupMembers(fromBitmap, uint32(code), 0, -1)
		if !reflect.DeepEqual(listMembers, bmpMembers) {
			t.Errorf("GroupMembers(code=%d) differ: list %v vs bitmap %v", code, listMembers, bmpMembers)
		}
	}
}
