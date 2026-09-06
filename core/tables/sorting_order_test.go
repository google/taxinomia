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
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// keyedTable builds a small table sorted by its key "id" with a
// low-cardinality "grp" column, so ties on grp are the interesting case.
func keyedTable(t *testing.T, n int) *DataTable {
	t.Helper()
	id := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "id", ""))
	grp := columns.NewChunkedStringColumn(columns.NewColumnDef("grp", "grp", ""))
	for i := 0; i < n; i++ {
		id.Append(fmt.Sprintf("k%04d", i))
		grp.Append(fmt.Sprintf("g%d", i%3))
	}
	id.FinalizeColumn()
	grp.FinalizeColumn()
	tbl := NewDataTable()
	tbl.AddColumn(id)
	tbl.AddColumn(grp)
	if err := tbl.SortByKey([]string{"id"}); err != nil {
		t.Fatal(err)
	}
	return tbl
}

func ids(rows []map[string]string) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r["id"]
	}
	return out
}

// TestSortedRowsKeyTieBreak: rows equal on every requested column come
// back in storage-key order, both through the heap (limit) and the full
// sort (no limit), ascending and descending.
func TestSortedRowsKeyTieBreak(t *testing.T) {
	tv := NewTableView(keyedTable(t, 30), "t")
	cols := []string{"id", "grp"}
	byGrp := []queryspec.SortColumn{{Name: "grp"}}

	top := ids(tv.GetFilteredRowsSorted(cols, byGrp, 4))
	if want := []string{"k0000", "k0003", "k0006", "k0009"}; fmt.Sprint(top) != fmt.Sprint(want) {
		t.Errorf("limit 4 by grp: %v, want %v (g0 rows in key order)", top, want)
	}
	all := ids(tv.GetFilteredRowsSorted(cols, byGrp, 0))
	if len(all) != 30 || all[0] != "k0000" || all[9] != "k0027" || all[10] != "k0001" || all[29] != "k0029" {
		t.Errorf("no limit by grp: %v", all)
	}
	desc := ids(tv.GetFilteredRowsSorted(cols, []queryspec.SortColumn{{Name: "grp", Descending: true}}, 3))
	if want := []string{"k0002", "k0005", "k0008"}; fmt.Sprint(desc) != fmt.Sprint(want) {
		t.Errorf("limit 3 by grp desc: %v, want %v (g2 rows, key still ascending)", desc, want)
	}
}

// TestSortedRowsStorageFastPath: an order that starts with the storage key
// ascending is storage order; the result equals the unsorted read and is
// unaffected by later columns.
func TestSortedRowsStorageFastPath(t *testing.T) {
	tv := NewTableView(keyedTable(t, 30), "t")
	cols := []string{"id", "grp"}
	plain := ids(tv.GetFilteredRows(cols, 7))
	sorted := ids(tv.GetFilteredRowsSorted(cols, []queryspec.SortColumn{{Name: "id"}, {Name: "grp", Descending: true}}, 7))
	if fmt.Sprint(plain) != fmt.Sprint(sorted) {
		t.Errorf("key-first order %v != storage order %v", sorted, plain)
	}
	if !tv.sortedByStorage([]sortableColumn{{name: "id"}, {name: "grp", descending: true}}, []string{"id"}) {
		t.Error("key-first ascending must be recognised as storage order")
	}
	if tv.sortedByStorage([]sortableColumn{{name: "id", descending: true}}, []string{"id"}) || tv.sortedByStorage([]sortableColumn{{name: "grp"}, {name: "id"}}, []string{"id"}) {
		t.Error("key descending or key not first is not storage order")
	}
	// With a filter, storage order is the selection's order.
	tv.ApplyFilters(map[string]string{"grp": "g1"})
	got := ids(tv.GetFilteredRowsSorted(cols, []queryspec.SortColumn{{Name: "id"}}, 3))
	if want := []string{"k0001", "k0004", "k0007"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("filtered key-first: %v, want %v", got, want)
	}
}
