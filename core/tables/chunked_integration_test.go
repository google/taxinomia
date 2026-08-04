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
	"github.com/google/taxinomia/core/query"
)

// chunkedDemoTable builds the exact demoTable dataset with chunked columns.
// Phase 3c: loaders build chunked tables, so the whole TableView pipeline must
// produce byte-identical output on them.
func chunkedDemoTable() (*DataTable, []string) {
	table := NewDataTable()

	statuses := []string{"Active", "Inactive", "Pending"}
	regions := []string{"North", "South", "East"}
	categories := []string{"A", "A", "B", "A", "B", "B", "Z", "C", "C"}

	statusCol := columns.NewChunkedStringColumn(columns.NewColumnDef("status", "Status", ""))
	regionCol := columns.NewChunkedStringColumn(columns.NewColumnDef("region", "Region", ""))
	categoryCol := columns.NewChunkedStringColumn(columns.NewColumnDef("category", "Category", ""))
	amountCol := columns.NewChunkedUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	scoreCol := columns.NewChunkedFloat64Column(columns.NewColumnDef("score", "Score", ""))
	flagCol := columns.NewChunkedBoolColumn(columns.NewColumnDef("flag", "Flag", ""))
	noteCol := columns.NewChunkedStringColumn(columns.NewColumnDef("note", "Note", ""))

	row := 0
	for _, status := range statuses {
		for _, region := range regions {
			for k := 0; k < 3; k++ {
				statusCol.Append(status)
				regionCol.Append(region)
				categoryCol.Append(categories[(row)%len(categories)])
				amountCol.Append(uint32(7 + row*3))
				scoreCol.Append(float64(row%5) + 0.25)
				flagCol.Append(row%3 == 0)
				noteCol.Append(fmt.Sprintf("note-%d", row%4))
				row++
			}
		}
	}
	statusCol.FinalizeColumn()
	regionCol.FinalizeColumn()
	categoryCol.FinalizeColumn()
	amountCol.FinalizeColumn()
	scoreCol.FinalizeColumn()
	flagCol.FinalizeColumn()
	noteCol.FinalizeColumn()

	table.AddColumn(statusCol)
	table.AddColumn(regionCol)
	table.AddColumn(categoryCol)
	table.AddColumn(amountCol)
	table.AddColumn(scoreCol)
	table.AddColumn(flagCol)
	table.AddColumn(noteCol)

	visible := []string{"status", "region", "category", "amount", "score", "flag", "note"}
	return table, visible
}

// TestGoldenChunkedMultiLevelStrings pins that a chunked-column table renders
// byte-identically to the plain-column table: same golden file as
// TestGoldenMultiLevelStrings.
func TestGoldenChunkedMultiLevelStrings(t *testing.T) {
	table, visible := chunkedDemoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	tv.GroupTable([]string{"status", "region", "category"}, nil, make(map[string]Compare), map[string]bool{"status": true})

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "multilevel_strings", out)
}

// TestGoldenChunkedFilteredDescLimit exercises filtering (the substring path)
// plus descending sort and limit on chunked columns against the plain golden.
func TestGoldenChunkedFilteredDescLimit(t *testing.T) {
	table, visible := chunkedDemoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	tv.ApplyFilters(map[string]string{"region": "orth"})
	tv.GroupTableWithLimit([]string{"status", "region"}, nil, make(map[string]Compare), map[string]bool{"status": false}, 2)

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "filtered_desc_limit", out)
}

// TestChunkedFilterParity pins that ApplyFilters produces identical selections
// on plain and chunked tables for every filter form. Exact and multi-value
// filters on chunked columns take the structured chunk-pruned path; substring
// stays on the generic scan.
func TestChunkedFilterParity(t *testing.T) {
	plainTable, _ := demoTable()
	chunkedTable, _ := chunkedDemoTable()

	cases := []struct {
		name    string
		filters map[string]string
	}{
		{"substring", map[string]string{"region": "orth"}},
		{"exact", map[string]string{"status": `"Active"`}},
		{"exact miss", map[string]string{"status": `"active"`}},
		{"multi-value", map[string]string{"note": "note-1|note-2|note-0"}},
		{"multi-value with miss", map[string]string{"note": "note-1|absent"}},
		{"and across columns", map[string]string{"status": `"Pending"`, "note": "note-0|note-3", "region": "S"}},
		{"missing column", map[string]string{"nope": "x"}},
		{"empty exact", map[string]string{"status": `""`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ptv := NewTableView(plainTable, "plain")
			ctv := NewTableView(chunkedTable, "chunked")
			ptv.ApplyFilters(tc.filters)
			ctv.ApplyFilters(tc.filters)
			if p, c := ptv.GetFilteredRowCount(), ctv.GetFilteredRowCount(); p != c {
				t.Fatalf("filtered row count: plain %d, chunked %d", p, c)
			}
			pRows := ptv.GetFilteredRows([]string{"status", "region", "note", "amount"}, -1)
			cRows := ctv.GetFilteredRows([]string{"status", "region", "note", "amount"}, -1)
			if len(pRows) != len(cRows) {
				t.Fatalf("row count: plain %d, chunked %d", len(pRows), len(cRows))
			}
			for i := range pRows {
				for _, col := range []string{"status", "region", "note", "amount"} {
					if pRows[i][col] != cRows[i][col] {
						t.Fatalf("row %d col %s: plain %q, chunked %q", i, col, pRows[i][col], cRows[i][col])
					}
				}
			}
		})
	}
}

// TestChunkedDictFilterParity runs the filter forms against a chunked
// dictionary column, including a value absent from the dictionary (which the
// structured path answers without touching a chunk).
func TestChunkedDictFilterParity(t *testing.T) {
	build := func(dict bool) *DataTable {
		table := NewDataTable()
		values := []string{"north", "south", "east", "west"}
		var col columns.IDataColumn
		if dict {
			c := columns.NewChunkedDictStringColumn[uint16](columns.NewColumnDef("region", "Region", ""))
			for i := 0; i < 40; i++ {
				c.Append(values[i%4])
			}
			c.FinalizeColumn()
			col = c
		} else {
			c := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
			for i := 0; i < 40; i++ {
				c.Append(values[i%4])
			}
			c.FinalizeColumn()
			col = c
		}
		table.AddColumn(col)
		amount := columns.NewChunkedInt64Column(columns.NewColumnDef("amount", "Amount", ""))
		for i := 0; i < 40; i++ {
			amount.Append(int64(i))
		}
		amount.FinalizeColumn()
		table.AddColumn(amount)
		return table
	}

	for _, tc := range []struct {
		name    string
		filters map[string]string
	}{
		{"exact hit", map[string]string{"region": `"south"`}},
		{"exact absent from dict", map[string]string{"region": `"atlantis"`}},
		{"multi-value", map[string]string{"region": "north|west|atlantis"}},
		{"substring", map[string]string{"region": "st"}}, // east + west
	} {
		t.Run(tc.name, func(t *testing.T) {
			ptv := NewTableView(build(false), "plain")
			ctv := NewTableView(build(true), "dict")
			ptv.ApplyFilters(tc.filters)
			ctv.ApplyFilters(tc.filters)
			if p, c := ptv.GetFilteredRowCount(), ctv.GetFilteredRowCount(); p != c {
				t.Fatalf("filtered row count: plain %d, dict-chunked %d", p, c)
			}
			pRows := ptv.GetFilteredRows([]string{"region", "amount"}, -1)
			cRows := ctv.GetFilteredRows([]string{"region", "amount"}, -1)
			for i := range pRows {
				if pRows[i]["region"] != cRows[i]["region"] || pRows[i]["amount"] != cRows[i]["amount"] {
					t.Fatalf("row %d: plain %v, dict-chunked %v", i, pRows[i], cRows[i])
				}
			}
		})
	}
}

// TestChunkedSortParity pins that sorting a chunked table orders rows by
// value: the same top-K as the plain table, in particular for numeric columns
// where the string fallback would order "10" before "9".
func TestChunkedSortParity(t *testing.T) {
	plainTable, _ := demoTable()
	chunkedTable, _ := chunkedDemoTable()
	ptv := NewTableView(plainTable, "plain")
	ctv := NewTableView(chunkedTable, "chunked")

	for _, tc := range []struct {
		name string
		sort []query.SortColumn
	}{
		{"amount desc", []query.SortColumn{{Name: "amount", Descending: true}}},
		{"score asc amount desc", []query.SortColumn{{Name: "score"}, {Name: "amount", Descending: true}}},
		{"status asc", []query.SortColumn{{Name: "status"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols := []string{"status", "region", "amount", "score"}
			pRows := ptv.GetFilteredRowsSorted(cols, tc.sort, 10)
			cRows := ctv.GetFilteredRowsSorted(cols, tc.sort, 10)
			if len(pRows) != len(cRows) {
				t.Fatalf("row count: plain %d, chunked %d", len(pRows), len(cRows))
			}
			for i := range pRows {
				for _, col := range cols {
					if pRows[i][col] != cRows[i][col] {
						t.Fatalf("row %d col %s: plain %q, chunked %q", i, col, pRows[i][col], cRows[i][col])
					}
				}
			}
		})
	}
}
