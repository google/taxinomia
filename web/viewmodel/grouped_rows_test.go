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

package viewmodel

import (
	"net/url"
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/web/urlquery"
)

// demoTable is the tables package's demo fixture: 3 statuses x 3 regions x
// 3 rows, amount = 7 + 3*row.
func demoTable() *tables.DataTable {
	table := tables.NewDataTable()
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", ""))
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	categories := []string{"A", "A", "B", "A", "B", "B", "Z", "C", "C"}
	row := 0
	for _, status := range []string{"Active", "Inactive", "Pending"} {
		for _, region := range []string{"North", "South", "East"} {
			for k := 0; k < 3; k++ {
				statusCol.Append(status)
				regionCol.Append(region)
				categoryCol.Append(categories[row%len(categories)])
				amountCol.Append(uint32(7 + row*3))
				row++
			}
		}
	}
	statusCol.FinalizeColumn()
	regionCol.FinalizeColumn()
	categoryCol.FinalizeColumn()
	amountCol.FinalizeColumn()
	table.AddColumn(statusCol)
	table.AddColumn(regionCol)
	table.AddColumn(categoryCol)
	table.AddColumn(amountCol)
	return table
}

// groupedRowsFor groups the demo table as the URL asks (the handler's
// grouping call) and builds the grouped rows for it.
func groupedRowsFor(t *testing.T, rawURL string) (GroupBuildResult, *urlquery.Query) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	q := urlquery.NewQuery(u)
	tv := tables.NewTableView(demoTable(), "demo")
	tv.VisibleColumns = []string{"status", "region", "category", "amount"}
	asc := make(map[string]bool)
	for _, sc := range q.EffectiveSortOrder() {
		asc[sc.Name] = !sc.Descending
	}
	expansion := tables.GroupExpansion{ExpandAll: !q.HasExpandedGroups, Paths: q.ExpandedGroups}
	tv.GroupTableWindowed(q.GroupedColumns, nil, make(map[string]tables.Compare), asc, q.Limit, expansion)
	return buildGroupedRows(tv, tv.VisibleColumns, q, q.Limit, map[string]string{"category": "demo.category"}, func(entityType, value string) string {
		return "/entity/" + entityType + "/" + value
	}), q
}

func cellNames(row GroupedRow) string {
	parts := make([]string, len(row.Cells))
	for i, c := range row.Cells {
		switch {
		case c.IsGroupedColumn:
			parts[i] = c.ColumnName + "=" + c.Value
		case c.IsRowValue:
			parts[i] = c.ColumnName + ":" + c.Value
		default:
			parts[i] = c.ColumnName + "(agg)"
		}
	}
	return strings.Join(parts, " | ")
}

func toggleQuery(t *testing.T, c GroupedCell) *urlquery.Query {
	t.Helper()
	u, err := url.Parse(c.ToggleURL.String())
	if err != nil {
		t.Fatal(err)
	}
	return urlquery.NewQuery(u)
}

// TestGroupedRowsListOpenedInnermostGroup renders an explicit expansion:
// Active is open, Active/North is an opened innermost group whose rows are
// listed beneath its cell, sorted by the visible columns (amount descending
// here); every other group is collapsed to one row of aggregates.
func TestGroupedRowsListOpenedInnermostGroup(t *testing.T) {
	res, _ := groupedRowsFor(t, "/table?table=demo&columns=status,region,amount,category&sort=-amount&grouped=status,region&gexp=Active,Active%2FNorth&limit=25")

	// Level 1 sorts by value (East, North, South); leaf columns keep the
	// visible order (category, amount); listed rows sort by amount desc.
	want := []string{
		"status=Active | region=East | category(agg) | amount(agg)",
		"region=North | category:B | amount:13",
		"category:A | amount:10",
		"category:A | amount:7",
		"region=South | category(agg) | amount(agg)",
		"status=Inactive | category(agg) | amount(agg)",
		"status=Pending | category(agg) | amount(agg)",
	}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d:\n%s", len(res.Rows), len(want), dumpRows(res.Rows))
	}
	for i, w := range want {
		if got := cellNames(res.Rows[i]); got != w {
			t.Errorf("row %d = %q, want %q", i, got, w)
		}
	}
	if res.Truncated || res.TotalRows != 7 || res.ShownRows != 7 {
		t.Errorf("truncated=%v total=%d shown=%d, want false/7/7", res.Truncated, res.TotalRows, res.ShownRows)
	}

	active, east, north, south := res.Rows[0].Cells[0], res.Rows[0].Cells[1], res.Rows[1].Cells[0], res.Rows[4].Cells[0]
	if active.Rowspan != 5 || !active.IsExpanded {
		t.Errorf("Active: rowspan %d expanded %v, want 5/true", active.Rowspan, active.IsExpanded)
	}
	if north.Rowspan != 3 || !north.IsExpanded || north.NumRows != 3 {
		t.Errorf("North: rowspan %d expanded %v rows %d, want 3/true/3", north.Rowspan, north.IsExpanded, north.NumRows)
	}
	if cat := res.Rows[1].Cells[1]; cat.ValueURL != "/entity/demo.category/B" {
		t.Errorf("listed row value URL = %q, want the resolver's", cat.ValueURL)
	}
	if east.IsExpanded || east.Rowspan != 1 || south.IsExpanded || south.Rowspan != 1 {
		t.Errorf("East and South must be collapsed to one row: %v/%d %v/%d", east.IsExpanded, east.Rowspan, south.IsExpanded, south.Rowspan)
	}

	// Toggles: closing North keeps Active open; closing Active closes both;
	// opening South adds its path to the list.
	if q := toggleQuery(t, north); q.IsGroupExpanded([]string{"Active", "North"}) || !q.IsGroupExpanded([]string{"Active"}) {
		t.Errorf("North's toggle must close North and keep Active: %v", q.ExpandedGroups)
	}
	if q := toggleQuery(t, active); q.IsGroupExpanded([]string{"Active"}) || !q.HasExpandedGroups {
		t.Errorf("Active's toggle must close Active: %v", q.ExpandedGroups)
	}
	if q := toggleQuery(t, south); !q.IsGroupExpanded([]string{"Active", "South"}) || !q.IsGroupExpanded([]string{"Active", "North"}) {
		t.Errorf("South's toggle must open South and keep North: %v", q.ExpandedGroups)
	}
}

// TestGroupedRowsExpandAllTogglesMaterialize renders the historical
// expand-all page: no rows are listed, and a toggle URL carries the page's
// other open groups explicitly, so that closing one keeps the rest.
func TestGroupedRowsExpandAllTogglesMaterialize(t *testing.T) {
	res, _ := groupedRowsFor(t, "/table?table=demo&grouped=status,region&limit=25")
	if len(res.Rows) != 9 {
		t.Fatalf("got %d rows, want 9 (3 x 3 innermost groups):\n%s", len(res.Rows), dumpRows(res.Rows))
	}
	for _, row := range res.Rows {
		for _, c := range row.Cells {
			if c.IsRowValue {
				t.Fatalf("expand-all must not list rows: %s", cellNames(row))
			}
		}
	}
	active, east := res.Rows[0].Cells[0], res.Rows[0].Cells[1]
	if !active.IsExpanded || east.IsExpanded {
		t.Errorf("level 0 open (%v), innermost closed (%v)", active.IsExpanded, east.IsExpanded)
	}
	q := toggleQuery(t, active)
	if !q.HasExpandedGroups || q.IsGroupExpanded([]string{"Active"}) {
		t.Errorf("Active's toggle must close Active explicitly: %v", q.ExpandedGroups)
	}
	for _, other := range []string{"Inactive", "Pending"} {
		if !q.IsGroupExpanded([]string{other}) {
			t.Errorf("%s must stay open after closing Active: %v", other, q.ExpandedGroups)
		}
	}
	q = toggleQuery(t, east)
	if !q.IsGroupExpanded([]string{"Active", "East"}) || !q.IsGroupExpanded([]string{"Pending"}) {
		t.Errorf("East's toggle must list its rows and keep the other groups open: %v", q.ExpandedGroups)
	}
}

// TestGroupedRowsListedRowsRespectTheLimit cuts an opened group's listing
// at the display limit: the group cells span what was shown and are marked
// incomplete, and the totals report the full height.
func TestGroupedRowsListedRowsRespectTheLimit(t *testing.T) {
	res, _ := groupedRowsFor(t, "/table?table=demo&grouped=status,region&gexp=Active,Active%2FNorth&limit=2")
	if len(res.Rows) != 2 || !res.Truncated {
		t.Fatalf("got %d rows truncated=%v, want 2/true:\n%s", len(res.Rows), res.Truncated, dumpRows(res.Rows))
	}
	// TotalRows counts every level-0 group the build kept (the level-0
	// top-K trim keeps limit groups): Active's 5 display rows plus one
	// collapsed group.
	if res.TotalRows != 6 {
		t.Errorf("TotalRows = %d, want 6", res.TotalRows)
	}
	// Row 0 is Active/East (collapsed); row 1 starts North's listing, cut
	// after its first row: both spanning cells are trimmed and marked.
	if active := res.Rows[0].Cells[0]; active.Rowspan != 2 || !active.IsIncomplete {
		t.Errorf("Active: rowspan %d incomplete %v, want 2/true", active.Rowspan, active.IsIncomplete)
	}
	if north := res.Rows[1].Cells[0]; north.Rowspan != 1 || !north.IsIncomplete || north.Value != "North" {
		t.Errorf("North: rowspan %d incomplete %v value %q, want 1/true/North", north.Rowspan, north.IsIncomplete, north.Value)
	}
	if got := cellNames(res.Rows[1]); got != "region=North | category:A | amount:7" {
		t.Errorf("first listed row = %q", got)
	}
}

func dumpRows(rows []GroupedRow) string {
	lines := make([]string, len(rows))
	for i, r := range rows {
		lines[i] = cellNames(r)
	}
	return strings.Join(lines, "\n")
}
