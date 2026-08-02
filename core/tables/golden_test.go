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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
	"github.com/google/taxinomia/core/query"
)

// The golden tests pin the rendered output of grouping: group order, values,
// row counts, subgroup counts, aggregates and the ASCII rendering. They were
// captured before the grouping interface migration (phase 1b) and must stay
// byte-identical across it. Regenerate only with a deliberate
//
//	go test ./core/tables/ -run TestGolden -update
//
// and only when a rendering change is intended.
var updateGolden = flag.Bool("update", false, "rewrite golden files with current output")

// aggTypesFor returns a fixed, deterministic list of aggregate types to render
// per column type.
func aggTypesFor(colType query.ColumnType) []query.AggregateType {
	switch colType {
	case query.ColumnTypeNumeric:
		return []query.AggregateType{query.AggCount, query.AggSum, query.AggAvg, query.AggMin, query.AggMax, query.AggStdDev}
	case query.ColumnTypeBool:
		return []query.AggregateType{query.AggCount, query.AggTrue, query.AggFalse, query.AggRatio}
	case query.ColumnTypeDatetime:
		return []query.AggregateType{query.AggCount, query.AggMin, query.AggMax, query.AggSpan}
	default:
		return []query.AggregateType{query.AggCount, query.AggUnique}
	}
}

// dumpGroupTree renders the full grouping hierarchy through the public group
// accessors only (no Group.Indices), so it works before and after the
// interface migration.
func dumpGroupTree(tv *TableView, leafCols []string) string {
	var sb strings.Builder
	first := tv.GetFirstBlock()
	if first == nil {
		return "<no grouping>\n"
	}
	var walk func(block *grouping.Block, depth int)
	walk = func(block *grouping.Block, depth int) {
		for _, g := range block.Groups {
			indent := strings.Repeat("  ", depth)
			fmt.Fprintf(&sb, "%s%q rows=%d subgroups=%d height=%d complete=%v\n",
				indent, g.GetValue(), g.Length(), g.NumSubgroups(), g.Height(), g.IsComplete)
			for _, colName := range leafCols {
				state, ok := g.Aggregates[colName]
				if !ok || state == nil {
					continue
				}
				colType := tv.GetColumnType(colName)
				var parts []string
				for _, aggType := range aggTypesFor(colType) {
					parts = append(parts, fmt.Sprintf("%s=%s", aggType, state.Format(aggType)))
				}
				fmt.Fprintf(&sb, "%s  agg[%s]: %s\n", indent, colName, strings.Join(parts, " "))
			}
			if g.ChildBlock != nil {
				walk(g.ChildBlock, depth+1)
			}
		}
	}
	walk(first, 0)
	return sb.String()
}

func checkGolden(t *testing.T, name string, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden file %s: %v (run with -update to create)", path, err)
	}
	if string(want) != got {
		t.Errorf("output differs from golden file %s.\n--- got ---\n%s\n--- want ---\n%s", path, got, string(want))
	}
}

// demoTable builds a small, fully deterministic demo table:
// 27 rows of status/region/category plus numeric, float, bool and string leaves.
func demoTable() (*DataTable, []string) {
	table := NewDataTable()

	statuses := []string{"Active", "Inactive", "Pending"}
	regions := []string{"North", "South", "East"}
	categories := []string{"A", "A", "B", "A", "B", "B", "Z", "C", "C"}

	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", ""))
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	scoreCol := columns.NewFloat64Column(columns.NewColumnDef("score", "Score", ""))
	flagCol := columns.NewBoolColumn(columns.NewColumnDef("flag", "Flag", ""))
	noteCol := columns.NewStringColumn(columns.NewColumnDef("note", "Note", ""))

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

func TestGoldenMultiLevelStrings(t *testing.T) {
	table, visible := demoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	tv.GroupTable([]string{"status", "region", "category"}, nil, make(map[string]Compare), map[string]bool{"status": true})

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "multilevel_strings", out)
}

func TestGoldenFilteredDescLimit(t *testing.T) {
	table, visible := demoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	tv.ApplyFilters(map[string]string{"region": "orth"})
	tv.GroupTableWithLimit([]string{"status", "region"}, nil, make(map[string]Compare), map[string]bool{"status": false}, 2)

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "filtered_desc_limit", out)
}

func TestGoldenAggregateSort(t *testing.T) {
	table, visible := demoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	// Filter to distinct per-group row counts so the aggregate sort has no ties
	// (sort.Slice is not stable; ties would be nondeterministic before and
	// after the migration alike).
	tv.ApplyFilters(map[string]string{"note": "note-1|note-2|note-0"})
	tv.GroupTable([]string{"status"}, nil, make(map[string]Compare), make(map[string]bool))
	tv.SortGroupsByAggregate(map[string]*query.GroupAggSort{
		"status": {GroupedColumn: "status", LeafColumn: "amount", AggType: query.AggSum, Descending: true},
	})

	out := dumpGroupTree(tv, tv.GetLeafColumns())
	checkGolden(t, "aggregate_sort", out)
}

func TestGoldenDictColumn(t *testing.T) {
	table := NewDataTable()

	statuses := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"}
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	for i := 0; i < 5000; i++ {
		statusCol.Append(statuses[i%len(statuses)])
		amountCol.Append(uint32(i % 13))
	}
	statusCol.FinalizeColumn()
	amountCol.FinalizeColumn()

	compacted, ok := columns.CompactStringColumn(statusCol)
	if !ok {
		t.Fatal("expected status column to be dictionary-compacted")
	}
	table.AddColumn(compacted)
	table.AddColumn(amountCol)

	tv := NewTableView(table, "dictdemo")
	tv.VisibleColumns = []string{"status", "amount"}
	tv.GroupTable([]string{"status"}, nil, make(map[string]Compare), map[string]bool{"status": true})

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "dict_column", out)
}

func TestGoldenJoinedColumn(t *testing.T) {
	// Customers table: key column with entity type (so the value index is
	// kept) plus a name column reachable through the join.
	custID := columns.NewStringColumn(columns.NewColumnDef("id", "ID", "customer"))
	custName := columns.NewStringColumn(columns.NewColumnDef("name", "Name", ""))
	for _, c := range [][2]string{{"C1", "Ada"}, {"C2", "Grace"}, {"C3", "Edsger"}} {
		custID.Append(c[0])
		custName.Append(c[1])
	}
	custID.FinalizeColumn()
	custName.FinalizeColumn()

	// Orders table: fk column with one dangling reference (C9 -> unmatched).
	table := NewDataTable()
	fkCol := columns.NewStringColumn(columns.NewColumnDef("customer", "Customer", "customer"))
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	fks := []string{"C1", "C2", "C1", "C9", "C3", "C2", "C1", "C9"}
	for i, fk := range fks {
		fkCol.Append(fk)
		amountCol.Append(uint32(10 + i))
	}
	fkCol.FinalizeColumn()
	amountCol.FinalizeColumn()
	table.AddColumn(fkCol)
	table.AddColumn(amountCol)

	joiner := &columns.JoinerString{Joiner: columns.Joiner[string]{FromColumn: fkCol, ToColumn: custID}}
	joined := custName.CreateJoinedColumn(columns.NewColumnDef("customer.customers.id.name", "Customer Name", ""), joiner)

	tv := NewTableView(table, "orders")
	tv.AddJoinedColumn(joined)
	tv.VisibleColumns = []string{"customer.customers.id.name", "amount"}
	tv.GroupTable([]string{"customer.customers.id.name"}, nil, make(map[string]Compare), map[string]bool{"customer.customers.id.name": true})

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "joined_column", out)
}

func TestGoldenComputedColumn(t *testing.T) {
	table, visible := demoTable()

	// Computed column that errors for every 7th row; those rows are dropped
	// from the grouping (unmapped), which the golden output pins.
	amount := table.GetColumn("amount").(*columns.Uint32Column)
	computed := columns.NewComputedStringColumn(
		columns.NewColumnDef("bucket", "Bucket", ""),
		table.Length(),
		func(i uint32) (string, error) {
			if i%7 == 3 {
				return "", fmt.Errorf("no bucket for row %d", i)
			}
			v, err := amount.GetValue(i)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("b%d", v%3), nil
		})

	tv := NewTableView(table, "demo")
	tv.AddComputedColumn("bucket", computed)
	tv.VisibleColumns = append([]string{"bucket"}, visible...)
	tv.GroupTable([]string{"bucket", "status"}, nil, make(map[string]Compare), map[string]bool{"bucket": true})

	out := dumpGroupTree(tv, tv.GetLeafColumns()) + "\n=== ascii ===\n" + tv.ToAscii()
	checkGolden(t, "computed_column", out)
}
