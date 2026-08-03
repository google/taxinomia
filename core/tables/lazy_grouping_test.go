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
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// buildTwoLevelTable builds a table whose "a" column has nGroups distinct
// values and whose "b" column has nSub distinct values within every a-group.
func buildTwoLevelTable(rows, nGroups, nSub int) *DataTable {
	table := NewDataTable()
	aCol := columns.NewUint32Column(columns.NewColumnDef("a", "A", ""))
	bCol := columns.NewUint32Column(columns.NewColumnDef("b", "B", ""))
	perGroup := rows / nGroups
	for i := 0; i < rows; i++ {
		aCol.Append(uint32(i / perGroup))
		bCol.Append(uint32(i % nSub))
	}
	aCol.FinalizeColumn()
	bCol.FinalizeColumn()
	table.AddColumn(aCol)
	table.AddColumn(bCol)
	return table
}

// TestLazyGroupingComputesOnlyVisibleGroups is the phase-1c acceptance test:
// grouping a 1M-row table with 1,000 level-0 groups and 1,000 level-1 groups
// each, with nothing expanded, computes 1,000 groups — not 10^6. Expanding one
// group then computes exactly one additional level for that group, and
// collapsing computes nothing. Asserted with the groupsBuilt counter.
func TestLazyGroupingComputesOnlyVisibleGroups(t *testing.T) {
	const rows, nGroups, nSub = 1_000_000, 1000, 1000
	table := buildTwoLevelTable(rows, nGroups, nSub)
	tv := NewTableView(table, "big")
	tv.VisibleColumns = []string{"a", "b"}

	grouped := []string{"a", "b"}
	noSort := make(map[string]bool)

	// Nothing expanded: only level 0 is built.
	before := groupsBuilt.Load()
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 0, GroupExpansion{})
	if delta := groupsBuilt.Load() - before; delta != nGroups {
		t.Fatalf("initial lazy grouping built %d groups, want exactly %d (level 0 only)", delta, nGroups)
	}
	first := tv.GetFirstBlock()
	if first == nil || len(first.Groups) != nGroups {
		t.Fatalf("level 0 has %d groups, want %d", len(first.Groups), nGroups)
	}
	for _, g := range first.Groups {
		if g.ChildBlock != nil {
			t.Fatalf("group %q has a child block; nothing was expanded", g.GetValue())
		}
		if g.Indices != nil {
			t.Fatalf("group %q retains membership after the build", g.GetValue())
		}
	}

	// Expanding one group builds exactly its child level and nothing else.
	target := first.Groups[3]
	before = groupsBuilt.Load()
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 0,
		GroupExpansion{Paths: [][]string{{target.GetValue()}}})
	if delta := groupsBuilt.Load() - before; delta != nSub {
		t.Fatalf("expanding one group built %d groups, want exactly %d (one child level)", delta, nSub)
	}
	if target.ChildBlock == nil || len(target.ChildBlock.Groups) != nSub {
		t.Fatalf("expanded group has %d children, want %d", target.NumSubgroups(), nSub)
	}
	for i, g := range first.Groups {
		if i != 3 && g.ChildBlock != nil {
			t.Fatalf("group %q was expanded but should not have been", g.GetValue())
		}
	}
	rowsInChildren := 0
	for _, g := range target.ChildBlock.Groups {
		if g.Indices != nil {
			t.Fatalf("child group %q retains membership after the build", g.GetValue())
		}
		rowsInChildren += g.Length()
	}
	if rowsInChildren != target.Length() {
		t.Fatalf("children cover %d rows, parent has %d", rowsInChildren, target.Length())
	}

	// Collapsing computes nothing new.
	before = groupsBuilt.Load()
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 0, GroupExpansion{})
	if delta := groupsBuilt.Load() - before; delta != 0 {
		t.Fatalf("collapsing built %d groups, want 0", delta)
	}
	if target.ChildBlock != nil {
		t.Fatal("child block not dropped on collapse")
	}

	// Repeating the identical request computes nothing.
	before = groupsBuilt.Load()
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 0, GroupExpansion{})
	if delta := groupsBuilt.Load() - before; delta != 0 {
		t.Fatalf("identical request built %d groups, want 0", delta)
	}
}

// collectExpandablePaths returns the path of every group that has a child
// block in an eagerly built tree.
func collectExpandablePaths(block *grouping.Block, prefix []string) [][]string {
	var paths [][]string
	if block == nil {
		return nil
	}
	for _, g := range block.Groups {
		path := append(append([]string{}, prefix...), g.GetValue())
		if g.ChildBlock != nil {
			paths = append(paths, path)
			paths = append(paths, collectExpandablePaths(g.ChildBlock, path)...)
		}
	}
	return paths
}

// TestLazyGroupingMatchesEagerWhenFullyExpanded pins the lazy path to the
// eager one: with every group expanded, the tree dump (order, values, counts,
// aggregates) must be identical.
func TestLazyGroupingMatchesEagerWhenFullyExpanded(t *testing.T) {
	grouped := []string{"status", "region", "category"}
	asc := map[string]bool{"status": true}

	tableE, visible := demoTable()
	tvE := NewTableView(tableE, "demo")
	tvE.VisibleColumns = visible
	tvE.GroupTable(grouped, nil, make(map[string]Compare), asc)
	want := dumpGroupTree(tvE, tvE.GetLeafColumns())

	paths := collectExpandablePaths(tvE.GetFirstBlock(), nil)

	tableL, _ := demoTable()
	tvL := NewTableView(tableL, "demo")
	tvL.VisibleColumns = visible
	tvL.GroupTableWindowed(grouped, nil, make(map[string]Compare), asc, 0, GroupExpansion{Paths: paths})
	got := dumpGroupTree(tvL, tvL.GetLeafColumns())

	if got != want {
		t.Errorf("lazy fully-expanded tree differs from eager tree.\n--- lazy ---\n%s\n--- eager ---\n%s", got, want)
	}
}

// TestLazyGroupingWithFilterAndLimitMatchesEager exercises the lazy path with
// a filter mask and a display limit (top-K truncation of level 0).
func TestLazyGroupingWithFilterAndLimitMatchesEager(t *testing.T) {
	grouped := []string{"status", "region"}
	asc := map[string]bool{"status": false}

	tableE, visible := demoTable()
	tvE := NewTableView(tableE, "demo")
	tvE.VisibleColumns = visible
	tvE.ApplyFilters(map[string]string{"region": "orth"})
	tvE.GroupTableWithLimit(grouped, nil, make(map[string]Compare), asc, 2)
	want := dumpGroupTree(tvE, tvE.GetLeafColumns())

	paths := collectExpandablePaths(tvE.GetFirstBlock(), nil)

	tableL, _ := demoTable()
	tvL := NewTableView(tableL, "demo")
	tvL.VisibleColumns = visible
	tvL.ApplyFilters(map[string]string{"region": "orth"})
	tvL.GroupTableWindowed(grouped, nil, make(map[string]Compare), asc, 2, GroupExpansion{Paths: paths})
	got := dumpGroupTree(tvL, tvL.GetLeafColumns())

	if got != want {
		t.Errorf("lazy filtered/limited tree differs from eager tree.\n--- lazy ---\n%s\n--- eager ---\n%s", got, want)
	}
}

// TestIncrementalExpansionMatchesFreshBuild verifies that expanding step by
// step on a cached table view produces the same tree as building each
// expansion state from scratch, including after a collapse, and that a nested
// path implies its ancestors.
func TestIncrementalExpansionMatchesFreshBuild(t *testing.T) {
	grouped := []string{"status", "region", "category"}
	asc := map[string]bool{"status": true}

	freshDump := func(expansion GroupExpansion) string {
		table, visible := demoTable()
		tv := NewTableView(table, "demo")
		tv.VisibleColumns = visible
		tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), asc, 0, expansion)
		return dumpGroupTree(tv, tv.GetLeafColumns())
	}

	table, visible := demoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	steps := []GroupExpansion{
		{},                                         // nothing expanded
		{Paths: [][]string{{"Active"}}},            // one level-0 group
		{Paths: [][]string{{"Active", "North"}}},   // nested path implies ancestor
		{Paths: [][]string{{"Pending"}}},           // switch: collapse Active, open Pending
		{},                                         // collapse everything
	}
	for i, expansion := range steps {
		tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), asc, 0, expansion)
		got := dumpGroupTree(tv, tv.GetLeafColumns())
		want := freshDump(expansion)
		if got != want {
			t.Fatalf("step %d: incremental tree differs from fresh build.\n--- incremental ---\n%s\n--- fresh ---\n%s", i, got, want)
		}
	}

	// The nested step must have opened both Active and Active/North.
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), asc, 0,
		GroupExpansion{Paths: [][]string{{"Active", "North"}}})
	var active *grouping.Group
	for _, g := range tv.GetFirstBlock().Groups {
		if g.GetValue() == "Active" {
			active = g
		}
	}
	if active == nil || active.ChildBlock == nil {
		t.Fatal("nested path did not expand its level-0 ancestor")
	}
	opened := 0
	for _, g := range active.ChildBlock.Groups {
		if g.ChildBlock != nil {
			if g.GetValue() != "North" {
				t.Errorf("unexpected expanded child %q", g.GetValue())
			}
			opened++
		}
	}
	if opened != 1 {
		t.Errorf("%d level-1 groups expanded, want exactly 1 (North)", opened)
	}
}
