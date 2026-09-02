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

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// buildAggSortTable builds a table with a string group column "g" holding
// nGroups distinct values ("g00".."gNN", rowsPerGroup rows each) and a
// float column "amount" arranged so that the value-wise LAST group has the
// largest sum: sum(g_k) grows with k.
func buildAggSortTable(nGroups, rowsPerGroup int) *DataTable {
	table := NewDataTable()
	gCol := columns.NewStringColumn(columns.NewColumnDef("g", "G", ""))
	aCol := columns.NewFloat64Column(columns.NewColumnDef("amount", "Amount", ""))
	for k := 0; k < nGroups; k++ {
		for r := 0; r < rowsPerGroup; r++ {
			gCol.Append(fmt.Sprintf("g%02d", k))
			aCol.Append(float64(k + 1))
		}
	}
	gCol.FinalizeColumn()
	aCol.FinalizeColumn()
	table.AddColumn(gCol)
	table.AddColumn(aCol)
	return table
}

// TestGroupingRebuildOnDisplayLimitChange: grouping state built under one
// display limit must not be reused for a request with a different limit —
// the level-0 top-K trim bakes the limit into the retained groups.
func TestGroupingRebuildOnDisplayLimitChange(t *testing.T) {
	const nGroups = 100
	table := buildAggSortTable(nGroups, 10)
	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"g", "amount"}

	grouped := []string{"g"}
	noSort := make(map[string]bool)
	expandAll := GroupExpansion{ExpandAll: true}

	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 10, expandAll)
	if got := len(tv.GetFirstBlock().Groups); got != 10 {
		t.Fatalf("trimmed grouping has %d groups, want 10", got)
	}

	// Same grouping, no trim: must rebuild, not reuse the 10-group state.
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 0, expandAll)
	if got := len(tv.GetFirstBlock().Groups); got != nGroups {
		t.Fatalf("after limit change 10→0 the grouping has %d groups, want %d (stale trimmed state reused)", got, nGroups)
	}
}

// TestAggregateSortRanksAllGroups pins the handler contract behind
// urlquery.EffectiveGroupDisplayLimit: when groups are ordered by an
// aggregate, the grouping must be built untrimmed (limit 0) so the sort
// ranks every group — the display limit is applied at render time. The
// aggregate-largest group here is the value-wise last ("g99"), which a
// value-trimmed build would have discarded before the aggregate sort ran.
func TestAggregateSortRanksAllGroups(t *testing.T) {
	const nGroups = 100
	table := buildAggSortTable(nGroups, 10)
	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"g", "amount"}

	grouped := []string{"g"}
	noSort := make(map[string]bool)
	expandAll := GroupExpansion{ExpandAll: true}

	// The buggy shape (kept as documentation of the mechanism): a value-wise
	// trim to 25 discards g99 before any aggregate sort could rank it.
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 25, expandAll)
	for _, g := range tv.GetFirstBlock().Groups {
		if g.GetValue() == "g99" {
			t.Fatalf("value-trimmed build retained g99; test premise broken")
		}
	}

	// The fixed shape: untrimmed build, aggregate sort, then render-limit.
	tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), noSort, 0, expandAll)
	tv.ComputeAggregates([]string{"amount"}, map[string]queryspec.ColumnType{"amount": queryspec.ColumnTypeNumeric})
	tv.SortGroupsByAggregate(map[string]*queryspec.GroupAggSort{
		"g": {GroupedColumn: "g", LeafColumn: "amount", AggType: queryspec.AggSum, Descending: true},
	})

	groups := tv.GetFirstBlock().Groups
	if len(groups) != nGroups {
		t.Fatalf("aggregate-sorted grouping has %d groups, want %d", len(groups), nGroups)
	}
	if got := groups[0].GetValue(); got != "g99" {
		t.Fatalf("top group by sum is %q, want g99 (largest sum)", got)
	}
	if got := groups[nGroups-1].GetValue(); got != "g00" {
		t.Fatalf("bottom group by sum is %q, want g00 (smallest sum)", got)
	}
}

// TestRegroupingReleasesPreviousBlocks: repeated regrouping on one view must
// not accumulate block trees in the registry — before the fix, every rebuild
// appended its blocks to blocksByColumn while ClearGroupings and the eager
// build never reset it, keeping every previous grouping's groups reachable
// (measured as warm grouping getting slower than cold at 1e7 in benchsuite Q6).
func TestRegroupingReleasesPreviousBlocks(t *testing.T) {
	table := buildAggSortTable(100, 100)
	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"g", "amount"}
	expandAll := GroupExpansion{ExpandAll: true}

	for i := 0; i < 5; i++ {
		tv.ClearGroupings()
		tv.GroupTableWindowed([]string{"g"}, nil, make(map[string]Compare), make(map[string]bool), 0, expandAll)
	}
	if got := len(tv.blocksByColumn["g"]); got != 1 {
		t.Errorf("after 5 regroupings the registry holds %d level-0 blocks, want 1 (previous trees leaked)", got)
	}

	// Regroup without an intervening ClearGroupings (the handler path when
	// the grouping inputs change): still exactly one tree registered.
	tv.GroupTableWindowed([]string{"g"}, nil, make(map[string]Compare), map[string]bool{"g": false}, 0, expandAll)
	if got := len(tv.blocksByColumn["g"]); got != 1 {
		t.Errorf("after an in-place regroup the registry holds %d level-0 blocks, want 1", got)
	}
}

// TestAggregateNeedsSkipsStates: with SetAggregateNeeds, storage columns not
// in the needs map skip aggregate-state building (their count is the group
// size); changing the needs invalidates the grouping cache; nil needs keeps
// the historical compute-everything behavior.
func TestAggregateNeedsSkipsStates(t *testing.T) {
	table := NewDataTable()
	gCol := columns.NewStringColumn(columns.NewColumnDef("g", "G", ""))
	noteCol := columns.NewStringColumn(columns.NewColumnDef("note", "Note", ""))
	aCol := columns.NewFloat64Column(columns.NewColumnDef("amount", "Amount", ""))
	for i := 0; i < 100; i++ {
		gCol.Append(fmt.Sprintf("g%d", i%4))
		noteCol.Append(fmt.Sprintf("n%d", i))
		aCol.Append(float64(i))
	}
	gCol.FinalizeColumn()
	noteCol.FinalizeColumn()
	aCol.FinalizeColumn()
	table.AddColumn(gCol)
	table.AddColumn(noteCol)
	table.AddColumn(aCol)

	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"g", "note", "amount"}
	expandAll := GroupExpansion{ExpandAll: true}

	tv.SetAggregateNeeds(map[string]bool{"amount": true})
	tv.GroupTableWindowed([]string{"g"}, nil, make(map[string]Compare), make(map[string]bool), 0, expandAll)
	g0 := tv.GetFirstBlock().Groups[0]
	if g0.Aggregates["amount"] == nil {
		t.Error("amount state missing despite being needed")
	}
	if g0.Aggregates["note"] != nil {
		t.Error("note state built despite count-only needs (should be skipped)")
	}

	// Changing needs must invalidate the cached grouping.
	tv.SetAggregateNeeds(nil)
	tv.GroupTableWindowed([]string{"g"}, nil, make(map[string]Compare), make(map[string]bool), 0, expandAll)
	g0 = tv.GetFirstBlock().Groups[0]
	if g0.Aggregates["note"] == nil {
		t.Error("note state missing after needs reset to nil (legacy compute-everything)")
	}
}

// TestLevelZeroAggSortBuildsOnlyTopSubtrees: with an in-build level-0
// aggregate ranking, all level-0 groups are ranked (complete, sorted by
// the aggregate) but child subtrees exist only for the displayed top-K;
// the rest are final leaves with aggregates and released membership.
func TestLevelZeroAggSortBuildsOnlyTopSubtrees(t *testing.T) {
	table := buildAggSortTable(100, 10) // sum grows with group index
	sub := columns.NewStringColumn(columns.NewColumnDef("sub", "Sub", ""))
	for i := 0; i < 1000; i++ {
		sub.Append(fmt.Sprintf("s%d", i%3))
	}
	sub.FinalizeColumn()
	table.AddColumn(sub)

	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"g", "sub", "amount"}
	tv.SetLevelZeroAggSort(&queryspec.GroupAggSort{
		GroupedColumn: "g", LeafColumn: "amount", AggType: queryspec.AggSum, Descending: true,
	})
	tv.GroupTableWindowed([]string{"g", "sub"}, nil, make(map[string]Compare), make(map[string]bool), 10, GroupExpansion{ExpandAll: true})

	groups := tv.GetFirstBlock().Groups
	if len(groups) != 100 {
		t.Fatalf("level 0 has %d groups, want all 100 ranked", len(groups))
	}
	if groups[0].GetValue() != "g99" || groups[99].GetValue() != "g00" {
		t.Fatalf("ranking wrong: first=%s last=%s, want g99..g00", groups[0].GetValue(), groups[99].GetValue())
	}
	for i, g := range groups {
		if i < 10 {
			if g.ChildBlock == nil {
				t.Errorf("top group %d (%s) missing child subtree", i, g.GetValue())
			}
		} else {
			if g.ChildBlock != nil {
				t.Errorf("group %d (%s) has a subtree despite being outside the display window", i, g.GetValue())
			}
			if g.Aggregates["amount"] == nil {
				t.Errorf("group %d (%s) missing aggregates (needed for the ranking)", i, g.GetValue())
			}
		}
	}
}

// TestUniqueAggregateCompactionAndKeyShortcut: (a) key columns never build
// per-group unique sets — unique equals count by construction; (b) after an
// eager build, remaining unique sets are compacted to counts so grouping
// state does not retain member strings.
func TestUniqueAggregateCompactionAndKeyShortcut(t *testing.T) {
	table := NewDataTable()
	idSrc := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "t.id"))
	gCol := columns.NewChunkedStringColumn(columns.NewColumnDef("g", "G", ""))
	noteCol := columns.NewChunkedStringColumn(columns.NewColumnDef("note", "Note", ""))
	for i := 0; i < 1000; i++ {
		idSrc.Append(fmt.Sprintf("k%04d", i))
		gCol.Append(fmt.Sprintf("g%d", i%4))
		noteCol.Append(fmt.Sprintf("n%d", i%3))
	}
	table.AddColumn(idSrc)
	table.AddColumn(gCol)
	table.AddColumn(noteCol)
	if err := table.SortByKey([]string{"id"}); err != nil {
		t.Fatal(err)
	}
	table.SelectEncodings() // id becomes the front-coded key

	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"id", "g", "note"}
	tv.GroupTableWindowed([]string{"g"}, nil, make(map[string]Compare), make(map[string]bool), 0, GroupExpansion{ExpandAll: true})

	g0 := tv.GetFirstBlock().Groups[0]
	idState, ok := g0.Aggregates["id"].(*aggregates.StringAggState)
	if !ok {
		t.Fatalf("no string state for id; got %T", g0.Aggregates["id"])
	}
	if !idState.KeyUnique {
		t.Error("key column state not marked KeyUnique")
	}
	if got, want := idState.UniqueCount(), g0.Length(); got != want {
		t.Errorf("key unique = %d, want group size %d", got, want)
	}

	noteState := g0.Aggregates["note"].(*aggregates.StringAggState)
	if noteState.UniqueSet != nil {
		t.Error("note unique set not compacted after eager build (retains member strings)")
	}
	if got := noteState.UniqueCount(); got != 3 {
		t.Errorf("note unique = %d after compaction, want 3", got)
	}
}
