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
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// TestGroupTableReleasesMembership pins the phase-1b acceptance criterion
// that grouping no longer retains a membership list per group: after
// GroupTable returns, every group in the hierarchy carries Count, First and
// aggregates, and no group carries Indices.
func TestGroupTableReleasesMembership(t *testing.T) {
	table, visible := demoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible

	tv.GroupTable([]string{"status", "region"}, nil, make(map[string]Compare), map[string]bool{"status": true})

	var groups, totalTopLevelRows int
	var walk func(block *grouping.Block, topLevel bool)
	walk = func(block *grouping.Block, topLevel bool) {
		for _, g := range block.Groups {
			groups++
			if g.Indices != nil {
				t.Errorf("group %q still carries %d membership indices after GroupTable", g.GetValue(), len(g.Indices))
			}
			if g.Length() == 0 {
				t.Errorf("group %q has zero Count", g.GetValue())
			}
			if g.Aggregates == nil {
				t.Errorf("group %q has no aggregates", g.GetValue())
			}
			if v, err := table.GetColumn(block.GroupedColumn.DataColumn.ColumnDef().Name()).GetString(g.First); err != nil || v != g.GetValue() {
				t.Errorf("group %q: First=%d resolves to %q, err=%v", g.GetValue(), g.First, v, err)
			}
			if topLevel {
				totalTopLevelRows += g.Length()
			}
			if g.ChildBlock != nil {
				walk(g.ChildBlock, false)
			}
		}
	}
	walk(tv.GetFirstBlock(), true)

	if groups != 3+9 {
		t.Errorf("expected 12 groups (3 status x 3 regions each), got %d", groups)
	}
	if totalTopLevelRows != table.Length() {
		t.Errorf("top-level counts sum to %d, want %d", totalTopLevelRows, table.Length())
	}
}

// TestGroupTableRetainedMemory pins the phase-1b acceptance criterion that
// grouping a 1M-row / 100-group table retains O(distinct) memory, not
// O(rows). Before the migration the retained tree held every group's full
// membership (~14 MB for 1M string rows); now everything reachable after
// GroupTable is counts, representatives and aggregates.
func TestGroupTableRetainedMemory(t *testing.T) {
	if raceEnabled {
		t.Skip("memory measurement is not meaningful under the race detector")
	}
	const rows, distinct = 1_000_000, 100

	table := NewDataTable()
	col := columns.NewStringColumn(columns.NewColumnDef("value", "Value", ""))
	for i := 0; i < rows; i++ {
		col.Append(fmt.Sprintf("group-%03d", i%distinct))
	}
	col.FinalizeColumn()
	table.AddColumn(col)

	tv := NewTableView(table, "bench")
	tv.VisibleColumns = []string{"value"}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	tv.GroupTable([]string{"value"}, nil, make(map[string]Compare), make(map[string]bool))

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(tv)

	if got := len(tv.GetFirstBlock().Groups); got != distinct {
		t.Fatalf("expected %d groups, got %d", distinct, got)
	}

	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("retained by grouping: %d bytes for %d groups", retained, distinct)
	// O(distinct) for 100 groups is a few KB; 1 MiB leaves generous headroom
	// while still failing loudly on any O(rows) retention (4 MB backing alone
	// would trip it).
	if retained > 1<<20 {
		t.Errorf("grouping retained %d bytes; want O(distinct) (< 1 MiB)", retained)
	}
}
