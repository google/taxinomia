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
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// TestAggregateSortOnReusedGrouping: the grouping tree is reused across
// requests while its inputs are unchanged, and SortGroupsByAggregate
// reorders it in place. Each call must start from the build's order, so a
// second-level sort that is removed or changed does not leave the previous
// request's order behind (seen as the ⟳ cycle of a second grouped column
// apparently not working).
func TestAggregateSortOnReusedGrouping(t *testing.T) {
	table := NewDataTable()
	p := columns.NewStringColumn(columns.NewColumnDef("p", "P", ""))
	g := columns.NewStringColumn(columns.NewColumnDef("g", "G", ""))
	// Under each p: x has 1 row, y 3, z 2. Value order x y z, rows order x z y.
	for _, pv := range []string{"a", "b"} {
		for _, gv := range []string{"x", "y", "y", "y", "z", "z"} {
			p.Append(pv)
			g.Append(gv)
		}
	}
	p.FinalizeColumn()
	g.FinalizeColumn()
	table.AddColumn(p)
	table.AddColumn(g)

	tv := NewTableView(table, "t")
	tv.VisibleColumns = []string{"p", "g"}
	grouped := []string{"p", "g"}
	expandAll := GroupExpansion{ExpandAll: true}
	order := func() string {
		var parts []string
		for _, pg := range tv.GetFirstBlock().Groups {
			var sub []string
			for _, cg := range pg.ChildBlock.Groups {
				sub = append(sub, cg.GetValue())
			}
			parts = append(parts, pg.GetValue()+":"+strings.Join(sub, ","))
		}
		return strings.Join(parts, " ")
	}
	request := func(sorts map[string]*queryspec.GroupAggSort) string {
		tv.GroupTableWindowed(grouped, nil, make(map[string]Compare), make(map[string]bool), 0, expandAll)
		tv.SortGroupsByAggregate(sorts)
		return order()
	}
	byRows := map[string]*queryspec.GroupAggSort{"g": {GroupedColumn: "g", AggType: queryspec.AggRowCount}}
	byRowsDesc := map[string]*queryspec.GroupAggSort{"g": {GroupedColumn: "g", AggType: queryspec.AggRowCount, Descending: true}}

	if got, want := request(nil), "a:x,y,z b:x,y,z"; got != want {
		t.Fatalf("value order = %q, want %q", got, want)
	}
	tree := tv.GetFirstBlock()
	if got, want := request(byRows), "a:x,z,y b:x,z,y"; got != want {
		t.Fatalf("rows ascending = %q, want %q", got, want)
	}
	if tv.GetFirstBlock() != tree {
		t.Fatalf("grouping was rebuilt; the test needs the reused tree")
	}
	if got, want := request(byRowsDesc), "a:y,z,x b:y,z,x"; got != want {
		t.Fatalf("rows descending = %q, want %q", got, want)
	}
	if got, want := request(nil), "a:x,y,z b:x,y,z"; got != want {
		t.Fatalf("after removing the sort = %q, want value order %q", got, want)
	}
}
