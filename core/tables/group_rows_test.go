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

	"github.com/google/taxinomia/core/grouping"
	"github.com/google/taxinomia/core/queryspec"
)

// findGroup returns the group at path in the tree rooted at block.
func findGroup(block *grouping.Block, path ...string) *grouping.Group {
	var g *grouping.Group
	for _, v := range path {
		if block == nil {
			return nil
		}
		g = nil
		for _, c := range block.Groups {
			if c.GetValue() == v {
				g = c
				break
			}
		}
		if g == nil {
			return nil
		}
		block = g.ChildBlock
	}
	return g
}

// TestGroupRowsSorted lists the rows of one innermost group after the lazy
// build released every membership list: membership is re-resolved through
// the grouping column, the rows come sorted as asked, and the limit bounds
// the listing.
func TestGroupRowsSorted(t *testing.T) {
	table, visible := demoTable()
	tv := NewTableView(table, "demo")
	tv.VisibleColumns = visible
	tv.GroupTableWindowed([]string{"status", "region"}, nil, make(map[string]Compare), map[string]bool{"status": true}, 0,
		GroupExpansion{Paths: [][]string{{"Active"}}})

	g := findGroup(tv.GetFirstBlock(), "Active", "North")
	if g == nil {
		t.Fatal("group Active/North not built")
	}
	if g.Indices != nil {
		t.Fatal("test premise: the build must have released the membership list")
	}
	if g.Length() != 3 {
		t.Fatalf("Active/North has %d rows, want 3", g.Length())
	}

	cols := []string{"status", "region", "amount", "category"}

	// Storage order: the three rows of the group as loaded.
	all := tv.GroupRowsSorted(g, cols, nil, 0)
	if len(all) != 3 {
		t.Fatalf("got %d rows, want 3", len(all))
	}
	for i, want := range []string{"7", "10", "13"} {
		if all[i]["amount"] != want || all[i]["status"] != "Active" || all[i]["region"] != "North" {
			t.Errorf("row %d = %v, want Active/North amount %s", i, all[i], want)
		}
	}

	// Sorted by amount descending, top 2.
	top := tv.GroupRowsSorted(g, cols, []queryspec.SortColumn{{Name: "amount", Descending: true}}, 2)
	if len(top) != 2 {
		t.Fatalf("got %d rows, want 2", len(top))
	}
	for i, want := range []string{"13", "10"} {
		if top[i]["amount"] != want {
			t.Errorf("top row %d amount = %s, want %s", i, top[i]["amount"], want)
		}
	}

	// A nested group after an incremental expansion change (membership
	// re-resolved through the parent's members).
	tv.GroupTableWindowed([]string{"status", "region"}, nil, make(map[string]Compare), map[string]bool{"status": true}, 0,
		GroupExpansion{Paths: [][]string{{"Active"}, {"Pending"}}})
	p := findGroup(tv.GetFirstBlock(), "Pending", "East")
	if p == nil {
		t.Fatal("group Pending/East not built")
	}
	rows := tv.GroupRowsSorted(p, cols, nil, 0)
	if len(rows) != 3 {
		t.Fatalf("Pending/East: got %d rows, want 3", len(rows))
	}
	for _, r := range rows {
		if r["status"] != "Pending" || r["region"] != "East" {
			t.Errorf("row %v is not in Pending/East", r)
		}
	}

	if tv.GroupRowsSorted(nil, cols, nil, 0) != nil {
		t.Error("nil group must list nothing")
	}
}
