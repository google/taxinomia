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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/taxinomia/core/columns"
)

func stepNames(steps []GroupingStep) string {
	names := make([]string, len(steps))
	for i, s := range steps {
		names[i] = s.Name
	}
	return strings.Join(names, " | ")
}

// threeColumnTable is keyedTable plus a two-valued "sub" column for
// two-level groupings.
func threeColumnTable(t *testing.T, n int) *DataTable {
	t.Helper()
	tbl := keyedTable(t, n)
	sub := columns.NewChunkedStringColumn(columns.NewColumnDef("sub", "sub", ""))
	for i := 0; i < n; i++ {
		sub.Append(fmt.Sprintf("s%d", i%2))
	}
	sub.FinalizeColumn()
	tbl.AddColumn(sub)
	return tbl
}

// TestGroupingStepsCoverTheBuild: the grouping build reports its steps in
// execution order, a cache hit reports a single cached step, and a
// two-level build reports each level and the per-row aggregate pass.
func TestGroupingStepsCoverTheBuild(t *testing.T) {
	tv := NewTableView(keyedTable(t, 60), "t")
	tv.VisibleColumns = []string{"id", "grp"}
	tv.SetAggregateNeeds(map[string]bool{})
	group := func(tv *TableView, order []string) {
		t.Helper()
		if err := tv.GroupTableWindowedContext(context.Background(), order, nil, map[string]Compare{}, map[string]bool{}, 25, GroupExpansion{ExpandAll: true}); err != nil {
			t.Fatal(err)
		}
	}
	group(tv, []string{"grp"})
	got := stepNames(tv.LastGroupingSteps())
	for _, want := range []string{"partition by grp: 3 groups", "sort level 0 by value, keep top 25", "aggregates: none needed", "release membership"} {
		if !strings.Contains(got, want) {
			t.Errorf("steps %q lack %q", got, want)
		}
	}
	// Same inputs again: served from the cache.
	group(tv, []string{"grp"})
	if got := stepNames(tv.LastGroupingSteps()); !strings.Contains(got, "cached") || strings.Contains(got, "partition") {
		t.Errorf("cache hit steps = %q", got)
	}
	// Two levels with an aggregate needed: each level and the per-row pass show up.
	tv = NewTableView(threeColumnTable(t, 60), "t")
	tv.VisibleColumns = []string{"id", "grp", "sub"}
	tv.SetAggregateNeeds(map[string]bool{"id": true})
	group(tv, []string{"grp", "sub"})
	got = stepNames(tv.LastGroupingSteps())
	for _, want := range []string{"partition by grp", "level 1 by sub: 3 blocks, 6 groups", "aggregates per row: id (all levels)"} {
		if !strings.Contains(got, want) {
			t.Errorf("two-level steps %q lack %q", got, want)
		}
	}
}
