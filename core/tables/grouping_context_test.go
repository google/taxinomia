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
	"errors"
	"fmt"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// newChunkedGroupingTestView builds a chunked-backed view with two grouping
// dimensions and a numeric column, sized to span many chunks so the grouping
// build takes the parallel partition path.
func newChunkedGroupingTestView(t *testing.T, rows int) *TableView {
	t.Helper()
	table := NewDataTable()
	regionCol := columns.NewChunkedStringColumn(columns.NewColumnDef("region", "Region", ""))
	cityCol := columns.NewChunkedStringColumn(columns.NewColumnDef("city", "City", ""))
	amountCol := columns.NewChunkedInt64Column(columns.NewColumnDef("amount", "Amount", ""))
	regions := []string{"north", "south", "east", "west"}
	for i := 0; i < rows; i++ {
		regionCol.Append(regions[i%len(regions)])
		cityCol.Append(fmt.Sprintf("city-%02d", i%40))
		amountCol.Append(int64(i % 1000))
	}
	regionCol.FinalizeColumn()
	cityCol.FinalizeColumn()
	amountCol.FinalizeColumn()
	table.AddColumn(regionCol)
	table.AddColumn(cityCol)
	table.AddColumn(amountCol)
	tv := NewTableView(table, "groupctxtest")
	tv.VisibleColumns = []string{"region", "city", "amount"}
	return tv
}

// groupingTreeDump renders the grouped view's tree as comparable text.
func groupingTreeDump(tv *TableView) string {
	var out string
	var walk func(b *grouping.Block, depth int)
	walk = func(b *grouping.Block, depth int) {
		if b == nil {
			return
		}
		for _, g := range b.Groups {
			out += fmt.Sprintf("%*s%s (%d, first=%d)\n", depth*2, "", g.GetValue(), g.Length(), g.First)
			walk(g.ChildBlock, depth+1)
		}
	}
	walk(tv.firstBlock, 0)
	return out
}

// TestGroupTableWindowedContextParity: with a live context the grouped tree is
// identical to GroupTableWindowed's, eager and lazy.
func TestGroupTableWindowedContextParity(t *testing.T) {
	const rows = 32 * 1024
	order := []string{"region", "city"}
	asc := map[string]bool{"region": true, "city": true}
	expansions := []GroupExpansion{
		{ExpandAll: true},
		{Paths: [][]string{{"north"}}},
	}
	for _, exp := range expansions {
		plain := newChunkedGroupingTestView(t, rows)
		plain.GroupTableWindowed(order, nil, map[string]Compare{}, asc, 0, exp)
		ctxView := newChunkedGroupingTestView(t, rows)
		if err := ctxView.GroupTableWindowedContext(context.Background(), order, nil, map[string]Compare{}, asc, 0, exp); err != nil {
			t.Fatalf("GroupTableWindowedContext(ExpandAll=%v): %v", exp.ExpandAll, err)
		}
		if p, c := groupingTreeDump(plain), groupingTreeDump(ctxView); p != c {
			t.Fatalf("ExpandAll=%v: context tree differs from plain tree\nplain:\n%s\nctx:\n%s", exp.ExpandAll, p, c)
		}
	}
}

// TestGroupTableWindowedContextCancelled: a cancelled context aborts the
// grouping build with ctx.Err(), caches no grouping state, and a later call
// with a live context regroups from scratch and succeeds.
func TestGroupTableWindowedContextCancelled(t *testing.T) {
	const rows = 32 * 1024
	tv := newChunkedGroupingTestView(t, rows)
	order := []string{"region", "city"}
	asc := map[string]bool{"region": true, "city": true}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tv.GroupTableWindowedContext(ctx, order, nil, map[string]Compare{}, asc, 0, GroupExpansion{ExpandAll: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GroupTableWindowedContext under cancelled context = %v, want context.Canceled", err)
	}
	if tv.firstBlock != nil || len(tv.groupedColumns) != 0 || tv.lastExpansion != nil {
		t.Fatal("cancelled grouping build left grouping state behind")
	}

	if err := tv.GroupTableWindowedContext(context.Background(), order, nil, map[string]Compare{}, asc, 0, GroupExpansion{ExpandAll: true}); err != nil {
		t.Fatalf("GroupTableWindowedContext after cancellation: %v", err)
	}
	if got := len(tv.firstBlock.Groups); got != 4 {
		t.Fatalf("regrouped view has %d level-0 groups, want 4", got)
	}
}

// TestGroupTableWindowedContextCancelledIncremental: cancellation during an
// incremental expansion update also drops all grouping state — nothing
// half-synchronized survives — and the next request rebuilds.
func TestGroupTableWindowedContextCancelledIncremental(t *testing.T) {
	const rows = 32 * 1024
	tv := newChunkedGroupingTestView(t, rows)
	order := []string{"region", "city"}
	asc := map[string]bool{"region": true, "city": true}

	if err := tv.GroupTableWindowedContext(context.Background(), order, nil, map[string]Compare{}, asc, 0, GroupExpansion{Paths: nil}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tv.GroupTableWindowedContext(ctx, order, nil, map[string]Compare{}, asc, 0, GroupExpansion{Paths: [][]string{{"north"}}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("incremental update under cancelled context = %v, want context.Canceled", err)
	}
	if tv.firstBlock != nil || tv.lastExpansion != nil {
		t.Fatal("cancelled incremental update left grouping state behind")
	}

	if err := tv.GroupTableWindowedContext(context.Background(), order, nil, map[string]Compare{}, asc, 0, GroupExpansion{Paths: [][]string{{"north"}}}); err != nil {
		t.Fatalf("regroup after cancelled incremental update: %v", err)
	}
	if got := len(tv.firstBlock.Groups); got != 4 {
		t.Fatalf("regrouped view has %d level-0 groups, want 4", got)
	}
}
