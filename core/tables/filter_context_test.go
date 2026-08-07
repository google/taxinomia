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
)

// newChunkedFilterTestView builds a table backed by chunked columns (the
// loader-produced storage), so the structured Context filter paths are the
// ones exercised.
func newChunkedFilterTestView(t *testing.T, rows int) *TableView {
	t.Helper()
	table := NewDataTable()
	regionCol := columns.NewChunkedStringColumn(columns.NewColumnDef("region", "Region", ""))
	noteCol := columns.NewChunkedStringColumn(columns.NewColumnDef("note", "Note", ""))
	regions := []string{"North", "south", "east", "west", "northeast"}
	for i := 0; i < rows; i++ {
		regionCol.Append(regions[i%len(regions)])
		noteCol.Append(fmt.Sprintf("note-%d", i%7))
	}
	regionCol.FinalizeColumn()
	noteCol.FinalizeColumn()
	table.AddColumn(regionCol)
	table.AddColumn(noteCol)
	tv := NewTableView(table, "filterctxtest")
	tv.VisibleColumns = []string{"region", "note"}
	return tv
}

// TestApplyFiltersContextParity: with a live context, ApplyFiltersContext
// selects exactly what ApplyFilters selects, for every filter form.
func TestApplyFiltersContextParity(t *testing.T) {
	const rows = 1000
	filterSets := []map[string]string{
		{"region": `"North"`},
		{"note": "note-1|note-3"},
		{"region": "orth"},
		{"region": `"North"`, "note": "note-1|note-3"},
	}
	for _, filters := range filterSets {
		plain := newChunkedFilterTestView(t, rows)
		plain.ApplyFilters(filters)
		ctxView := newChunkedFilterTestView(t, rows)
		if err := ctxView.ApplyFiltersContext(context.Background(), filters); err != nil {
			t.Fatalf("ApplyFiltersContext(%v): %v", filters, err)
		}
		if p, c := plain.GetFilteredRowCount(), ctxView.GetFilteredRowCount(); p != c {
			t.Fatalf("filters %v: ApplyFilters selects %d rows, ApplyFiltersContext %d", filters, p, c)
		}
	}
}

// TestApplyFiltersContextCancelled: a cancelled context aborts filtering
// with ctx.Err(), caches nothing (the view behaves as unfiltered), and a
// later call with a live context recomputes and succeeds.
func TestApplyFiltersContextCancelled(t *testing.T) {
	const rows = 1000
	tv := newChunkedFilterTestView(t, rows)
	filters := map[string]string{"region": `"North"`}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tv.ApplyFiltersContext(ctx, filters); !errors.Is(err, context.Canceled) {
		t.Fatalf("ApplyFiltersContext under cancelled context = %v, want context.Canceled", err)
	}
	if count := tv.GetFilteredRowCount(); count != rows {
		t.Fatalf("after cancellation the view holds a partial filter: %d rows visible, want all %d", count, rows)
	}

	if err := tv.ApplyFiltersContext(context.Background(), filters); err != nil {
		t.Fatalf("ApplyFiltersContext after cancellation: %v", err)
	}
	if count := tv.GetFilteredRowCount(); count != rows/5 {
		t.Fatalf("recomputed filter selects %d rows, want %d", count, rows/5)
	}
}
