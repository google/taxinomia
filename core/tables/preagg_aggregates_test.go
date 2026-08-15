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
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// preAggTable builds a multi-chunk table around a dictionary-encoded grouping
// dimension: numeric, bool and datetime measures that pre-aggregate, plus a
// string measure and a plain (non-chunked) measure that must fall back to the
// per-row path in the same request. Integer-valued floats keep formatted sums
// exact across both paths.
func preAggTable(n int) (*DataTable, []string) {
	table := NewDataTable()

	dim := columns.NewChunkedDictStringColumn[uint16](columns.NewColumnDef("dim", "Dim", ""))
	sub := columns.NewChunkedDictStringColumn[uint16](columns.NewColumnDef("sub", "Sub", ""))
	cat := columns.NewChunkedStringColumn(columns.NewColumnDef("cat", "Cat", ""))
	score := columns.NewChunkedFloat64Column(columns.NewColumnDef("score", "Score", ""))
	amount := columns.NewChunkedUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	flag := columns.NewChunkedBoolColumn(columns.NewColumnDef("flag", "Flag", ""))
	when := columns.NewChunkedDatetimeColumn(columns.NewColumnDef("when", "When", ""))
	note := columns.NewChunkedStringColumn(columns.NewColumnDef("note", "Note", ""))
	plain := columns.NewInt64Column(columns.NewColumnDef("plain", "Plain", ""))

	base := time.Date(2024, 5, 1, 8, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		dim.Append(fmt.Sprintf("d%02d", i%40))
		sub.Append(fmt.Sprintf("s%d", (i/7)%3))
		cat.Append(fmt.Sprintf("c%d", i%3))
		score.Append(float64(i%100 - 50))
		amount.Append(uint32(i % 1000))
		flag.Append(i%4 == 0)
		when.Append(base.Add(time.Duration(i%5000) * time.Minute))
		note.Append(fmt.Sprintf("note-%d", i%6))
		plain.Append(int64(i % 17))
	}
	dim.FinalizeColumn()
	sub.FinalizeColumn()
	cat.FinalizeColumn()
	score.FinalizeColumn()
	amount.FinalizeColumn()
	flag.FinalizeColumn()
	when.FinalizeColumn()
	note.FinalizeColumn()
	plain.FinalizeColumn()
	for _, c := range []columns.IDataColumn{dim, sub, cat, score, amount, flag, when, note, plain} {
		table.AddColumn(c)
	}
	return table, []string{"dim", "sub", "cat", "score", "amount", "flag", "when", "note", "plain"}
}

// bulkParityDump runs the given grouping request twice on fresh views over
// fresh (identical) tables — once with the bulk path, once forced per-row —
// asserts the bulk path actually engaged for expectBulk columns, and returns
// both aggregate dumps for comparison.
func bulkParityDump(t *testing.T, n int, expectBulk uint64, request func(tv *TableView)) (withBulk, perRow string) {
	t.Helper()

	run := func(disabled bool) string {
		table, visible := preAggTable(n)
		tv := NewTableView(table, "preagg")
		tv.VisibleColumns = visible
		orig := disableBulkAggregates
		disableBulkAggregates = disabled
		defer func() { disableBulkAggregates = orig }()
		request(tv)
		return dumpGroupTree(tv, tv.GetLeafColumns())
	}

	before := bulkAggregatedColumns.Load()
	withBulk = run(false)
	if got := bulkAggregatedColumns.Load() - before; got != expectBulk {
		t.Fatalf("bulk path aggregated %d columns, want %d", got, expectBulk)
	}
	before = bulkAggregatedColumns.Load()
	perRow = run(true)
	if got := bulkAggregatedColumns.Load() - before; got != 0 {
		t.Fatalf("disabled run aggregated %d columns in bulk, want 0", got)
	}
	return withBulk, perRow
}

// TestBulkAggregatesParityEager: single-level grouping over the full
// universe — every level-0 group is a leaf, the four supported measures merge
// per-chunk partials, the string and plain measures fall back — and every
// formatted aggregate matches the per-row path exactly.
func TestBulkAggregatesParityEager(t *testing.T) {
	const n = 200_000 // 4 chunks at DefaultChunkSize, the last partial
	withBulk, perRow := bulkParityDump(t, n, 4, func(tv *TableView) {
		tv.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
	})
	if withBulk != perRow {
		t.Errorf("aggregate dumps differ:\n--- bulk ---\n%s--- per-row ---\n%s", withBulk, perRow)
	}
}

// TestBulkAggregatesParityFiltered: an unquoted filter selects a third of the
// rows spread through every chunk, so the merge classifies every chunk as cut
// through and scans only selected rows.
func TestBulkAggregatesParityFiltered(t *testing.T) {
	const n = 200_000
	withBulk, perRow := bulkParityDump(t, n, 4, func(tv *TableView) {
		tv.ApplyFilters(map[string]string{"cat": `"c1"`})
		tv.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
	})
	if withBulk != perRow {
		t.Errorf("aggregate dumps differ:\n--- bulk ---\n%s--- per-row ---\n%s", withBulk, perRow)
	}
}

// TestBulkAggregatesParityMultiLevelEager: with a second grouping level built
// eagerly, level-0 groups are parents (combined from their children, which
// aggregate per row); the bulk pass must not disturb the combine path.
func TestBulkAggregatesParityMultiLevelEager(t *testing.T) {
	const n = 100_000
	withBulk, perRow := bulkParityDump(t, n, 0, func(tv *TableView) {
		tv.GroupTable([]string{"dim", "sub"}, nil, make(map[string]Compare), map[string]bool{"dim": true})
	})
	if withBulk != perRow {
		t.Errorf("aggregate dumps differ:\n--- bulk ---\n%s--- per-row ---\n%s", withBulk, perRow)
	}
}

// TestBulkAggregatesParityLazyExpansion: lazy grouping with one expanded
// group mixes both paths at level 0 — collapsed groups are leaves (bulk),
// the expanded group is a parent combined from per-row children — and an
// incremental expansion switch keeps every retained state consistent.
func TestBulkAggregatesParityLazyExpansion(t *testing.T) {
	const n = 100_000
	expansions := []GroupExpansion{
		{Paths: [][]string{{"d03"}}},
		{Paths: [][]string{{"d03"}, {"d17"}}},
	}
	run := func(disabled bool) []string {
		table, visible := preAggTable(n)
		tv := NewTableView(table, "preagg")
		tv.VisibleColumns = visible
		orig := disableBulkAggregates
		disableBulkAggregates = disabled
		defer func() { disableBulkAggregates = orig }()
		var dumps []string
		for _, exp := range expansions {
			tv.GroupTableWindowed([]string{"dim", "sub"}, nil, make(map[string]Compare), map[string]bool{"dim": true}, 0, exp)
			dumps = append(dumps, dumpGroupTree(tv, tv.GetLeafColumns()))
		}
		return dumps
	}
	withBulk := run(false)
	perRow := run(true)
	for i := range expansions {
		if withBulk[i] != perRow[i] {
			t.Errorf("expansion step %d: aggregate dumps differ:\n--- bulk ---\n%s--- per-row ---\n%s", i, withBulk[i], perRow[i])
		}
	}
}

// TestBulkAggregatesRepeatKeepsStates: a repeated ComputeAggregates call
// after membership release keeps the level-0 aggregate states instead of
// remerging (the bulk pass runs only when some group needs it).
func TestBulkAggregatesRepeatKeepsStates(t *testing.T) {
	const n = 100_000
	table, visible := preAggTable(n)
	tv := NewTableView(table, "preagg")
	tv.VisibleColumns = visible
	tv.GroupTable([]string{"dim"}, nil, make(map[string]Compare), map[string]bool{"dim": true})

	first := tv.GetFirstBlock()
	states := make(map[uint32]any)
	for _, g := range first.Groups {
		states[g.GroupKey] = g.Aggregates["score"]
	}
	before := bulkAggregatedColumns.Load()
	leafColumns := tv.GetLeafColumns()
	columnTypes := make(map[string]queryspec.ColumnType)
	for _, colName := range leafColumns {
		columnTypes[colName] = tv.GetColumnType(colName)
	}
	tv.ComputeAggregates(leafColumns, columnTypes)
	if got := bulkAggregatedColumns.Load() - before; got != 0 {
		t.Errorf("repeat ComputeAggregates ran the bulk pass for %d columns, want 0", got)
	}
	for _, g := range tv.GetFirstBlock().Groups {
		if states[g.GroupKey] != g.Aggregates["score"] {
			t.Errorf("group %q: aggregate state replaced on repeat call", g.GetValue())
		}
	}
}
