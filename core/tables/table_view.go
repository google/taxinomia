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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
	"github.com/google/taxinomia/core/hrclock"
	"github.com/google/taxinomia/core/queryspec"
)

// groupHeap implements a max-heap for top-K group selection.
// When we want the K "best" groups (smallest for asc, largest for desc),
// we use a max-heap where the "worst" of the K best is at the top.
type groupHeap struct {
	groups     []*grouping.Group
	col        columns.IDataColumn
	descending bool
}

func (h *groupHeap) Len() int { return len(h.groups) }

// Less returns true if element at i should be ABOVE j in the heap.
// For a max-heap of "best" elements, the worst element should be at the top.
func (h *groupHeap) Less(i, j int) bool {
	idxI := h.groups[i].First
	idxJ := h.groups[j].First
	cmp := columns.CompareAtIndex(h.col, idxI, idxJ)
	// For ascending sort: we want smallest, so largest (worst) should be at top
	// For descending sort: we want largest, so smallest (worst) should be at top
	if h.descending {
		return cmp < 0 // smaller values bubble up (they're "worse" for descending)
	}
	return cmp > 0 // larger values bubble up (they're "worse" for ascending)
}

func (h *groupHeap) Swap(i, j int) {
	h.groups[i], h.groups[j] = h.groups[j], h.groups[i]
}

func (h *groupHeap) Push(x interface{}) {
	h.groups = append(h.groups, x.(*grouping.Group))
}

func (h *groupHeap) Pop() interface{} {
	old := h.groups
	n := len(old)
	x := old[n-1]
	h.groups = old[0 : n-1]
	return x
}

// JoinResolver is an interface for resolving join and table information
// The GetJoin method returns an object that has a GetJoiner() method
type JoinResolver interface {
	GetJoin(key string) interface{} // Returns object with GetJoiner() columns.IJoiner method
	GetTable(name string) *DataTable
}

// Compare is a placeholder type for comparison functions (WIP)
type Compare int

// TableView represents a view of a DataTable with additional joined columns.
// This allows the underlying DataTable to remain immutable while views can
// have their own set of joined columns based on the query context.
type TableView struct {
	baseTable       *DataTable
	tableName       string
	VisibleColumns  []string
	joins           map[string]columns.IJoinedDataColumn
	computedColumns map[string]columns.IDataColumn

	groupedColumns map[string]*grouping.GroupedColumn
	groupingOrder  []string
	blocksByColumn map[string][]*grouping.Block
	columnViews    map[string]*columns.ColumnView
	firstBlock     *grouping.Block

	// Filtering
	filterSel   *columns.Selection // Cached filter selection bitmap (nil = no filter, all rows shown)
	// filtersRecomputed reports whether the last ApplyFilters call scanned
	// the table (false: filters unchanged, selection reused).
	filtersRecomputed bool
	lastFilters map[string]string  // Filters that produced current selection (for change detection)

	// Grouping cache tracking
	lastGroupingOrder   []string                // Grouping order when grouping was computed
	lastGroupingFilters map[string]string       // Filter state when grouping was computed
	lastGroupingSortAsc map[string]bool         // Sort direction when grouping was computed
	lastExpansion       *GroupExpansion         // Expansion state when grouping was computed (nil = never grouped)
	lastDisplayLimit    int                     // Display limit (level-0 top-K trim) when grouping was computed
	aggNeeds            map[string]bool         // Columns needing full aggregate state (nil = all; see SetAggregateNeeds)
	lastAggNeeds        map[string]bool         // aggNeeds when grouping was computed
	level0AggSort       *queryspec.GroupAggSort // In-build aggregate ranking of level-0 groups (nil = value order)
	lastLevel0AggSort   *queryspec.GroupAggSort // level0AggSort when grouping was computed

	// Grouping build timing (see grouping_steps.go)
	clock         hrclock.Clock  // nil = hrclock.System()
	groupingSteps []GroupingStep // steps of the last grouping call
}

// SetLevelZeroAggSort tells the eager grouping build to rank level-0 groups
// by an aggregate during the build, so child subtrees are constructed only
// for the displayed top of the ranking instead of for every group. nil (the
// default) keeps value ordering; callers then apply SortGroupsByAggregate
// after the build if they need aggregate order, at the cost of a full-tree
// build. The subgroup-count kind cannot rank before children exist and is
// ignored here.
func (t *TableView) SetLevelZeroAggSort(s *queryspec.GroupAggSort) {
	t.level0AggSort = s
}

// withAggSortColumn ensures the level-0 ranking column is part of an
// aggregate column set even when it is not displayed: a groupsort may
// reference a column outside the visible set (columnless URLs default to
// the first few columns), and ranking by a column with no state silently
// produces garbage order.
func (t *TableView) withAggSortColumn(cols []string) []string {
	s := t.level0AggSort
	if s == nil || s.LeafColumn == "" || t.IsColGrouped(s.LeafColumn) || t.GetColumn(s.LeafColumn) == nil {
		return cols
	}
	for _, c := range cols {
		if c == s.LeafColumn {
			return cols
		}
	}
	return append(append([]string(nil), cols...), s.LeafColumn)
}

// childUniqueSetsCompacted reports whether any child group carries a
// string aggregate whose unique set was already released — after which a
// parent recombine cannot reproduce unique counts.
func childUniqueSetsCompacted(block *grouping.Block) bool {
	if block == nil {
		return false
	}
	for _, group := range block.Groups {
		for _, state := range group.Aggregates {
			if s, ok := state.(*aggregates.StringAggState); ok {
				if s.UniqueSet == nil && !s.KeyUnique && s.Count > 0 {
					return true
				}
			}
		}
	}
	return false
}

// compactUniqueSets walks all groups reachable from block and releases
// their string unique sets, keeping the counts only.
func (t *TableView) compactUniqueSets(block *grouping.Block) {
	if block == nil {
		return
	}
	for _, group := range block.Groups {
		for _, state := range group.Aggregates {
			if s, ok := state.(*aggregates.StringAggState); ok {
				s.ReleaseUniqueSet()
			}
		}
		t.compactUniqueSets(group.ChildBlock)
	}
}

// computeLevelZeroAggregates materializes aggregate states for the level-0
// groups, for the RANKING COLUMN ONLY: the in-build aggregate sort needs
// exactly that column across all groups; every other enabled aggregate is
// computed later, by the normal walk, for just the displayed subtrees.
// Row-count ranking kinds (empty LeafColumn) need no states at all.
func (t *TableView) computeLevelZeroAggregates(ctx context.Context) error {
	s := t.level0AggSort
	if s == nil || s.LeafColumn == "" || t.firstBlock == nil {
		return nil
	}
	if t.GetColumn(s.LeafColumn) == nil || t.IsColGrouped(s.LeafColumn) {
		return nil
	}
	leafColumns := []string{s.LeafColumn}
	columnTypes := make(map[string]queryspec.ColumnType, len(leafColumns))
	for _, colName := range leafColumns {
		columnTypes[colName] = t.GetColumnType(colName)
	}
	bulk, err := t.bulkLevel0Aggregates(ctx, leafColumns, columnTypes)
	if err != nil {
		return err
	}
	for _, group := range t.firstBlock.Groups {
		if err := ctx.Err(); err != nil {
			return err
		}
		group.Aggregates = make(map[string]aggregates.AggregateState)
		t.computeLeafAggregates(group, leafColumns, columnTypes, bulk)
	}
	return nil
}

// SetAggregateNeeds tells grouping which leaf columns need a full aggregate
// state. nil (the default) computes states for every leaf column — the
// historical behavior every existing caller keeps. With a non-nil map, a
// storage column absent from the map (or false) skips state building
// entirely: its only displayable aggregate is the count, which equals the
// group size, so the display layer reads Group.Length() instead of paying a
// per-group state (for strings, a UniqueSet map fed by every member row).
// Computed and joined columns always keep their states — their per-row
// errors make counts genuinely per-column. Callers must include any column
// whose aggregates are displayed beyond count or used as a group-sort key.
func (t *TableView) SetAggregateNeeds(needs map[string]bool) {
	t.aggNeeds = needs
}

// needsAggState reports whether a leaf column's aggregate state must be
// built under the current aggregate needs.
func (t *TableView) needsAggState(col string) bool {
	if t.aggNeeds == nil || t.aggNeeds[col] {
		return true
	}
	// Virtual columns drop rows with per-row errors, so even their count
	// requires the real state. (Detected via the view's registries — the
	// IJoinedDataColumn interface adds no methods, so a type assertion
	// would match every column.)
	if _, isJoined := t.joins[col]; isJoined {
		return true
	}
	_, isComputed := t.computedColumns[col]
	return isComputed
}

// filterAggLeafColumns returns the leaf columns whose aggregate states must
// be computed under the current aggregate needs.
func (t *TableView) filterAggLeafColumns(leafColumns []string) []string {
	if t.aggNeeds == nil {
		return leafColumns
	}
	filtered := make([]string, 0, len(leafColumns))
	for _, col := range leafColumns {
		if t.needsAggState(col) {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

// ApplyFilters builds and caches a filter selection bitmap based on the provided filters
// Each filter is a column name mapped to a filter value
// Filter matching:
//   - If filter value is enclosed in double quotes (e.g., "exact"), performs case-sensitive exact match
//   - Otherwise, performs case-insensitive substring match
//
// # All filters must match (AND logic) for a row to pass
//
// Optimization: Processes each column once, applying filter logic column-by-column
// rather than row-by-row. This minimizes redundant condition checks and improves
// cache locality when accessing column data sequentially.
//
// Caching: Skips recomputation if filters are identical to the previous call.
func (t *TableView) ApplyFilters(filters map[string]string) {
	// context.Background never cancels, so the error is impossible.
	_ = t.ApplyFiltersContext(context.Background(), filters)
}

// ApplyFiltersContext is ApplyFilters under a context: the structured filter
// scans run in parallel on the executor pool and stop at chunk granularity
// once ctx is cancelled (docs/scaling-to-1b-rows.md §3, "everything is
// cancellable"). On cancellation it returns ctx.Err() and caches nothing —
// the view is left as if the filters had never been applied, so a later call
// recomputes them. Filters on columns without structured filter support are
// scanned per row; those scans observe the context only between columns.
func (t *TableView) ApplyFiltersContext(ctx context.Context, filters map[string]string) error {
	t.filtersRecomputed = false
	// Check if filters are unchanged - skip recomputation
	if t.filtersEqual(filters) {
		return nil
	}

	// If no filters, clear the selection
	if len(filters) == 0 {
		t.filterSel = nil
		t.lastFilters = nil
		return nil
	}
	t.filtersRecomputed = true

	// Initialize the selection - start with all rows passing
	t.filterSel = columns.NewSelectionAll(t.baseTable.Length())

	// Apply each filter one column at a time
	for colName, filterValue := range filters {
		if err := ctx.Err(); err != nil {
			t.abandonFilters()
			return err
		}
		col := t.GetColumn(colName)
		if col == nil {
			// Column not found - no rows pass
			t.filterSel = columns.NewSelection(t.baseTable.Length())
			return nil
		}

		// Check for multi-value filter (pipe-separated exact matches)
		if strings.Contains(filterValue, "|") {
			// Multi-value OR filter - match any of the pipe-separated values (exact match)
			values := strings.Split(filterValue, "|")
			// Columns with a structured multi-value filter (the chunked
			// columns) evaluate it themselves, skipping chunks their zone
			// maps rule out. Same matches as the per-row scan below.
			if fc, ok := col.(interface {
				FilterSelectionInContext(context.Context, []string) (*columns.Selection, error)
			}); ok {
				sel, err := fc.FilterSelectionInContext(ctx, values)
				if err != nil {
					t.abandonFilters()
					return err
				}
				t.filterSel.And(sel)
				continue
			}
			if fc, ok := col.(interface {
				FilterSelectionIn([]string) *columns.Selection
			}); ok {
				t.filterSel.And(fc.FilterSelectionIn(values))
				continue
			}
			valueSet := make(map[string]bool, len(values))
			for _, v := range values {
				valueSet[v] = true
			}
			t.filterSel.ForEach(func(i uint32) {
				rowValue, err := col.GetString(i)
				if err != nil || !valueSet[rowValue] {
					t.filterSel.Remove(i)
				}
			})
		} else {
			// Single value filter - determine filter type
			isExactMatch := len(filterValue) >= 2 && filterValue[0] == '"' && filterValue[len(filterValue)-1] == '"'

			if isExactMatch {
				// Exact match (case-sensitive) - strip quotes
				exactValue := filterValue[1 : len(filterValue)-1]
				// Structured equality with chunk pruning, when the column
				// offers it. Same matches as the per-row scan below.
				if fc, ok := col.(interface {
					FilterSelectionEqualContext(context.Context, string) (*columns.Selection, error)
				}); ok {
					sel, err := fc.FilterSelectionEqualContext(ctx, exactValue)
					if err != nil {
						t.abandonFilters()
						return err
					}
					t.filterSel.And(sel)
					continue
				}
				if fc, ok := col.(interface {
					FilterSelectionEqual(string) *columns.Selection
				}); ok {
					t.filterSel.And(fc.FilterSelectionEqual(exactValue))
					continue
				}
				t.filterSel.ForEach(func(i uint32) {
					rowValue, err := col.GetString(i)
					if err != nil || rowValue != exactValue {
						t.filterSel.Remove(i)
					}
				})
			} else {
				// Substring match (case-insensitive)
				substringValue := strings.ToLower(filterValue)
				t.filterSel.ForEach(func(i uint32) {
					rowValue, err := col.GetString(i)
					if err != nil || !strings.Contains(strings.ToLower(rowValue), substringValue) {
						t.filterSel.Remove(i)
					}
				})
			}
		}
	}

	// Save the filters that produced this selection
	t.lastFilters = make(map[string]string, len(filters))
	for k, v := range filters {
		t.lastFilters[k] = v
	}
	return nil
}

// abandonFilters discards a partially built filter selection so a cancelled
// ApplyFiltersContext caches nothing: the next call starts fresh.
func (t *TableView) abandonFilters() {
	t.filterSel = nil
	t.lastFilters = nil
}

// filtersEqual checks if the provided filters match the last applied filters
func (t *TableView) filtersEqual(filters map[string]string) bool {
	if len(filters) != len(t.lastFilters) {
		return false
	}
	for k, v := range filters {
		if t.lastFilters[k] != v {
			return false
		}
	}
	return true
}

// ClearFilters removes the active filter selection
func (t *TableView) ClearFilters() {
	t.filterSel = nil
	t.lastFilters = nil
}

// GetFilteredRowCount returns the number of rows that pass the current filter
// Returns total row count if no filter is active
func (t *TableView) GetFilteredRowCount() int {
	if t.filterSel == nil {
		return t.baseTable.Length()
	}
	return t.filterSel.Count()
}

// rowSet returns the current filter selection as a columns.RowSet without
// materialising indices: the cached bitmap when a filter is active, the full
// row universe otherwise. Every selection-consuming path starts here.
func (t *TableView) rowSet() columns.RowSet {
	if t.filterSel == nil {
		return columns.AllRows(t.baseTable.Length())
	}
	return t.filterSel
}

// GetFilteredIndices returns the indices of rows that pass the current filter
// Returns all indices if no filter is active
//
// Deprecated: the result costs four bytes per passing row, which is what the
// Selection bitmap exists to avoid. Nothing in this repository uses it; it
// remains as an adapter for external callers and will be removed in a future
// major cleanup. Use GetFilteredRowCount, GetFilteredRows or
// GetFilteredRowsSorted instead.
func (t *TableView) GetFilteredIndices() []uint32 {
	if t.filterSel == nil {
		// No filter - return all indices
		indices := make([]uint32, t.baseTable.Length())
		for i := 0; i < t.baseTable.Length(); i++ {
			indices[i] = uint32(i)
		}
		return indices
	}
	return t.filterSel.ToIndices()
}

// GetFilteredRows returns rows as maps of column name to string value
// Returns only rows that pass the current filter, up to the specified limit
// If limit <= 0, returns all filtered rows
func (t *TableView) GetFilteredRows(columnNames []string, limit int) []map[string]string {
	sel := t.rowSet()

	// Determine how many rows to return
	rowCount := sel.NumRows()
	if limit > 0 && limit < rowCount {
		rowCount = limit
	}

	rows := make([]map[string]string, 0, rowCount)
	sel.ForEachRow(func(rowIndex uint32) bool {
		row := make(map[string]string)
		for _, colName := range columnNames {
			col := t.GetColumn(colName)
			if col != nil {
				value, err := col.GetString(rowIndex)
				if err != nil {
					if errors.Is(err, columns.ErrUnmatched) {
						row[colName] = columns.UnmatchedLabel
					} else {
						row[colName] = columns.ErrorLabel
					}
				} else {
					row[colName] = value
				}
			}
		}
		rows = append(rows, row)
		return len(rows) < rowCount
	})
	return rows
}

func (t *TableView) NumRows() int {
	return t.baseTable.Length()
}

func (t *TableView) GetGroupCount(col string) int {
	return t.groupedColumns[col].GetGroupCount()
}

func (t *TableView) ClearGroupings() {
	t.groupedColumns = make(map[string]*grouping.GroupedColumn)
	t.firstBlock = nil
	t.groupingSteps = nil
	// The block registry pins every block (and its groups) it references;
	// without this reset each regrouping on a live view would keep the
	// previous block tree reachable forever.
	t.blocksByColumn = make(map[string][]*grouping.Block)
	t.lastGroupingOrder = nil
	t.lastGroupingFilters = nil
	t.lastGroupingSortAsc = nil
	t.lastExpansion = nil
	t.lastDisplayLimit = 0
	t.lastAggNeeds = nil
	t.lastLevel0AggSort = nil
}

func (t *TableView) GroupTable(groupingOrder []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool) {
	t.GroupTableWithLimit(groupingOrder, aggregatedColumns, compare, asc, 0)
}

// GroupTableWithLimit groups the table with an optional display limit for top-K optimization.
// When displayLimit > 0, only the top K groups are kept after sorting (O(n log k) vs O(n log n)).
// Use displayLimit=0 to sort all groups (original behavior).
//
// The entire tree is built eagerly (every group of every level). To compute
// only opened subtrees, use GroupTableWindowed with an explicit expansion.
func (t *TableView) GroupTableWithLimit(groupingOrder []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool, displayLimit int) {
	t.GroupTableWindowed(groupingOrder, aggregatedColumns, compare, asc, displayLimit, GroupExpansion{ExpandAll: true})
}

// GroupExpansion selects which subtrees of the grouping hierarchy a request
// wants computed. Expansion is a query concept — which parts of the tree to
// build — not a presentation detail.
type GroupExpansion struct {
	// ExpandAll reproduces the historical behavior: every group of every
	// level is built. Paths is ignored when set.
	ExpandAll bool
	// Paths lists the open group paths. Each path names a group by its
	// rendered values from the first grouping level downward (mirroring
	// engine.GroupPath). Opening a nested group implies opening every
	// ancestor on its path.
	Paths [][]string
}

// groupsBuilt counts every grouping.Group created by grouping builds in this
// process. Tests read it to assert how much of the tree a request computed.
var groupsBuilt atomic.Uint64

// GroupTableWindowed groups the table computing only the subtrees selected by
// expansion. Level 0 is always computed (counts, representative rows and
// aggregates are O(distinct)); deeper levels are built only underneath groups
// named in expansion.Paths. When only the expansion differs from the previous
// call, the cached level-0 state is kept and child blocks are built or
// dropped incrementally, without rescanning the table.
func (t *TableView) GroupTableWindowed(groupingOrder []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool, displayLimit int, expansion GroupExpansion) {
	// context.Background is never cancelled, so the error is impossible.
	_ = t.GroupTableWindowedContext(context.Background(), groupingOrder, aggregatedColumns, compare, asc, displayLimit, expansion)
}

// GroupTableWindowedContext is GroupTableWindowed under a context: the
// grouping build runs its level-0 partition as per-chunk partials on the
// shared executor pool and observes ctx at chunk granularity there, at group
// granularity elsewhere. On cancellation it returns ctx's error with every
// grouping state dropped — nothing half-built is cached, and the next request
// regroups from scratch.
func (t *TableView) GroupTableWindowedContext(ctx context.Context, groupingOrder []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool, displayLimit int, expansion GroupExpansion) error {
	// Check if grouping inputs are unchanged - skip recomputation. The display
	// limit is part of the inputs: level-0 groups are top-K-trimmed to it, so
	// state built under a different limit cannot be reused.
	t.groupingSteps = nil
	if t.groupingEqual(groupingOrder, asc) && t.lastDisplayLimit == displayLimit && t.lastExpansion != nil {
		if expansionEqual(*t.lastExpansion, expansion) {
			t.noteStep("cached: grouping inputs unchanged")
			return nil
		}
		if !t.lastExpansion.ExpandAll && !expansion.ExpandAll {
			// Same grouping, different expansion: reuse the level-0 state
			// and adjust only the affected subtrees.
			s := t.stepStart()
			if err := t.updateGroupExpansion(ctx, asc, expansion); err != nil {
				t.dropGroupingState()
				return err
			}
			t.recordStep("expansion update (level 0 reused)", s, t.GetFilteredRowCount(), StepSetting{})
			return nil
		}
		// Switching between expand-all and explicit expansion falls through
		// to a full rebuild.
	}

	var err error
	if expansion.ExpandAll {
		err = t.groupTableEager(ctx, groupingOrder, asc, displayLimit)
	} else {
		err = t.groupTableLazy(ctx, groupingOrder, asc, displayLimit, expansion)
	}
	if err != nil {
		t.dropGroupingState()
		return err
	}
	return nil
}

// dropGroupingState resets every piece of grouping state. A cancelled build
// caches nothing. (ClearGroupings resets the block registry too, so this is
// now a plain alias kept for its call sites' intent.)
func (t *TableView) dropGroupingState() {
	t.ClearGroupings()
}

// groupTableEager builds the full grouping tree: every group of every level.
// This is the historical behavior and the byte-identical default.
func (t *TableView) groupTableEager(ctx context.Context, groupingOrder []string, asc map[string]bool, displayLimit int) error {
	// clear current groups (including the block registry — a regroup must
	// not keep the previous block tree reachable)
	t.groupedColumns = make(map[string]*grouping.GroupedColumn)
	t.firstBlock = nil
	t.blocksByColumn = make(map[string][]*grouping.Block)

	// Group directly from the cached filter selection; the bitmap is never
	// materialised as an index list.
	t.groupingOrder = groupingOrder
	sel := t.rowSet()

	// Process first column
	// groupedTable.columns = columns
	step := t.stepStart()
	parentBlocks, err := t.groupFirstColumnInTable(ctx, sel)
	if err != nil {
		return err
	}
	t.firstBlock = parentBlocks[0]
	t.recordStep(fmt.Sprintf("partition by %s: %s groups", groupingOrder[0], formatGroupCount(len(t.firstBlock.Groups))), step, t.GetFilteredRowCount(), groupSetting(groupingOrder[0]))

	// An aggregate sort on the level-0 column must rank ALL groups, but
	// only the displayed top-K groups need their child subtrees: compute
	// level-0 aggregates, sort by them, then restrict the child build to
	// the top of the ranking. Without this, a deep grouping under an
	// aggregate sort builds subtrees for every level-0 group (10'000
	// entity subtrees to display 25 — seconds of wasted build).
	childParents := parentBlocks
	if s := t.level0AggSort; s != nil && s.AggType != queryspec.AggSubgroupCount {
		// (the level-0 aggregates it needs record their own steps)
		if err := t.computeLevelZeroAggregates(ctx); err != nil {
			return err
		}
		step = t.stepStart()
		t.sortBlockByAggregate(t.firstBlock, map[string]*queryspec.GroupAggSort{groupingOrder[0]: s})
		t.recordStep(fmt.Sprintf("rank all %d level-0 groups by %s(%s)", len(t.firstBlock.Groups), s.AggType, s.LeafColumn), step, 0, aggSortSetting(groupingOrder[0]))
		if displayLimit > 0 && displayLimit < len(t.firstBlock.Groups) {
			// Groups outside the display window stay in the block (the
			// ranking and totals are complete) but become final leaves:
			// membership released now, aggregates already computed, so
			// the walk below skips them.
			for _, g := range t.firstBlock.Groups[displayLimit:] {
				g.Indices = nil
			}
			top := *t.firstBlock
			top.Groups = t.firstBlock.Groups[:displayLimit]
			childParents = []*grouping.Block{&top}
		}
	} else {
		// Sort first column groups with top-K optimization
		firstColumn := groupingOrder[0]
		ascending, hasSort := asc[firstColumn]
		descending := hasSort && !ascending // default to ascending if not specified
		step = t.stepStart()
		t.sortGroupsInBlockTopK(t.firstBlock, descending, displayLimit)
		t.recordStep(fmt.Sprintf("sort level 0 by value%s", topKNote(displayLimit)), step, 0, groupSetting(groupingOrder[0]))
	}

	// Process subsequent columns
	if err := t.groupSubsequentColumnsInTable(ctx, t.groupingOrder[1:], childParents, asc); err != nil {
		return err
	}

	// Compute aggregates for all groups
	leafColumns := t.GetLeafColumns()
	columnTypes := make(map[string]queryspec.ColumnType)
	for _, colName := range leafColumns {
		columnTypes[colName] = t.GetColumnType(colName)
	}
	if err := t.computeAggregates(ctx, leafColumns, columnTypes); err != nil {
		return err
	}

	// Membership lists were only needed to build child levels and leaf
	// aggregates; drop them so retained grouping state is O(distinct), not
	// O(rows).
	step = t.stepStart()
	releaseGroupMembership(t.firstBlock)
	// The eager tree is final — no incremental re-merge will ever need the
	// unique sets, so collapse them into counts: retained grouping state
	// must not pin every member string of a unique-aggregated column
	// (~1 GB per grouping measured on a 10M-row key column). The lazy path
	// keeps its sets: incremental expansion re-combines parent states from
	// children, and its trees are bounded by what is expanded.
	t.compactUniqueSets(t.firstBlock)
	t.recordStep("release membership, compact unique sets", step, 0, StepSetting{})

	t.saveGroupingState(groupingOrder, asc, displayLimit)
	t.lastExpansion = &GroupExpansion{ExpandAll: true}
	return nil
}

// formatGroupCount renders a group count for step names.
func formatGroupCount(n int) string { return fmt.Sprintf("%d", n) }

// topKNote describes the level-0 display trim for step names.
func topKNote(displayLimit int) string {
	if displayLimit > 0 {
		return fmt.Sprintf(", keep top %d", displayLimit)
	}
	return ""
}

// groupTableLazy builds level 0 in full (O(distinct) retained state) and
// deeper levels only underneath groups opened by expansion.
func (t *TableView) groupTableLazy(ctx context.Context, groupingOrder []string, asc map[string]bool, displayLimit int, expansion GroupExpansion) error {
	t.groupedColumns = make(map[string]*grouping.GroupedColumn)
	t.firstBlock = nil
	t.blocksByColumn = make(map[string][]*grouping.Block)

	t.groupingOrder = groupingOrder

	step := t.stepStart()
	parentBlocks, err := t.groupFirstColumnInTable(ctx, t.rowSet())
	if err != nil {
		return err
	}
	t.firstBlock = parentBlocks[0]
	t.recordStep(fmt.Sprintf("partition by %s: %s groups", groupingOrder[0], formatGroupCount(len(t.firstBlock.Groups))), step, t.GetFilteredRowCount(), groupSetting(groupingOrder[0]))

	firstColumn := groupingOrder[0]
	ascending, hasSort := asc[firstColumn]
	descending := hasSort && !ascending // default to ascending if not specified
	step = t.stepStart()
	if t.level0AggSort != nil {
		// An aggregate sort ranks level 0 after the build; a value trim
		// here would pre-select the wrong groups (candidate-4 bug shape).
		t.sortGroupsInBlockTopK(t.firstBlock, descending, 0)
		t.recordStep("sort level 0 by value (no trim: aggregate sort ranks after the build)", step, 0, groupSetting(groupingOrder[0]))
	} else {
		t.sortGroupsInBlockTopK(t.firstBlock, descending, displayLimit)
		t.recordStep(fmt.Sprintf("sort level 0 by value%s", topKNote(displayLimit)), step, 0, groupSetting(groupingOrder[0]))
	}

	// Register a GroupedColumn for every deeper level up front so group
	// counts and stats resolve even when no subtree at that level is open.
	for level, col := range groupingOrder[1:] {
		t.groupedColumns[col] = &grouping.GroupedColumn{
			DataColumn: t.GetColumn(col),
			ColumnView: t.columnViews[col],
			Level:      level + 1,
			Tag:        "next",
		}
	}

	expanded := normalizeExpansion(expansion.Paths)
	step = t.stepStart()
	if err := t.buildExpandedChildren(ctx, t.firstBlock, 0, nil, expanded, asc); err != nil {
		return err
	}
	t.recordStep(fmt.Sprintf("build %d expanded subtrees", len(expansion.Paths)), step, t.GetFilteredRowCount(), StepSetting{})

	leafColumns := t.GetLeafColumns()
	columnTypes := make(map[string]queryspec.ColumnType)
	for _, colName := range leafColumns {
		columnTypes[colName] = t.GetColumnType(colName)
	}
	if err := t.computeAggregates(ctx, leafColumns, columnTypes); err != nil {
		return err
	}

	step = t.stepStart()
	releaseGroupMembership(t.firstBlock)
	t.recordStep("release membership", step, 0, StepSetting{})

	t.saveGroupingState(groupingOrder, asc, displayLimit)
	exp := expansion
	t.lastExpansion = &exp
	return nil
}

// updateGroupExpansion adjusts an existing lazy grouping to a new expansion
// state: newly opened subtrees are built (membership re-resolved through the
// column's GroupMembers operation), closed ones are dropped. The cached
// level-0 state — counts, representative rows, aggregates, sort order — is
// reused untouched.
func (t *TableView) updateGroupExpansion(ctx context.Context, asc map[string]bool, expansion GroupExpansion) error {
	expanded := normalizeExpansion(expansion.Paths)
	if err := t.syncExpansion(ctx, t.firstBlock, 0, nil, expanded, asc); err != nil {
		return err
	}
	t.rebuildBlockRegistry()

	leafColumns := t.GetLeafColumns()
	columnTypes := make(map[string]queryspec.ColumnType)
	for _, colName := range leafColumns {
		columnTypes[colName] = t.GetColumnType(colName)
	}
	if err := t.computeAggregates(ctx, leafColumns, columnTypes); err != nil {
		return err
	}

	releaseGroupMembership(t.firstBlock)

	exp := expansion
	t.lastExpansion = &exp
	return nil
}

// buildExpandedChildren descends from block into every group opened by
// expanded, building child blocks from the still-transient membership lists
// of the initial build.
func (t *TableView) buildExpandedChildren(ctx context.Context, block *grouping.Block, level int, prefix []string, expanded map[string]bool, asc map[string]bool) error {
	if block == nil || level+1 >= len(t.groupingOrder) {
		return nil
	}
	for _, g := range block.Groups {
		path := appendPath(prefix, g.GetValue())
		if !expanded[expansionKey(path)] {
			continue
		}
		child, err := t.buildChildBlock(ctx, g, level+1, g.Indices, asc)
		if err != nil {
			return err
		}
		if err := t.buildExpandedChildren(ctx, child, level+1, path, expanded, asc); err != nil {
			return err
		}
	}
	return nil
}

// syncExpansion walks an existing tree and reconciles it with the requested
// expansion: builds missing child blocks, drops no-longer-open ones.
func (t *TableView) syncExpansion(ctx context.Context, block *grouping.Block, level int, prefix []string, expanded map[string]bool, asc map[string]bool) error {
	if block == nil {
		return nil
	}
	lastLevel := level+1 >= len(t.groupingOrder)
	for _, g := range block.Groups {
		path := appendPath(prefix, g.GetValue())
		want := !lastLevel && expanded[expansionKey(path)]
		if want && g.ChildBlock == nil {
			if _, err := t.buildChildBlock(ctx, g, level+1, t.membersForGroup(g), asc); err != nil {
				return err
			}
		} else if !want && g.ChildBlock != nil {
			g.ChildBlock = nil
		}
		if err := t.syncExpansion(ctx, g.ChildBlock, level+1, path, expanded, asc); err != nil {
			return err
		}
	}
	return nil
}

// buildChildBlock groups members (the parent group's rows) by the column at
// the given level, attaches the resulting block to the parent group, and
// sorts its groups by value.
func (t *TableView) buildChildBlock(ctx context.Context, parentGroup *grouping.Group, level int, members []uint32, asc map[string]bool) (*grouping.Block, error) {
	col := t.groupingOrder[level]
	gcol := t.groupedColumns[col]
	b := &grouping.Block{
		ParentGroup:   parentGroup,
		GroupedColumn: gcol,
	}
	gcol.Blocks = append(gcol.Blocks, b)
	t.blocksByColumn[col] = append(t.blocksByColumn[col], b)
	parentGroup.ChildBlock = b

	if err := buildGroupsForBlock(ctx, gcol.DataColumn, gcol.ColumnView, columns.RowIndices(members), b, parentGroup); err != nil {
		return nil, err
	}

	ascending, hasSort := asc[col]
	t.sortGroupsInBlock(b, hasSort && !ascending)
	return b, nil
}

// membersForGroup re-resolves a group's membership after the transient build
// lists have been released, by chaining the column's GroupMembers operation
// from the filtered selection down the group's ancestry. Cost is O(selection)
// per ancestry level — a scan, not a rebuild of the grouping state.
func (t *TableView) membersForGroup(g *grouping.Group) []uint32 {
	if g.Indices != nil {
		return g.Indices
	}
	var sel columns.RowSet
	if g.ParentGroup == nil {
		sel = t.rowSet()
	} else {
		sel = columns.RowIndices(t.membersForGroup(g.ParentGroup))
	}
	gcol := g.Block.GroupedColumn
	ops := columns.GroupOpsFor(gcol.DataColumn, gcol.ColumnView)
	return ops.GroupMembers(sel, g.GroupKey, 0, int(g.Count))
}

// rebuildBlockRegistry rebuilds the per-column block lists from the tree.
// Incremental expansion changes mutate the tree in place; rebuilding the
// registry afterwards keeps GetGroupCount and blocksByColumn consistent.
func (t *TableView) rebuildBlockRegistry() {
	t.blocksByColumn = make(map[string][]*grouping.Block)
	for _, col := range t.groupingOrder {
		if gc := t.groupedColumns[col]; gc != nil {
			gc.Blocks = nil
		}
	}
	var walk func(b *grouping.Block)
	walk = func(b *grouping.Block) {
		if b == nil {
			return
		}
		gc := b.GroupedColumn
		gc.Blocks = append(gc.Blocks, b)
		col := t.groupingOrder[gc.Level]
		t.blocksByColumn[col] = append(t.blocksByColumn[col], b)
		for _, g := range b.Groups {
			walk(g.ChildBlock)
		}
	}
	walk(t.firstBlock)
}

// saveGroupingState records the inputs that produced the current grouping so
// unchanged requests can skip recomputation.
func (t *TableView) saveGroupingState(groupingOrder []string, asc map[string]bool, displayLimit int) {
	t.lastDisplayLimit = displayLimit
	if t.aggNeeds == nil {
		t.lastAggNeeds = nil
	} else {
		t.lastAggNeeds = make(map[string]bool, len(t.aggNeeds))
		for k, v := range t.aggNeeds {
			t.lastAggNeeds[k] = v
		}
	}
	if t.level0AggSort == nil {
		t.lastLevel0AggSort = nil
	} else {
		s := *t.level0AggSort
		t.lastLevel0AggSort = &s
	}
	t.lastGroupingOrder = make([]string, len(groupingOrder))
	copy(t.lastGroupingOrder, groupingOrder)
	t.lastGroupingFilters = make(map[string]string, len(t.lastFilters))
	for k, v := range t.lastFilters {
		t.lastGroupingFilters[k] = v
	}
	t.lastGroupingSortAsc = make(map[string]bool, len(asc))
	for k, v := range asc {
		t.lastGroupingSortAsc[k] = v
	}
}

// expansionKey joins path components with an unprintable separator so group
// values containing "/" or "," cannot collide.
func expansionKey(path []string) string {
	return strings.Join(path, "\x1f")
}

// appendPath returns prefix + value as a fresh slice (no aliasing).
func appendPath(prefix []string, value string) []string {
	path := make([]string, 0, len(prefix)+1)
	path = append(path, prefix...)
	return append(path, value)
}

// normalizeExpansion expands the path list into a prefix-closed set: opening
// a nested group implies opening every ancestor on its path.
func normalizeExpansion(paths [][]string) map[string]bool {
	set := make(map[string]bool)
	for _, p := range paths {
		for i := 1; i <= len(p); i++ {
			set[expansionKey(p[:i])] = true
		}
	}
	return set
}

// expansionEqual reports whether two expansion states select the same
// subtrees.
func expansionEqual(a, b GroupExpansion) bool {
	if a.ExpandAll || b.ExpandAll {
		return a.ExpandAll == b.ExpandAll
	}
	sa, sb := normalizeExpansion(a.Paths), normalizeExpansion(b.Paths)
	if len(sa) != len(sb) {
		return false
	}
	for k := range sa {
		if !sb[k] {
			return false
		}
	}
	return true
}

// sortGroupsInBlock sorts the groups within a block based on their values
func (t *TableView) sortGroupsInBlock(block *grouping.Block, descending bool) {
	t.sortGroupsInBlockTopK(block, descending, 0) // 0 = no limit, sort all
}

// sortGroupsInBlockTopK sorts groups using top-K selection when limit > 0.
// Uses heap-based selection for O(n log k) instead of O(n log n) when k << n.
// If limit is 0 or >= len(groups), sorts all groups.
func (t *TableView) sortGroupsInBlockTopK(block *grouping.Block, descending bool, limit int) {
	if block == nil || len(block.Groups) <= 1 {
		return
	}

	col := block.GroupedColumn.DataColumn
	groups := block.Groups

	// If no limit or limit >= total groups, use standard sort
	if limit <= 0 || limit >= len(groups) {
		sort.Slice(groups, func(i, j int) bool {
			idxI := groups[i].First
			idxJ := groups[j].First
			cmp := columns.CompareAtIndex(col, idxI, idxJ)
			if descending {
				return cmp > 0
			}
			return cmp < 0
		})
		return
	}

	// Use heap-based top-K selection
	h := &groupHeap{
		groups:     make([]*grouping.Group, 0, limit),
		col:        col,
		descending: descending,
	}

	// Initialize heap with first K groups
	for i := 0; i < limit; i++ {
		h.groups = append(h.groups, groups[i])
	}
	heap.Init(h)

	// Process remaining groups
	for i := limit; i < len(groups); i++ {
		group := groups[i]
		// Compare with heap top (the "worst" of current K best)
		idxNew := group.First
		idxTop := h.groups[0].First
		cmp := columns.CompareAtIndex(col, idxNew, idxTop)

		// For ascending: we want smallest, so heap top is largest of K smallest
		// For descending: we want largest, so heap top is smallest of K largest
		isBetter := (cmp < 0 && !descending) || (cmp > 0 && descending)
		if isBetter {
			heap.Pop(h)
			heap.Push(h, group)
		}
	}

	// Extract top K and sort them
	topK := h.groups
	sort.Slice(topK, func(i, j int) bool {
		idxI := topK[i].First
		idxJ := topK[j].First
		cmp := columns.CompareAtIndex(col, idxI, idxJ)
		if descending {
			return cmp > 0
		}
		return cmp < 0
	})

	// Replace block groups with sorted top K
	block.Groups = topK
}

// groupingEqual checks if the grouping inputs match the last computed grouping
func (t *TableView) groupingEqual(groupingOrder []string, asc map[string]bool) bool {
	// Check grouping order
	if len(groupingOrder) != len(t.lastGroupingOrder) {
		return false
	}
	for i, col := range groupingOrder {
		if t.lastGroupingOrder[i] != col {
			return false
		}
	}
	// Check if filter state matches what was used for grouping
	if len(t.lastFilters) != len(t.lastGroupingFilters) {
		return false
	}
	for k, v := range t.lastFilters {
		if t.lastGroupingFilters[k] != v {
			return false
		}
	}
	// Check if sort direction matches what was used for grouping
	if len(asc) != len(t.lastGroupingSortAsc) {
		return false
	}
	for k, v := range asc {
		if t.lastGroupingSortAsc[k] != v {
			return false
		}
	}
	// Check if aggregate needs match what was used for grouping — the
	// grouping build materializes exactly the needed aggregate states.
	if (t.aggNeeds == nil) != (t.lastAggNeeds == nil) || len(t.aggNeeds) != len(t.lastAggNeeds) {
		return false
	}
	for k, v := range t.aggNeeds {
		if t.lastAggNeeds[k] != v {
			return false
		}
	}
	// Check if the in-build level-0 aggregate ranking matches.
	if (t.level0AggSort == nil) != (t.lastLevel0AggSort == nil) {
		return false
	}
	if t.level0AggSort != nil && *t.level0AggSort != *t.lastLevel0AggSort {
		return false
	}
	return true
}

// buildGroupsForBlock partitions sel with the column's grouping operations —
// per-chunk partials on the executor pool where the column supports it, the
// sequential columns.IGroupOps passes otherwise — and creates one Group per
// non-empty code, in code order. Count and First are final; Indices holds the
// group's members only transiently — sliced from a single backing array —
// until releaseGroupMembership drops them at the end of the grouping build.
func buildGroupsForBlock(ctx context.Context, dataColumn columns.IDataColumn, columnView *columns.ColumnView, sel columns.RowSet, block *grouping.Block, parent *grouping.Group) error {
	part, err := columns.PartitionGroups(ctx, dataColumn, columnView, sel)
	if err != nil {
		return err
	}

	created := uint64(0)
	for code, n := range part.Counts {
		if n == 0 {
			continue
		}
		start := part.Offsets[code]
		block.Groups = append(block.Groups, &grouping.Group{
			GroupKey:    uint32(code),
			Indices:     part.Backing[start : start+n : start+n],
			Count:       n,
			First:       part.Firsts[code],
			ParentGroup: parent,
			Block:       block,
			IsComplete:  true,
		})
		created++
	}
	groupsBuilt.Add(created)
	return nil
}

// releaseGroupMembership drops the transient membership lists of every group
// reachable from block. After this the grouping state is O(distinct): counts,
// representative rows and aggregates survive; full row lists do not.
func releaseGroupMembership(block *grouping.Block) {
	if block == nil {
		return
	}
	for _, group := range block.Groups {
		group.Indices = nil
		releaseGroupMembership(group.ChildBlock)
	}
}

func (t *TableView) groupFirstColumnInTable(ctx context.Context, sel columns.RowSet) ([]*grouping.Block, error) {
	firstColumn := t.groupingOrder[0]
	columnView := t.columnViews[firstColumn]
	dataColumn := t.GetColumn(firstColumn)

	g := &grouping.GroupedColumn{
		DataColumn: dataColumn,
		ColumnView: columnView,
		Level:      0,
		Tag:        "first",
	}

	t.groupedColumns[firstColumn] = g

	b := &grouping.Block{
		Groups:        nil,
		ParentGroup:   nil,
		GroupedColumn: g,
	}
	g.Blocks = append(g.Blocks, b)
	t.blocksByColumn[firstColumn] = append(t.blocksByColumn[firstColumn], b)

	if err := buildGroupsForBlock(ctx, dataColumn, columnView, sel, b, nil); err != nil {
		return nil, err
	}

	return []*grouping.Block{b}, nil
}

func (t *TableView) groupSubsequentColumnsInTable(ctx context.Context, cols []string, parentBlocks []*grouping.Block, asc map[string]bool) error {
	if len(cols) == 0 {
		return nil
	}

	// for following columns, each parent group spawns a child block
	for level, col := range cols {
		step := t.stepStart()
		parentRows := 0
		dataColumn := t.GetColumn(col)
		columnView := t.columnViews[col]

		g := &grouping.GroupedColumn{
			DataColumn: dataColumn,
			ColumnView: columnView,
			Level:      level + 1,
			Tag:        "next",
		}

		t.groupedColumns[col] = g

		// Determine sort direction for this column
		ascending, hasSort := asc[col]
		descending := hasSort && !ascending // default to ascending if not specified

		// every parent group spawns a block
		for _, parentBlock := range parentBlocks {
			for _, parentGroup := range parentBlock.Groups {
				parentRows += parentGroup.Length()
				b := &grouping.Block{
					ParentGroup:   parentGroup,
					GroupedColumn: g,
				}
				g.Blocks = append(g.Blocks, b)
				t.blocksByColumn[col] = append(t.blocksByColumn[col], b)

				// Link the parent group to this child block
				parentGroup.ChildBlock = b

				// now group within the parent group
				if err := buildGroupsForBlock(ctx, dataColumn, columnView, columns.RowIndices(parentGroup.Indices), b, parentGroup); err != nil {
					return err
				}

				// Sort groups within this block
				t.sortGroupsInBlock(b, descending)
			}
		}
		parentBlocks = g.Blocks
		groups := 0
		for _, b := range g.Blocks {
			groups += len(b.Groups)
		}
		t.recordStep(fmt.Sprintf("level %d by %s: %d blocks, %d groups", level+1, col, len(g.Blocks), groups), step, parentRows, groupSetting(col))
	}
	return nil
}

// NewTableView creates a new TableView wrapping a DataTable
func NewTableView(baseTable *DataTable, tableName string) *TableView {
	return &TableView{
		baseTable:       baseTable,
		tableName:       tableName,
		joins:           make(map[string]columns.IJoinedDataColumn),
		computedColumns: make(map[string]columns.IDataColumn),
		columnViews:     make(map[string]*columns.ColumnView),
		groupedColumns:  make(map[string]*grouping.GroupedColumn),
		blocksByColumn:  make(map[string][]*grouping.Block),
	}
}

// AddComputedColumn adds a computed column to the view
func (tv *TableView) AddComputedColumn(name string, col columns.IDataColumn) {
	tv.computedColumns[name] = col
	// Also add a column view so the column can be used for grouping
	tv.columnViews[name] = &columns.ColumnView{}
}

// RemoveComputedColumn removes a computed column from the view
func (tv *TableView) RemoveComputedColumn(name string) {
	delete(tv.computedColumns, name)
	delete(tv.columnViews, name)
}

// GetBaseTable returns the underlying immutable DataTable
func (tv *TableView) GetBaseTable() *DataTable {
	return tv.baseTable
}

// AddJoinedColumn adds a joined column to this view
func (tv *TableView) AddJoinedColumn(joinedCol columns.IJoinedDataColumn) {
	tv.joins[joinedCol.ColumnDef().Name()] = joinedCol
}

// RemoveJoinedColumn removes a joined column from this view
func (tv *TableView) RemoveJoinedColumn(name string) {
	delete(tv.joins, name)
}

// GetColumn retrieves a column by name, checking base table, joined columns, and computed columns
func (tv *TableView) GetColumn(name string) columns.IDataColumn {
	// First check base table columns
	if col := tv.baseTable.GetColumn(name); col != nil {
		return col
	}
	// Then check view's joined columns
	if col, ok := tv.joins[name]; ok {
		return col
	}
	// Finally check computed columns
	if col, ok := tv.computedColumns[name]; ok {
		return col
	}
	return nil
}

// GetColumnNames returns column names from the base table only
func (tv *TableView) GetColumnNames() []string {
	return tv.baseTable.GetColumnNames()
}

// GetAllColumnNames returns all column names including joined columns in this view
func (tv *TableView) GetAllColumnNames() []string {
	names := make([]string, 0, len(tv.baseTable.columns)+len(tv.joins))

	// Add regular columns from base table
	for name := range tv.baseTable.columns {
		names = append(names, name)
	}

	// Add joined columns from this view
	for name := range tv.joins {
		names = append(names, name)
	}

	return names
}

// GetJoinedColumnNames returns only joined column names in this view
func (tv *TableView) GetJoinedColumnNames() []string {
	names := make([]string, 0, len(tv.joins))
	for name := range tv.joins {
		names = append(names, name)
	}
	return names
}

// UpdateJoinedColumns updates the joined columns in this view to match the requested columns
// It adds new joined columns and removes ones that are no longer needed
// Joined columns are identified by the format:
// - Single hop: fromColumn.toTable.toColumn.selectedColumn (4 parts)
// - Multi hop: fromColumn.toTable.toColumn.fromColumn2.toTable2.toColumn2.selectedColumn (7 parts for 2 hops, etc.)
func (tv *TableView) UpdateJoinedColumns(columnNames []string, resolver JoinResolver) {
	// Debug: Print processing info
	fmt.Printf("\n=== UpdateJoinedColumns Debug Info ===\n")
	fmt.Printf("Table: %s\n", tv.tableName)
	fmt.Printf("Columns to process: %v\n", columnNames)

	// Track which joined columns we need
	neededJoinedColumns := make(map[string]bool)

	// Parse columns to identify joined ones (with dots)
	for _, colName := range columnNames {
		if strings.Contains(colName, ".") {
			// This is a joined column
			// Valid formats: 4 parts (1 hop), 7 parts (2 hops), 10 parts (3 hops), etc.
			// Pattern: 4 + 3*(n-1) = 3n + 1 parts for n hops
			parts := strings.Split(colName, ".")
			numParts := len(parts)
			// Check if it's a valid join path: (numParts - 1) must be divisible by 3
			if numParts >= 4 && (numParts-1)%3 == 0 {
				neededJoinedColumns[colName] = true
			}
		}
	}

	// Remove joined columns that are no longer needed
	currentJoinedColumns := tv.GetJoinedColumnNames()
	for _, colName := range currentJoinedColumns {
		if !neededJoinedColumns[colName] {
			fmt.Printf("Removing joined column %s from table view\n", colName)
			tv.RemoveJoinedColumn(colName)
		}
	}

	// Add needed joined columns that aren't already in the view
	for colName := range neededJoinedColumns {
		// Skip if already exists
		if tv.joins[colName] != nil {
			continue
		}

		joinedColumn := tv.createChainedJoinedColumn(colName, resolver)
		if joinedColumn != nil {
			fmt.Printf("Adding joined column %s to table view\n", colName)
			tv.AddJoinedColumn(joinedColumn)
		}
	}

	// Debug: Print final state
	fmt.Printf("Joined Columns in TableView: %v\n", tv.GetJoinedColumnNames())
	fmt.Printf("All Columns in TableView: %v\n", tv.GetAllColumnNames())
	fmt.Printf("===============================================\n\n")
}

// createChainedJoinedColumn creates a joined column that may chain through multiple tables
// Format: fromColumn.toTable.toColumn.fromColumn2.toTable2.toColumn2...selectedColumn
func (tv *TableView) createChainedJoinedColumn(colName string, resolver JoinResolver) columns.IJoinedDataColumn {
	parts := strings.Split(colName, ".")
	numParts := len(parts)

	// Calculate number of hops: (numParts - 1) / 3
	numHops := (numParts - 1) / 3
	if numHops < 1 {
		return nil
	}

	type JoinWithJoiner interface {
		GetJoiner() columns.IJoiner
	}

	// Collect all joiners for the chain
	joiners := make([]columns.IJoiner, 0, numHops)
	currentTableName := tv.tableName
	var lastTargetTable string

	// Process each hop to collect joiners
	for hop := 0; hop < numHops; hop++ {
		// Calculate indices for this hop
		// Hop 0: parts[0]=fromCol, parts[1]=toTable, parts[2]=toCol
		// Hop 1: parts[3]=fromCol, parts[4]=toTable, parts[5]=toCol
		baseIdx := hop * 3
		fromColumn := parts[baseIdx]
		toTable := parts[baseIdx+1]
		toColumn := parts[baseIdx+2]

		// Build join key for this hop
		joinKey := fmt.Sprintf("%s.%s->%s.%s", currentTableName, fromColumn, toTable, toColumn)
		foundJoin := resolver.GetJoin(joinKey)

		if foundJoin == nil {
			fmt.Printf("Could not find join for key: %s\n", joinKey)
			return nil
		}

		joinWithJoiner, ok := foundJoin.(JoinWithJoiner)
		if !ok {
			fmt.Printf("Join does not have GetJoiner method: %s\n", joinKey)
			return nil
		}

		joiner := joinWithJoiner.GetJoiner()
		if joiner == nil {
			fmt.Printf("Join has nil joiner: %s\n", joinKey)
			return nil
		}
		joiners = append(joiners, joiner)
		lastTargetTable = toTable
		currentTableName = toTable
	}

	// Get the final target column (last part of the path)
	selectedColName := parts[numParts-1]
	targetTable := resolver.GetTable(lastTargetTable)
	if targetTable == nil {
		fmt.Printf("Could not find target table: %s\n", lastTargetTable)
		return nil
	}

	targetDataCol := targetTable.GetColumn(selectedColName)
	if targetDataCol == nil {
		fmt.Printf("Could not find target column: %s.%s\n", lastTargetTable, selectedColName)
		return nil
	}

	// Create the final joined column with either a single joiner or a chained joiner
	colDef := columns.NewColumnDef(
		colName,
		fmt.Sprintf("%s → %s", lastTargetTable, targetDataCol.ColumnDef().DisplayName()),
		"",
	)

	var joiner columns.IJoiner
	if len(joiners) == 1 {
		joiner = joiners[0]
	} else {
		joiner = columns.NewChainedJoiner(joiners...)
	}

	return targetDataCol.CreateJoinedColumn(colDef, joiner)
}

// IsGrouped returns true if the table has active grouping
func (tv *TableView) IsGrouped() bool {
	return len(tv.groupedColumns) > 0
	//	return tv.firstBlock != nil
}

func (tv *TableView) IsColGrouped(colName string) bool {
	_, ok := tv.groupedColumns[colName]
	return ok
}

// GetFirstBlock returns the first block of the grouping hierarchy
// Returns nil if no grouping is active
func (tv *TableView) GetFirstBlock() *grouping.Block {
	return tv.firstBlock
}

// GetGroupingOrder returns the ordered list of grouped column names
func (tv *TableView) GetGroupingOrder() []string {
	return tv.groupingOrder
}

// GetLeafColumns returns the names of the non-grouped columns (leaf columns)
// These are columns that are not grouped - both filtered and others
// With filters, the visible columns order is: filtered → grouped → others
// Leaf columns include both filtered (displayed before grouped) and others (displayed after grouped)
// This maintains the display order: filtered leaves, grouped columns, other leaves
func (tv *TableView) GetLeafColumns() []string {
	var leafColumns []string
	for _, colName := range tv.VisibleColumns {
		if !tv.IsColGrouped(colName) {
			leafColumns = append(leafColumns, colName)
		}
	}
	return leafColumns
}

// IsColFiltered checks if a column has an active filter
func (tv *TableView) IsColFiltered(colName string) bool {
	if tv.lastFilters == nil {
		return false
	}
	_, hasFilter := tv.lastFilters[colName]
	return hasFilter
}

// GetFilteredLeafColumns returns the names of leaf columns that have active filters
// These are non-grouped columns with active filters
func (tv *TableView) GetFilteredLeafColumns() []string {
	var filteredLeafColumns []string
	for _, colName := range tv.VisibleColumns {
		if !tv.IsColGrouped(colName) && tv.IsColFiltered(colName) {
			filteredLeafColumns = append(filteredLeafColumns, colName)
		}
	}
	return filteredLeafColumns
}

// GetOtherLeafColumns returns the names of leaf columns that are not grouped
// (regardless of filter state - this returns all non-grouped columns except filtered ones)
func (tv *TableView) GetOtherLeafColumns() []string {
	var otherLeafColumns []string
	for _, colName := range tv.VisibleColumns {
		if !tv.IsColGrouped(colName) && !tv.IsColFiltered(colName) {
			otherLeafColumns = append(otherLeafColumns, colName)
		}
	}
	return otherLeafColumns
}

// ComputeAggregates computes aggregates for all groups in the hierarchy.
// It uses bottom-up aggregation: leaf groups compute from data, parent groups combine children.
// leafColumns specifies which columns to aggregate; columnTypes maps column names to types.
func (tv *TableView) ComputeAggregates(leafColumns []string, columnTypes map[string]queryspec.ColumnType) {
	// context.Background is never cancelled, so the error is impossible.
	_ = tv.computeAggregates(context.Background(), leafColumns, columnTypes)
}

// computeAggregates is ComputeAggregates under a context, observed at group
// granularity: the per-row aggregate work of one group runs uninterrupted.
func (tv *TableView) computeAggregates(ctx context.Context, leafColumns []string, columnTypes map[string]queryspec.ColumnType) error {
	leafColumns = tv.withAggSortColumn(tv.filterAggLeafColumns(leafColumns))
	if tv.firstBlock == nil || len(leafColumns) == 0 {
		tv.noteStep("aggregates: none needed (count-only columns read the group size)")
		return nil
	}
	for _, colName := range leafColumns {
		if _, ok := columnTypes[colName]; !ok {
			columnTypes[colName] = tv.GetColumnType(colName)
		}
	}

	// Level-0 leaf aggregates merge per-chunk pre-aggregated partials where
	// the grouping column supports it; columns absent from bulk take the
	// per-row path below.
	bulk, err := tv.bulkLevel0Aggregates(ctx, leafColumns, columnTypes)
	if err != nil {
		return err
	}

	// Columns whose level-0 state is already there (merged partials above,
	// or the ranking pass) only pay the per-row pass on deeper levels; the
	// others pay it on every level.
	var allLevels, deeperOnly []string
	for _, colName := range leafColumns {
		_, viaBulk := bulk[colName]
		have := len(tv.firstBlock.Groups) > 0 && tv.firstBlock.Groups[0].Aggregates != nil && tv.firstBlock.Groups[0].Aggregates[colName] != nil
		if viaBulk || have {
			deeperOnly = append(deeperOnly, colName)
		} else {
			allLevels = append(allLevels, colName)
		}
	}

	// Walk the hierarchy bottom-up, starting from leaves
	step := tv.stepStart()
	err = tv.computeAggregatesForBlock(ctx, tv.firstBlock, leafColumns, columnTypes, bulk)
	// Rows scanned: one pass over the selection per column and level.
	sel, levels := tv.GetFilteredRowCount(), len(tv.groupingOrder)
	var parts []string
	rows := 0
	if len(allLevels) > 0 {
		parts = append(parts, strings.Join(allLevels, ", ")+" (all levels)")
		rows += sel * len(allLevels) * levels
	}
	if len(deeperOnly) > 0 && levels > 1 {
		parts = append(parts, strings.Join(deeperOnly, ", ")+" (deeper levels)")
		rows += sel * len(deeperOnly) * (levels - 1)
	}
	if len(parts) > 0 {
		tv.recordStep("aggregates per row: "+strings.Join(parts, "; "), step, rows, aggregateSetting(append(allLevels, deeperOnly...)...))
	} else {
		tv.recordStep("assemble level-0 states from partials", step, 0, aggregateSetting(leafColumns...))
	}
	return err
}

// computeAggregatesForBlock recursively computes aggregates for a block and its children.
// Returns after processing all groups in the block.
func (tv *TableView) computeAggregatesForBlock(ctx context.Context, block *grouping.Block, leafColumns []string, columnTypes map[string]queryspec.ColumnType, bulk level0CodeAggs) error {
	if block == nil {
		return nil
	}

	for _, group := range block.Groups {
		if err := ctx.Err(); err != nil {
			return err
		}
		// First, process child block (if any) - bottom-up
		if group.ChildBlock != nil {
			if err := tv.computeAggregatesForBlock(ctx, group.ChildBlock, leafColumns, columnTypes, bulk); err != nil {
				return err
			}
		}

		// Leaf membership is released once the grouping build finishes; a
		// repeated ComputeAggregates call afterwards keeps the aggregates
		// computed during the build instead of zeroing them.
		if group.ChildBlock == nil && group.Indices == nil && group.Aggregates != nil {
			continue
		}
		// Parents recombine from child states; if a prior pass compacted
		// the children's unique sets, the merge inputs are gone and the
		// parent's existing (correct) state must be kept as-is.
		if group.ChildBlock != nil && group.Aggregates != nil && childUniqueSetsCompacted(group.ChildBlock) {
			continue
		}

		// Now compute aggregates for this group
		group.Aggregates = make(map[string]aggregates.AggregateState)

		if group.ChildBlock == nil {
			// Leaf group: compute from indices. The pre-aggregated partials
			// are keyed by the level-0 group keys, so they apply only there.
			groupBulk := bulk
			if group.ParentGroup != nil {
				groupBulk = nil
			}
			tv.computeLeafAggregates(group, leafColumns, columnTypes, groupBulk)
		} else {
			// Parent group: combine from children
			tv.combineChildAggregates(group, leafColumns, columnTypes)
		}
	}
	return nil
}

// computeLeafAggregates computes aggregates for a leaf group by iterating over
// its indices; columns present in bulk take their state from the merged
// per-code partials instead.
func (tv *TableView) computeLeafAggregates(group *grouping.Group, leafColumns []string, columnTypes map[string]queryspec.ColumnType, bulk level0CodeAggs) {
	for _, colName := range leafColumns {
		colType := columnTypes[colName]
		col := tv.GetColumn(colName)
		if col == nil {
			continue
		}

		if aggs := bulk[colName]; aggs != nil {
			if state := codeAggState(aggs, group.GroupKey); state != nil {
				group.Aggregates[colName] = state
				continue
			}
		}

		state := aggregates.CreateAggState(colType)
		// Key columns: every value is distinct, so the unique count is the
		// count — never build the per-group string set.
		if strState, ok := state.(*aggregates.StringAggState); ok && col.IsKey() {
			strState.KeyUnique = true
		}

		// Add each value from the group's indices
		for _, idx := range group.Indices {
			switch colType {
			case queryspec.ColumnTypeNumeric:
				if numState, ok := state.(*aggregates.NumericAggState); ok {
					tv.addNumericValue(numState, col, idx)
				}
			case queryspec.ColumnTypeBool:
				if boolState, ok := state.(*aggregates.BoolAggState); ok {
					tv.addBoolValue(boolState, col, idx)
				}
			case queryspec.ColumnTypeDatetime:
				if dtState, ok := state.(*aggregates.DatetimeAggState); ok {
					tv.addDatetimeValue(dtState, col, idx)
				}
			case queryspec.ColumnTypeString:
				if strState, ok := state.(*aggregates.StringAggState); ok {
					tv.addStringValue(strState, col, idx)
				}
			}
		}

		group.Aggregates[colName] = state
	}
}

// combineChildAggregates combines aggregates from child groups into a parent group.
func (tv *TableView) combineChildAggregates(group *grouping.Group, leafColumns []string, columnTypes map[string]queryspec.ColumnType) {
	for _, colName := range leafColumns {
		colType := columnTypes[colName]
		parentState := aggregates.CreateAggState(colType)

		// Combine from all child groups
		for _, childGroup := range group.ChildBlock.Groups {
			if childState, ok := childGroup.Aggregates[colName]; ok {
				parentState.Combine(childState)
			}
		}

		group.Aggregates[colName] = parentState
	}
}

// addNumericValue adds a numeric value from a column to the aggregate state.
func (tv *TableView) addNumericValue(state *aggregates.NumericAggState, col columns.IDataColumn, idx uint32) {
	// Try to get numeric value - check for typed columns
	switch typedCol := col.(type) {
	case *columns.Uint32Column:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.AddUint32(val)
		}
	case interface{ GetValue(uint32) (float64, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	case interface{ GetValue(uint32) (int64, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(float64(val))
		}
	case interface{ GetValue(uint32) (uint32, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.AddUint32(val)
		}
	case interface{ GetValue(uint32) (uint64, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(float64(val))
		}
	default:
		// Fallback: try to parse string as number
		if strVal, err := col.GetString(idx); err == nil {
			var f float64
			if _, err := fmt.Sscanf(strVal, "%f", &f); err == nil {
				state.Add(f)
			}
		}
	}
}

// addBoolValue adds a boolean value from a column to the aggregate state.
func (tv *TableView) addBoolValue(state *aggregates.BoolAggState, col columns.IDataColumn, idx uint32) {
	switch typedCol := col.(type) {
	case *columns.BoolColumn:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	case interface{ GetValue(uint32) (bool, error) }:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	default:
		// Fallback: parse string
		if strVal, err := col.GetString(idx); err == nil {
			if val, err := columns.ParseBool(strVal); err == nil {
				state.Add(val)
			}
		}
	}
}

// addDatetimeValue adds a datetime value from a column to the aggregate state.
func (tv *TableView) addDatetimeValue(state *aggregates.DatetimeAggState, col columns.IDataColumn, idx uint32) {
	switch typedCol := col.(type) {
	case *columns.DatetimeColumn:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	case interface {
		GetValue(uint32) (time.Time, error)
	}:
		if val, err := typedCol.GetValue(idx); err == nil {
			state.Add(val)
		}
	default:
		// Fallback: parse string
		if strVal, err := col.GetString(idx); err == nil {
			if val, err := columns.ParseDatetime(strVal, time.UTC); err == nil && !val.IsZero() {
				state.Add(val)
			}
		}
	}
}

// addStringValue adds a string value from a column to the aggregate state.
func (tv *TableView) addStringValue(state *aggregates.StringAggState, col columns.IDataColumn, idx uint32) {
	if strVal, err := col.GetString(idx); err == nil {
		state.Add(strVal)
	}
}

// GetColumnType determines the column type for aggregate purposes.
func (tv *TableView) GetColumnType(colName string) queryspec.ColumnType {
	col := tv.GetColumn(colName)
	if col == nil {
		return queryspec.ColumnTypeString
	}

	// Check concrete types
	switch col.(type) {
	case *columns.Uint32Column:
		return queryspec.ColumnTypeNumeric
	case *columns.Int64Column:
		return queryspec.ColumnTypeNumeric
	case *columns.Uint64Column:
		return queryspec.ColumnTypeNumeric
	case *columns.Float64Column:
		return queryspec.ColumnTypeNumeric
	case *columns.BoolColumn:
		return queryspec.ColumnTypeBool
	case *columns.DatetimeColumn:
		return queryspec.ColumnTypeDatetime
	case *columns.StringColumn:
		return queryspec.ColumnTypeString
	case *columns.DurationColumn:
		return queryspec.ColumnTypeDatetime // Duration treated like datetime for aggregation
	// Computed column types
	case *columns.ComputedUint32Column:
		return queryspec.ColumnTypeNumeric
	case *columns.ComputedFloat64Column:
		return queryspec.ColumnTypeNumeric
	case *columns.ComputedInt64Column:
		return queryspec.ColumnTypeNumeric
	case *columns.ComputedStringColumn:
		return queryspec.ColumnTypeString
	}

	// Check for typed column interfaces (for computed/joined columns)
	switch col.(type) {
	case interface{ GetValue(uint32) (uint32, error) }:
		return queryspec.ColumnTypeNumeric
	case interface{ GetValue(uint32) (int64, error) }:
		return queryspec.ColumnTypeNumeric
	case interface{ GetValue(uint32) (uint64, error) }:
		return queryspec.ColumnTypeNumeric
	case interface{ GetValue(uint32) (float64, error) }:
		return queryspec.ColumnTypeNumeric
	case interface{ GetValue(uint32) (bool, error) }:
		return queryspec.ColumnTypeBool
	case interface {
		GetValue(uint32) (time.Time, error)
	}:
		return queryspec.ColumnTypeDatetime
	}

	return queryspec.ColumnTypeString
}

// GetColumnTypeName returns the Go struct name for a column (e.g., "StringColumn", "Uint32Column").
func (tv *TableView) GetColumnTypeName(colName string) string {
	col := tv.GetColumn(colName)
	if col == nil {
		return "unknown"
	}

	// Check concrete types and return struct names
	switch col.(type) {
	case *columns.Uint32Column:
		return "Uint32Column"
	case *columns.Int64Column:
		return "Int64Column"
	case *columns.Uint64Column:
		return "Uint64Column"
	case *columns.BoolColumn:
		return "BoolColumn"
	case *columns.DatetimeColumn:
		return "DatetimeColumn"
	case *columns.StringColumn:
		return "StringColumn"
	case *columns.DurationColumn:
		return "DurationColumn"
	case *columns.Float64Column:
		return "Float64Column"
	// Chunked column types
	case *columns.ChunkedStringColumn:
		return "ChunkedStringColumn"
	case *columns.ChunkedUint32Column:
		return "ChunkedUint32Column"
	case *columns.ChunkedInt64Column:
		return "ChunkedInt64Column"
	case *columns.ChunkedUint64Column:
		return "ChunkedUint64Column"
	case *columns.ChunkedFloat64Column:
		return "ChunkedFloat64Column"
	case *columns.ChunkedBoolColumn:
		return "ChunkedBoolColumn"
	case *columns.ChunkedDatetimeColumn:
		return "ChunkedDatetimeColumn"
	// Dictionary-encoded string columns (all code widths)
	case *columns.DictStringColumn[uint8], *columns.DictStringColumn[uint16], *columns.DictStringColumn[uint32]:
		return "DictStringColumn"
	case *columns.ChunkedDictStringColumn[uint8], *columns.ChunkedDictStringColumn[uint16], *columns.ChunkedDictStringColumn[uint32]:
		return "ChunkedDictStringColumn"
	// Arena-backed string storage (phase 4c)
	case *columns.ChunkedArenaStringColumn:
		return "ChunkedArenaStringColumn"
	case *columns.ChunkedFrontCodedStringColumn:
		return "ChunkedFrontCodedStringColumn"
	// Computed column types
	case *columns.ComputedUint32Column:
		return "ComputedUint32Column"
	case *columns.ComputedFloat64Column:
		return "ComputedFloat64Column"
	case *columns.ComputedInt64Column:
		return "ComputedInt64Column"
	case *columns.ComputedStringColumn:
		return "ComputedStringColumn"
	}

	// For joined columns, check if we can get the underlying type
	if jc, ok := col.(columns.IJoinedDataColumn); ok {
		// Return a descriptive name for joined columns
		return "JoinedColumn[" + jc.ColumnDef().Name() + "]"
	}

	return "unknown"
}

// SortGroupsByAggregate re-sorts groups in a block by their aggregate value.
// This should be called after ComputeAggregates.
// groupAggSorts maps grouped column names to their aggregate sort specification.
func (tv *TableView) SortGroupsByAggregate(groupAggSorts map[string]*queryspec.GroupAggSort) {
	if tv.firstBlock == nil || len(groupAggSorts) == 0 {
		return
	}

	// Walk through grouping hierarchy and sort blocks that have aggregate sort specified
	tv.sortBlockByAggregate(tv.firstBlock, groupAggSorts)
}

// sortBlockByAggregate recursively sorts groups in a block and its children by aggregate values.
func (tv *TableView) sortBlockByAggregate(block *grouping.Block, groupAggSorts map[string]*queryspec.GroupAggSort) {
	if block == nil {
		return
	}

	// Get the grouped column name for this block
	groupedColName := ""
	for name, gc := range tv.groupedColumns {
		if gc == block.GroupedColumn {
			groupedColName = name
			break
		}
	}

	// Check if this grouped column has an aggregate sort
	if aggSort, ok := groupAggSorts[groupedColName]; ok && aggSort != nil {
		// Sort groups based on sort type
		sort.Slice(block.Groups, func(i, j int) bool {
			var cmp int

			switch aggSort.AggType {
			case queryspec.AggRowCount:
				// Sort by total row count in the group
				countI := tv.getGroupRowCount(block.Groups[i])
				countJ := tv.getGroupRowCount(block.Groups[j])
				if countI < countJ {
					cmp = -1
				} else if countI > countJ {
					cmp = 1
				}
			case queryspec.AggSubgroupCount:
				// Sort by number of subgroups
				countI := tv.getGroupSubgroupCount(block.Groups[i])
				countJ := tv.getGroupSubgroupCount(block.Groups[j])
				if countI < countJ {
					cmp = -1
				} else if countI > countJ {
					cmp = 1
				}
			default:
				// Sort by leaf column aggregate value
				stateI := block.Groups[i].Aggregates[aggSort.LeafColumn]
				stateJ := block.Groups[j].Aggregates[aggSort.LeafColumn]
				cmp = tv.compareAggregateValues(stateI, stateJ, aggSort.AggType)
			}

			if aggSort.Descending {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	// Recursively sort child blocks
	for _, group := range block.Groups {
		if group.ChildBlock != nil {
			tv.sortBlockByAggregate(group.ChildBlock, groupAggSorts)
		}
	}
}

// getGroupRowCount returns the total number of rows in a group (recursively counting leaf indices).
func (tv *TableView) getGroupRowCount(group *grouping.Group) int {
	if group.ChildBlock == nil {
		// Leaf group - return direct row count
		return group.Length()
	}
	// Parent group - sum up all child group row counts
	total := 0
	for _, childGroup := range group.ChildBlock.Groups {
		total += tv.getGroupRowCount(childGroup)
	}
	return total
}

// getGroupSubgroupCount returns the number of direct subgroups in a group.
func (tv *TableView) getGroupSubgroupCount(group *grouping.Group) int {
	if group.ChildBlock == nil {
		return 0
	}
	return len(group.ChildBlock.Groups)
}

// compareAggregateValues compares two aggregate states for a specific aggregate type.
// Returns -1 if a < b, 0 if equal, 1 if a > b.
func (tv *TableView) compareAggregateValues(a, b aggregates.AggregateState, aggType queryspec.AggregateType) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1 // nil sorts last
	}
	if b == nil {
		return -1
	}

	// Extract comparable values based on aggregate type
	valA := tv.getAggregateNumericValue(a, aggType)
	valB := tv.getAggregateNumericValue(b, aggType)

	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

// getAggregateNumericValue extracts a numeric value from an aggregate state for comparison.
func (tv *TableView) getAggregateNumericValue(state aggregates.AggregateState, aggType queryspec.AggregateType) float64 {
	switch s := state.(type) {
	case *aggregates.NumericAggState:
		switch aggType {
		case queryspec.AggCount:
			return float64(s.Count)
		case queryspec.AggSum:
			return s.Sum
		case queryspec.AggAvg:
			return s.Avg()
		case queryspec.AggStdDev:
			return s.StdDev()
		case queryspec.AggMin:
			return s.Min
		case queryspec.AggMax:
			return s.Max
		}
	case *aggregates.BoolAggState:
		switch aggType {
		case queryspec.AggCount:
			return float64(s.Count)
		case queryspec.AggTrue:
			return float64(s.TrueCount)
		case queryspec.AggFalse:
			return float64(s.FalseCount)
		case queryspec.AggRatio:
			return s.Ratio()
		}
	case *aggregates.StringAggState:
		switch aggType {
		case queryspec.AggCount:
			return float64(s.Count)
		case queryspec.AggUnique:
			return float64(s.UniqueCount())
		}
	case *aggregates.DatetimeAggState:
		switch aggType {
		case queryspec.AggCount:
			return float64(s.Count)
		case queryspec.AggMin:
			return float64(s.Min) // epoch nanoseconds
		case queryspec.AggMax:
			return float64(s.Max) // epoch nanoseconds
		case queryspec.AggAvg:
			// Average time as epoch nanoseconds
			if s.Count == 0 {
				return 0
			}
			return s.Sum / float64(s.Count)
		case queryspec.AggStdDev:
			return float64(s.StdDev()) // duration in nanoseconds
		case queryspec.AggSpan:
			return float64(s.Max - s.Min) // duration in nanoseconds
		}
	}
	return 0
}
