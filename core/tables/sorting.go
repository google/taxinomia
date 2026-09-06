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
	"sort"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/queryspec"
)

// sortableColumn holds a column reference and its sort direction
type sortableColumn struct {
	name       string
	col        columns.IDataColumn
	descending bool
}

// topKHeap implements a max-heap for top-K selection
// When we want the smallest K elements, we use a max-heap:
// - If new element is smaller than max, pop max and push new element
// - At the end, heap contains K smallest elements
type topKHeap struct {
	indices []uint32
	cols    []sortableColumn
}

func (h *topKHeap) Len() int { return len(h.indices) }

// Less returns true if element at i should be ABOVE j in the heap.
// For a max-heap of "best" elements, the worst element should be at the top.
// "Best" means what we want to keep (smallest for asc, largest for desc).
// So the top of the heap should be the "worst" of the K best elements.
func (h *topKHeap) Less(i, j int) bool {
	// Compare using multi-column comparison
	// We want the "worst" element at the top of the heap
	// "Worst" = largest for ascending, smallest for descending
	cmp := h.compare(h.indices[i], h.indices[j])
	// For max-heap semantics: return true if i > j (larger values bubble up)
	return cmp > 0
}

func (h *topKHeap) Swap(i, j int) {
	h.indices[i], h.indices[j] = h.indices[j], h.indices[i]
}

func (h *topKHeap) Push(x interface{}) {
	h.indices = append(h.indices, x.(uint32))
}

func (h *topKHeap) Pop() interface{} {
	old := h.indices
	n := len(old)
	x := old[n-1]
	h.indices = old[0 : n-1]
	return x
}

// compare compares two row indices using multi-column sort order
// Returns negative if i < j, zero if equal, positive if i > j
func (h *topKHeap) compare(i, j uint32) int {
	for _, sc := range h.cols {
		cmp := columns.CompareAtIndex(sc.col, i, j)
		if cmp != 0 {
			if sc.descending {
				return -cmp // Reverse for descending
			}
			return cmp
		}
	}
	return 0
}

// peek returns the top element without removing it
func (h *topKHeap) peek() uint32 {
	return h.indices[0]
}

// replaceTop overwrites the top element and restores the heap invariant.
// Equivalent to heap.Pop followed by heap.Push, but without boxing the
// uint32 into an interface{}, which allocates on every replacement.
func (h *topKHeap) replaceTop(idx uint32) {
	h.indices[0] = idx
	i := 0
	n := len(h.indices)
	for {
		l, r := 2*i+1, 2*i+2
		top := i
		if l < n && h.Less(l, top) {
			top = l
		}
		if r < n && h.Less(r, top) {
			top = r
		}
		if top == i {
			return
		}
		h.Swap(i, top)
		i = top
	}
}

// GetSortedTopK returns the top K indices from the input, sorted according to sortOrder.
// Uses heap-based selection: O(n log k) instead of O(n log n) for full sort.
//
// Algorithm:
// 1. Build a max-heap of size K (keeping the K "best" elements seen so far)
// 2. Scan all indices, replacing heap top when a better element is found
// 3. Sort the final K elements
//
// Deprecated: it consumes a materialised index list, which costs four bytes
// per row. Nothing in this repository uses it; it remains for external
// callers and will be removed in a future major cleanup. GetFilteredRowsSorted
// performs the same selection directly on the filter bitmap.
func (t *TableView) GetSortedTopK(indices []uint32, sortOrder []queryspec.SortColumn, limit int) []uint32 {
	if len(indices) == 0 || limit <= 0 {
		return []uint32{}
	}

	// Resolve columns and build sortable column list
	sortableCols := make([]sortableColumn, 0, len(sortOrder))
	for _, so := range sortOrder {
		col := t.GetColumn(so.Name)
		if col != nil {
			sortableCols = append(sortableCols, sortableColumn{
				col:        col,
				descending: so.Descending,
			})
		}
	}

	// If no valid sort columns, return first K indices as-is
	if len(sortableCols) == 0 {
		if limit >= len(indices) {
			return indices
		}
		return indices[:limit]
	}

	// If K >= n, just sort all and return
	if limit >= len(indices) {
		return t.sortIndices(indices, sortableCols)
	}

	// Heap-based top-K selection
	h := &topKHeap{
		indices: make([]uint32, 0, limit),
		cols:    sortableCols,
	}

	// Initialize heap with first K elements
	for i := 0; i < limit; i++ {
		h.indices = append(h.indices, indices[i])
	}
	heap.Init(h)

	// Process remaining elements
	for i := limit; i < len(indices); i++ {
		idx := indices[i]
		// Compare with heap top (the "worst" of current K best)
		cmp := h.compare(idx, h.peek())
		if cmp < 0 {
			// New element is "better" - replace heap top
			heap.Pop(h)
			heap.Push(h, idx)
		}
	}

	// Extract and sort the K elements
	result := h.indices
	return t.sortIndices(result, sortableCols)
}

// sortedByStorage reports whether the requested order is the storage
// order: the sort columns start with the full storage key, every key column
// ascending. Storage sorted by its key puts the rows of the selection in
// exactly that order (the key is a total order over the rows, so no later
// column can matter), which lets the caller skip sorting entirely.
func (t *TableView) sortedByStorage(cols []sortableColumn, storageKey []string) bool {
	if len(storageKey) == 0 || len(cols) < len(storageKey) {
		return false
	}
	for i, k := range storageKey {
		if cols[i].name != k || cols[i].descending {
			return false
		}
	}
	return true
}

// sortIndices sorts a slice of indices according to the sortable columns
func (t *TableView) sortIndices(indices []uint32, cols []sortableColumn) []uint32 {
	sort.Slice(indices, func(i, j int) bool {
		for _, sc := range cols {
			cmp := columns.CompareAtIndex(sc.col, indices[i], indices[j])
			if cmp != 0 {
				if sc.descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
	return indices
}

// collectRows materialises up to limit rows of sel in set order (all rows
// when limit < 0). The allocation is bounded by the caller's limit — or by the
// output size when everything was asked for — never by the match count alone.
func collectRows(sel columns.RowSet, limit int) []uint32 {
	n := sel.NumRows()
	if limit >= 0 && limit < n {
		n = limit
	}
	out := make([]uint32, 0, n)
	sel.ForEachRow(func(i uint32) bool {
		if len(out) >= n {
			return false
		}
		out = append(out, i)
		return true
	})
	return out
}

// sortedTopK selects the top limit rows of sel according to sortableCols with
// a bounded heap, scanning the selection once without materialising it.
// limit must be > 0.
func (t *TableView) sortedTopK(sel columns.RowSet, sortableCols []sortableColumn, limit int) []uint32 {
	if limit >= sel.NumRows() {
		return t.sortIndices(collectRows(sel, -1), sortableCols)
	}

	h := &topKHeap{
		indices: make([]uint32, 0, limit),
		cols:    sortableCols,
	}
	sel.ForEachRow(func(idx uint32) bool {
		if len(h.indices) < limit {
			h.indices = append(h.indices, idx)
			if len(h.indices) == limit {
				heap.Init(h)
			}
			return true
		}
		// Compare with heap top (the "worst" of current K best)
		if h.compare(idx, h.peek()) < 0 {
			// New element is "better" - replace heap top
			h.replaceTop(idx)
		}
		return true
	})
	return t.sortIndices(h.indices, sortableCols)
}

// GetFilteredRowsSorted returns rows sorted according to sortOrder, limited
// to the top K (all rows when limit <= 0). It combines filtering, sorting
// and limiting into one pass over the filter selection bitmap.
//
// The table's storage key (DataTable.SortKey) is appended as the final
// tie-breaker when sortOrder does not already contain it, so rows equal on
// every listed column come back in a deterministic order. When sortOrder
// begins with the storage key ascending, the selection is already in that
// order and is read straight from storage — the free path for the default
// view, where the key is the leftmost column.
func (t *TableView) GetFilteredRowsSorted(columnNames []string, sortOrder []queryspec.SortColumn, limit int) []map[string]string {
	sel := t.rowSet()

	// Resolve columns and build sortable column list
	sortableCols := make([]sortableColumn, 0, len(sortOrder)+1)
	listed := make(map[string]bool, len(sortOrder))
	for _, so := range sortOrder {
		if col := t.GetColumn(so.Name); col != nil && !listed[so.Name] {
			sortableCols = append(sortableCols, sortableColumn{name: so.Name, col: col, descending: so.Descending})
			listed[so.Name] = true
		}
	}
	storageKey := t.baseTable.SortKey()
	for _, k := range storageKey {
		if col := t.GetColumn(k); col != nil && !listed[k] {
			sortableCols = append(sortableCols, sortableColumn{name: k, col: col})
			listed[k] = true
		}
	}

	var sortedIndices []uint32
	switch {
	case len(sortableCols) == 0 || t.sortedByStorage(sortableCols, storageKey):
		// Storage order is the requested order: read the selection as is.
		if limit > 0 {
			sortedIndices = collectRows(sel, limit)
		} else {
			sortedIndices = collectRows(sel, -1)
		}
	case limit > 0:
		sortedIndices = t.sortedTopK(sel, sortableCols, limit)
	default:
		sortedIndices = t.sortIndices(collectRows(sel, -1), sortableCols)
	}

	// Build result rows
	rows := make([]map[string]string, 0, len(sortedIndices))
	for _, rowIndex := range sortedIndices {
		row := make(map[string]string)
		for _, colName := range columnNames {
			col := t.GetColumn(colName)
			if col != nil {
				value, err := col.GetString(rowIndex)
				if err != nil {
					row[colName] = columns.ErrorLabel
				} else {
					row[colName] = value
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}
