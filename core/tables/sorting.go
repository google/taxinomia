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
	"github.com/google/taxinomia/core/query"
)

// sortableColumn holds a column reference and its sort direction
type sortableColumn struct {
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

// GetSortedTopK returns the top K indices from the input, sorted according to sortOrder.
// Uses heap-based selection: O(n log k) instead of O(n log n) for full sort.
//
// Algorithm:
// 1. Build a max-heap of size K (keeping the K "best" elements seen so far)
// 2. Scan all indices, replacing heap top when a better element is found
// 3. Sort the final K elements
func (t *TableView) GetSortedTopK(indices []uint32, sortOrder []query.SortColumn, limit int) []uint32 {
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

// GetFilteredRowsSorted returns rows sorted according to sortOrder, limited to top K.
// This combines filtering, sorting, and limiting into an efficient operation.
func (t *TableView) GetFilteredRowsSorted(columnNames []string, sortOrder []query.SortColumn, limit int) []map[string]string {
	// Get filtered indices
	filteredIndices := t.GetFilteredIndices()

	// Get top K sorted indices
	var sortedIndices []uint32
	if len(sortOrder) > 0 && limit > 0 {
		sortedIndices = t.GetSortedTopK(filteredIndices, sortOrder, limit)
	} else if limit > 0 && limit < len(filteredIndices) {
		sortedIndices = filteredIndices[:limit]
	} else {
		sortedIndices = filteredIndices
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
