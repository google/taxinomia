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

package columns

// RowSet is an ordered set of row indices, the input every selection-consuming
// operation takes. It abstracts over the two shapes a selection has in
// practice: a Selection bitmap (a filter result over the whole table, O(rows/8)
// regardless of how many rows match) and an explicit index list (a group's
// transient membership during a grouping build). Operations that consume a
// RowSet never materialise it, so no selection allocation scales with the
// match count.
type RowSet interface {
	// ForEachRow calls f for each row in set order until f returns false.
	// The set must not be modified during iteration.
	ForEachRow(f func(i uint32) bool)
	// NumRows returns the number of rows in the set.
	NumRows() int
}

// AllRows returns the RowSet {0, 1, ..., n-1}: every row of an n-row table,
// without allocating. It is the selection of an unfiltered table.
func AllRows(n int) RowSet {
	return allRows(n)
}

type allRows int

func (r allRows) ForEachRow(f func(i uint32) bool) {
	for i := uint32(0); i < uint32(r); i++ {
		if !f(i) {
			return
		}
	}
}

func (r allRows) NumRows() int {
	return int(r)
}

// RowIndices is an explicit index list viewed as a RowSet. It bridges
// pre-materialised row lists — group membership, join results — into
// RowSet-consuming operations; rows are visited in slice order.
type RowIndices []uint32

func (r RowIndices) ForEachRow(f func(i uint32) bool) {
	for _, i := range r {
		if !f(i) {
			return
		}
	}
}

func (r RowIndices) NumRows() int {
	return len(r)
}

// rowSetIndices materialises sel as a []uint32, for bridging into index-list
// APIs (the deprecated GroupIndices behind the fallback shim). The result
// costs four bytes per row in the set — exactly what RowSet-consuming code
// avoids — so it stays confined to compatibility paths.
func rowSetIndices(sel RowSet) []uint32 {
	indices := make([]uint32, 0, sel.NumRows())
	sel.ForEachRow(func(i uint32) bool {
		indices = append(indices, i)
		return true
	})
	return indices
}
