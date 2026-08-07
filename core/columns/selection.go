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

import (
	"fmt"
	"math/bits"
)

// Selection is a dense bitmap of selected rows over a universe of Len rows.
// It is the scalable replacement for materialised index lists: one bit per
// row of the table, regardless of how many rows are selected, where []uint32
// costs four bytes per selected row.
//
// The zero value is an empty selection over zero rows; use NewSelection or
// NewSelectionAll to create one sized to a table.
type Selection struct {
	words []uint64
	n     int
}

// NewSelection returns a selection over n rows with no rows selected.
func NewSelection(n int) *Selection {
	return &Selection{words: make([]uint64, (n+63)/64), n: n}
}

// NewSelectionAll returns a selection over n rows with every row selected.
func NewSelectionAll(n int) *Selection {
	s := NewSelection(n)
	for i := range s.words {
		s.words[i] = ^uint64(0)
	}
	s.maskTail()
	return s
}

// SelectionFromIndices returns a selection over n rows containing exactly the
// given row indices.
func SelectionFromIndices(n int, indices []uint32) *Selection {
	s := NewSelection(n)
	for _, i := range indices {
		s.Add(i)
	}
	return s
}

// maskTail clears the unused bits of the last word so Count and ForEach never
// observe phantom rows beyond Len.
func (s *Selection) maskTail() {
	if rem := s.n % 64; rem != 0 && len(s.words) > 0 {
		s.words[len(s.words)-1] &= (uint64(1) << rem) - 1
	}
}

// Len returns the size of the row universe (not the number of selected rows).
func (s *Selection) Len() int {
	return s.n
}

// Count returns the number of selected rows.
func (s *Selection) Count() int {
	count := 0
	for _, w := range s.words {
		count += bits.OnesCount64(w)
	}
	return count
}

// Contains reports whether row i is selected.
func (s *Selection) Contains(i uint32) bool {
	if i >= uint32(s.n) {
		return false
	}
	return s.words[i/64]&(uint64(1)<<(i%64)) != 0
}

// Add selects row i. It panics if i is outside the row universe.
func (s *Selection) Add(i uint32) {
	if i >= uint32(s.n) {
		panic(fmt.Sprintf("columns: Selection.Add(%d) out of range (universe %d)", i, s.n))
	}
	s.words[i/64] |= uint64(1) << (i % 64)
}

// Remove deselects row i. Removing a row outside the universe is a no-op.
func (s *Selection) Remove(i uint32) {
	if i >= uint32(s.n) {
		return
	}
	s.words[i/64] &^= uint64(1) << (i % 64)
}

// And intersects s with o in place. Both selections must cover the same row
// universe.
func (s *Selection) And(o *Selection) {
	if s.n != o.n {
		panic(fmt.Sprintf("columns: Selection.And over mismatched universes (%d vs %d)", s.n, o.n))
	}
	for i := range s.words {
		s.words[i] &= o.words[i]
	}
}

// ForEach calls f for every selected row in ascending order. f may Add or
// Remove rows while iterating: changes to the 64-row word currently being
// visited are not observed (each word is read once, up front), changes to
// later words are.
func (s *Selection) ForEach(f func(i uint32)) {
	for w, word := range s.words {
		for word != 0 {
			tz := bits.TrailingZeros64(word)
			f(uint32(w*64 + tz))
			word &^= uint64(1) << tz
		}
	}
}

// ForEachRow implements RowSet: it calls f for every selected row in
// ascending order until f returns false. Unlike ForEach, the selection must
// not be modified during iteration.
func (s *Selection) ForEachRow(f func(i uint32) bool) {
	for w, word := range s.words {
		for word != 0 {
			tz := bits.TrailingZeros64(word)
			if !f(uint32(w*64 + tz)) {
				return
			}
			word &^= uint64(1) << tz
		}
	}
}

// NumRows implements RowSet; it is Count.
func (s *Selection) NumRows() int {
	return s.Count()
}

// forEachRowIn implements rangeRowSet: ForEachRow restricted to rows in
// [lo, hi). Word-aligned ranges (chunk boundaries with a chunk size divisible
// by 64) touch only whole words; unaligned edges are masked.
func (s *Selection) forEachRowIn(lo, hi int, f func(i uint32) bool) {
	if hi > s.n {
		hi = s.n
	}
	if lo < 0 {
		lo = 0
	}
	if lo >= hi {
		return
	}
	wLo, wLast := lo/64, (hi-1)/64
	for w := wLo; w <= wLast; w++ {
		word := s.words[w]
		if w == wLo && lo%64 != 0 {
			word &= ^uint64(0) << uint(lo%64)
		}
		if w == wLast && hi%64 != 0 {
			word &= (uint64(1) << uint(hi%64)) - 1
		}
		for word != 0 {
			tz := bits.TrailingZeros64(word)
			if !f(uint32(w*64 + tz)) {
				return
			}
			word &^= uint64(1) << tz
		}
	}
}

// ToIndices materialises the selection as a sorted []uint32 index list. It is
// the compatibility adapter for callers that predate Selection: the result
// costs four bytes per selected row, which is exactly the cost Selection
// exists to avoid — prefer ForEach or Contains in new code.
func (s *Selection) ToIndices() []uint32 {
	indices := make([]uint32, 0, s.Count())
	s.ForEach(func(i uint32) {
		indices = append(indices, i)
	})
	return indices
}

// filterToSelection builds a selection of the rows whose value satisfies the
// predicate. Shared loop behind the typed FilterSelection methods.
func filterToSelection[T any](data []T, predicate func(T) bool) *Selection {
	s := NewSelection(len(data))
	for i, v := range data {
		if predicate(v) {
			s.Add(uint32(i))
		}
	}
	return s
}
