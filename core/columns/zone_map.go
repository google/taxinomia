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

// zoneMap holds per-chunk min/max bounds for a chunked column
// (docs/scaling-to-1b-rows.md §3.A, §5): the structure that lets a filter
// eliminate whole chunks that cannot contain a match before any row is
// touched. It is built once at FinalizeColumn — columns are immutable after
// finalize, so the bounds never go stale.
//
// Values for which unordered reports true (NaN for floats) have no position
// in the ordering: they are excluded from the bounds and tracked per chunk in
// hasUnordered, so a chunk holding only NaNs still answers point queries for
// NaN correctly while range queries never match one.
//
// All query methods treat a nil receiver as "no zone map": every chunk may
// contain a match. Pruning is strictly an optimization; its absence (a column
// that was never finalized) leaves the filter methods correct but unpruned.
type zoneMap[T any] struct {
	cmp       func(T, T) int
	unordered func(T) bool // nil when every value is ordered

	mins, maxs   []T
	hasBounds    []bool // chunk has at least one ordered value
	hasUnordered []bool // chunk has at least one unordered value; nil when unordered is nil
}

// buildZoneMap scans the chunks once and records per-chunk bounds.
func buildZoneMap[T any](data *chunkedData[T], cmp func(T, T) int, unordered func(T) bool) *zoneMap[T] {
	n := data.numChunks()
	z := &zoneMap[T]{
		cmp:       cmp,
		unordered: unordered,
		mins:      make([]T, n),
		maxs:      make([]T, n),
		hasBounds: make([]bool, n),
	}
	if unordered != nil {
		z.hasUnordered = make([]bool, n)
	}
	for ci := 0; ci < n; ci++ {
		for _, v := range data.chunk(ci) {
			if unordered != nil && unordered(v) {
				z.hasUnordered[ci] = true
				continue
			}
			if !z.hasBounds[ci] {
				z.mins[ci], z.maxs[ci] = v, v
				z.hasBounds[ci] = true
				continue
			}
			if cmp(v, z.mins[ci]) < 0 {
				z.mins[ci] = v
			} else if cmp(v, z.maxs[ci]) > 0 {
				z.maxs[ci] = v
			}
		}
	}
	return z
}

// mayContainPoint reports whether chunk ci may contain a value equal to v
// (equality in the column's grouping sense: an unordered target matches
// chunks that hold unordered values).
func (z *zoneMap[T]) mayContainPoint(ci int, v T) bool {
	if z == nil {
		return true
	}
	if z.unordered != nil && z.unordered(v) {
		return z.hasUnordered[ci]
	}
	return z.hasBounds[ci] && z.cmp(v, z.mins[ci]) >= 0 && z.cmp(v, z.maxs[ci]) <= 0
}

// mayContainAny reports whether chunk ci may contain any of the given values.
func (z *zoneMap[T]) mayContainAny(ci int, vs []T) bool {
	if z == nil {
		return true
	}
	for _, v := range vs {
		if z.mayContainPoint(ci, v) {
			return true
		}
	}
	return false
}

// mayContainRange reports whether chunk ci may contain a value in [lo, hi]
// (inclusive; nil bound = unbounded). Unordered values never lie in a range,
// so a chunk holding only NaNs never survives.
func (z *zoneMap[T]) mayContainRange(ci int, lo, hi *T) bool {
	if z == nil {
		return true
	}
	if !z.hasBounds[ci] {
		return false
	}
	if lo != nil && z.cmp(z.maxs[ci], *lo) < 0 {
		return false
	}
	if hi != nil && z.cmp(z.mins[ci], *hi) > 0 {
		return false
	}
	return true
}

// bounds returns chunk ci's recorded bounds; ok is false when the chunk has
// no ordered values.
func (z *zoneMap[T]) bounds(ci int) (min, max T, ok bool) {
	if z == nil || !z.hasBounds[ci] {
		var zero T
		return zero, zero, false
	}
	return z.mins[ci], z.maxs[ci], true
}
