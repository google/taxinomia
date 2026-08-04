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
	"sort"
	"strings"
	"sync"
)

// ChunkedDictStringColumn is DictStringColumn with chunked code storage: one
// global, column-level dictionary plus per-row codes held in fixed-size heap
// chunks. The dictionary being global is what makes codes comparable across
// chunks (docs/scaling-to-1b-rows.md §8), so per-chunk partial group counts
// merge by array addition; the codes being chunked is what gives pruning,
// parallelism and cancellation their granularity.
type ChunkedDictStringColumn[K Unsigned] struct {
	columnDef *ColumnDef
	dict      []string        // code -> distinct value (global across chunks)
	codes     chunkedData[K]  // row -> code
	index     map[string]K    // value -> code (released by FinalizeColumn unless key)
	isKey     bool
	zones     *zoneMap[string] // per-chunk min/max value, built by FinalizeColumn
	ranks     []K              // code -> sort rank, built lazily by Ranks()
	ranksOnce sync.Once
}

// NewChunkedDictStringColumn creates an empty dictionary-encoded chunked
// string column.
func NewChunkedDictStringColumn[K Unsigned](columnDef *ColumnDef) *ChunkedDictStringColumn[K] {
	return newChunkedDictStringColumn[K](columnDef, DefaultChunkSize)
}

func newChunkedDictStringColumn[K Unsigned](columnDef *ColumnDef, chunkSize int) *ChunkedDictStringColumn[K] {
	return &ChunkedDictStringColumn[K]{
		columnDef: columnDef,
		dict:      make([]string, 0),
		codes:     newChunkedData[K](chunkSize),
		index:     make(map[string]K),
	}
}

// Append adds a value, interning it in the dictionary.
//
// It panics if the number of distinct values would exceed what the code width
// K can represent. Callers that don't know the cardinality up front should
// build a ChunkedStringColumn and run CompactChunkedStringColumn, which picks
// K from the data.
func (c *ChunkedDictStringColumn[K]) Append(value string) {
	code, exists := c.index[value]
	if !exists {
		if len(c.dict) > maxCode[K]() {
			panic(fmt.Sprintf("column %q: dictionary exceeded code width (%d distinct values)", c.columnDef.Name(), len(c.dict)))
		}
		code = K(len(c.dict))
		c.dict = append(c.dict, value)
		c.index[value] = code
	}
	c.codes.append(code)
}

func (c *ChunkedDictStringColumn[K]) Length() int {
	return c.codes.len()
}

func (c *ChunkedDictStringColumn[K]) ColumnDef() *ColumnDef {
	return c.columnDef
}

// Cardinality returns the number of distinct values held in the dictionary.
func (c *ChunkedDictStringColumn[K]) Cardinality() int {
	return len(c.dict)
}

func (c *ChunkedDictStringColumn[K]) GetValue(i uint32) (string, error) {
	if i >= uint32(c.codes.len()) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, c.codes.len())
	}
	return c.dict[c.codes.at(i)], nil
}

// GetString returns the string value at index i. No formatting, no allocation.
func (c *ChunkedDictStringColumn[K]) GetString(i uint32) (string, error) {
	return c.GetValue(i)
}

// GetIndex returns the row index holding the given value.
//
// Only meaningful for key columns. When every value is distinct the n-th
// appended value receives code n, so the code is also the row index.
func (c *ChunkedDictStringColumn[K]) GetIndex(v string) (uint32, error) {
	if !c.isKey {
		return 0, fmt.Errorf("column %q is not a key column and doesn't support reverse lookups", c.columnDef.Name())
	}
	if code, exists := c.index[v]; exists {
		return uint32(code), nil
	}
	return 0, fmt.Errorf("value %q not found in column %q", v, c.columnDef.Name())
}

// GetCode returns the dictionary code for row i. Joiners can use it to memoize
// a resolution per distinct value rather than per row.
func (c *ChunkedDictStringColumn[K]) GetCode(i uint32) K {
	return c.codes.at(i)
}

// DictValue returns the value for a dictionary code, which is also the group
// key used by the grouping operations.
func (c *ChunkedDictStringColumn[K]) DictValue(code uint32) string {
	return c.dict[code]
}

func (c *ChunkedDictStringColumn[K]) IsKey() bool {
	return c.isKey
}

func (c *ChunkedDictStringColumn[K]) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedStringColumn(columnDef, joiner, c)
}

// FinalizeColumn detects whether the column happens to be unique and builds
// the per-chunk zone maps. The dictionary and its index are already built by
// Append, so uniqueness needs no second pass over the rows.
//
// For non-key columns the interning map is released. No Append after
// FinalizeColumn.
func (c *ChunkedDictStringColumn[K]) FinalizeColumn() {
	c.isKey = len(c.dict) == c.codes.len()
	if !c.isKey {
		c.index = nil
	}
	c.zones = c.buildZones()
}

// buildZones records each chunk's min/max value. Codes are ranked by value
// once so the per-row tracking is an integer compare, then the bounds are
// stored as strings — the same zoneMap shape as the plain chunked columns,
// and what the native file format serializes.
func (c *ChunkedDictStringColumn[K]) buildZones() *zoneMap[string] {
	n := c.codes.numChunks()
	z := &zoneMap[string]{
		cmp:       strings.Compare,
		mins:      make([]string, n),
		maxs:      make([]string, n),
		hasBounds: make([]bool, n),
	}
	if len(c.dict) == 0 {
		return z
	}
	order := make([]int, len(c.dict)) // rank -> code
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return c.dict[order[a]] < c.dict[order[b]] })
	ranks := make([]int, len(c.dict)) // code -> rank
	for rank, code := range order {
		ranks[code] = rank
	}
	for ci := 0; ci < n; ci++ {
		chunk := c.codes.chunk(ci)
		minR, maxR := ranks[chunk[0]], ranks[chunk[0]]
		for _, code := range chunk[1:] {
			if r := ranks[code]; r < minR {
				minR = r
			} else if r > maxR {
				maxR = r
			}
		}
		z.mins[ci], z.maxs[ci] = c.dict[order[minR]], c.dict[order[maxR]]
		z.hasBounds[ci] = true
	}
	return z
}

// CompareRows compares the values at rows i and j (value order, resolved
// through the dictionary), satisfying RowComparator. Identical to what the
// string-fallback comparison produces for this column, without the error
// handling per row.
func (c *ChunkedDictStringColumn[K]) CompareRows(i, j uint32) int {
	return strings.Compare(c.dict[c.codes.at(i)], c.dict[c.codes.at(j)])
}

// lookupCode returns the dictionary code for a value: via the interning map
// while it exists (key columns), by dictionary scan otherwise — O(distinct),
// paid once per filter, never per row.
func (c *ChunkedDictStringColumn[K]) lookupCode(v string) (K, bool) {
	if c.index != nil {
		code, ok := c.index[v]
		return code, ok
	}
	for code, s := range c.dict {
		if s == v {
			return K(code), true
		}
	}
	return 0, false
}

// FilterSelectionEqual returns the rows whose value is exactly v. A value
// absent from the dictionary returns an empty selection without touching any
// chunk; a present one scans only the chunks whose zone map admits it,
// comparing codes.
func (c *ChunkedDictStringColumn[K]) FilterSelectionEqual(v string) *Selection {
	s := NewSelection(c.codes.len())
	code, ok := c.lookupCode(v)
	if !ok {
		return s
	}
	for ci := 0; ci < c.codes.numChunks(); ci++ {
		if !c.zones.mayContainPoint(ci, v) {
			continue
		}
		base := uint32(ci) << c.codes.shift
		for j, cd := range c.codes.chunk(ci) {
			if cd == code {
				s.Add(base + uint32(j))
			}
		}
	}
	return s
}

// FilterSelectionIn returns the rows whose value equals any of the given
// values (the multi-value OR filter), pruning chunks by zone map. Values
// absent from the dictionary are dropped up front.
func (c *ChunkedDictStringColumn[K]) FilterSelectionIn(values []string) *Selection {
	s := NewSelection(c.codes.len())
	keep := make([]bool, len(c.dict))
	present := make([]string, 0, len(values))
	for _, v := range values {
		if code, ok := c.lookupCode(v); ok && !keep[code] {
			keep[code] = true
			present = append(present, v)
		}
	}
	if len(present) == 0 {
		return s
	}
	for ci := 0; ci < c.codes.numChunks(); ci++ {
		if !c.zones.mayContainAny(ci, present) {
			continue
		}
		base := uint32(ci) << c.codes.shift
		for j, cd := range c.codes.chunk(ci) {
			if keep[cd] {
				s.Add(base + uint32(j))
			}
		}
	}
	return s
}

// FilterSelectionRange returns the rows whose value lies in [lo, hi]
// (inclusive; a nil bound is unbounded). The range test runs once per
// distinct value; chunks outside the range are skipped by zone map.
func (c *ChunkedDictStringColumn[K]) FilterSelectionRange(lo, hi *string) *Selection {
	s := NewSelection(c.codes.len())
	keep := make([]bool, len(c.dict))
	any := false
	for code, value := range c.dict {
		in := (lo == nil || value >= *lo) && (hi == nil || value <= *hi)
		keep[code] = in
		any = any || in
	}
	if !any {
		return s
	}
	for ci := 0; ci < c.codes.numChunks(); ci++ {
		if !c.zones.mayContainRange(ci, lo, hi) {
			continue
		}
		base := uint32(ci) << c.codes.shift
		for j, cd := range c.codes.chunk(ci) {
			if keep[cd] {
				s.Add(base + uint32(j))
			}
		}
	}
	return s
}

// ChunkBounds returns chunk ci's zone-map bounds (min and max value). ok is
// false when the column was not finalized.
func (c *ChunkedDictStringColumn[K]) ChunkBounds(ci int) (min, max string, ok bool) {
	return c.zones.bounds(ci)
}

// FilterSelection returns the rows whose value satisfies the predicate as a
// bitmap. The predicate runs once per distinct value, not once per row; the
// codes are then scanned chunk by chunk.
func (c *ChunkedDictStringColumn[K]) FilterSelection(predicate func(string) bool) *Selection {
	keep := make([]bool, len(c.dict))
	for code, value := range c.dict {
		keep[code] = predicate(value)
	}
	s := NewSelection(c.codes.len())
	for ci := 0; ci < c.codes.numChunks(); ci++ {
		base := uint32(ci) << c.codes.shift
		for j, code := range c.codes.chunk(ci) {
			if keep[code] {
				s.Add(base + uint32(j))
			}
		}
	}
	return s
}

// Ranks returns, per dictionary code, the position of its value in sorted
// order. Built once and cached, it turns row comparison into an integer
// compare. Safe for concurrent callers.
func (c *ChunkedDictStringColumn[K]) Ranks() []K {
	c.ranksOnce.Do(func() {
		order := make([]int, len(c.dict))
		for i := range order {
			order[i] = i
		}
		sort.Slice(order, func(a, b int) bool { return c.dict[order[a]] < c.dict[order[b]] })

		ranks := make([]K, len(c.dict))
		for rank, code := range order {
			ranks[code] = K(rank)
		}
		c.ranks = ranks
	})
	return c.ranks
}

// GroupIndices buckets the given indices by dictionary code.
//
// Deprecated: membership-returning grouping is O(rows) in output and does not
// scale; use the narrow IGroupOps operations instead.
func (c *ChunkedDictStringColumn[K]) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	if len(indices) == 0 {
		return map[uint32][]uint32{}, nil
	}

	// Small subset of a high-cardinality column: the dense arrays below are
	// sized by the dictionary and would dwarf the input. Hash instead.
	if len(indices) < len(c.dict)/8 {
		grouped := make(map[uint32][]uint32)
		for _, i := range indices {
			code := uint32(c.codes.at(i))
			grouped[code] = append(grouped[code], i)
		}
		return grouped, nil
	}

	// Pass 1: count rows per code.
	counts := make([]uint32, len(c.dict))
	for _, i := range indices {
		counts[c.codes.at(i)]++
	}

	// Lay the groups out contiguously in one backing array.
	offsets := make([]uint32, len(c.dict))
	nonEmpty := 0
	var off uint32
	for code, n := range counts {
		offsets[code] = off
		off += n
		if n > 0 {
			nonEmpty++
		}
	}

	// Pass 2: scatter each index into its group's slot.
	backing := make([]uint32, len(indices))
	cursor := make([]uint32, len(c.dict))
	copy(cursor, offsets)
	for _, i := range indices {
		code := c.codes.at(i)
		backing[cursor[code]] = i
		cursor[code]++
	}

	grouped := make(map[uint32][]uint32, nonEmpty)
	for code, n := range counts {
		if n == 0 {
			continue
		}
		start := offsets[code]
		grouped[uint32(code)] = backing[start : start+n : start+n]
	}

	// Every row resolves to a code, so nothing is left unmapped.
	return grouped, nil
}

// --- IGroupOps ---
//
// Same structure as DictStringColumn: the dictionary code is the group code,
// so counting is a dense array pass; a small selection over a large
// dictionary falls back to hash grouping, with the cutover depending only on
// (column, selection) so code assignment stays consistent across operations.

func (c *ChunkedDictStringColumn[K]) smallGroupSubset(sel RowSet) bool {
	return sel.NumRows() < len(c.dict)/8
}

func (c *ChunkedDictStringColumn[K]) groupKeyAt(i uint32) (K, bool) {
	if i >= uint32(c.codes.len()) {
		return 0, false
	}
	return c.codes.at(i), true
}

func (c *ChunkedDictStringColumn[K]) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	if c.smallGroupSubset(sel) {
		return groupCountsByKey(sel, c.groupKeyAt)
	}
	counts := make([]uint32, len(c.dict))
	firsts := make([]uint32, len(c.dict))
	// Full universe: scan the code chunks directly instead of paying the
	// (chunkID, offset) split per row. This chunk-at-a-time loop is the shape
	// the parallel executor later runs per worker.
	if ar, ok := sel.(allRows); ok && int(ar) == c.codes.len() {
		for ci := 0; ci < c.codes.numChunks(); ci++ {
			base := uint32(ci) << c.codes.shift
			for j, code := range c.codes.chunk(ci) {
				if counts[code] == 0 {
					firsts[code] = base + uint32(j)
				}
				counts[code]++
			}
		}
		return counts, firsts
	}
	sel.ForEachRow(func(i uint32) bool {
		code := c.codes.at(i)
		if counts[code] == 0 {
			firsts[code] = i
		}
		counts[code]++
		return true
	})
	return counts, firsts
}

func (c *ChunkedDictStringColumn[K]) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	if c.smallGroupSubset(sel) {
		groupAggregatesByKey(sel, c.groupKeyAt, acc)
		return
	}
	sel.ForEachRow(func(i uint32) bool {
		acc.Add(uint32(c.codes.at(i)), i)
		return true
	})
}

func (c *ChunkedDictStringColumn[K]) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	if c.smallGroupSubset(sel) {
		return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
	}
	if n == 0 {
		return nil
	}
	var members []uint32
	skipped := 0
	sel.ForEachRow(func(i uint32) bool {
		if uint32(c.codes.at(i)) != code {
			return true
		}
		if skipped < offset {
			skipped++
			return true
		}
		members = append(members, i)
		return n < 0 || len(members) < n
	})
	return members
}

// --- IChunkedColumn ---

func (c *ChunkedDictStringColumn[K]) ChunkSize() int {
	return c.codes.chunkSize()
}

func (c *ChunkedDictStringColumn[K]) NumChunks() int {
	return c.codes.numChunks()
}

func (c *ChunkedDictStringColumn[K]) ChunkLen(chunk int) int {
	return c.codes.chunkLen(chunk)
}

// CodesChunk returns chunk c's codes for chunk-granularity kernels; resolve
// them through DictValue. The slice aliases the column's storage and must not
// be modified: columns are immutable after finalize.
func (c *ChunkedDictStringColumn[K]) CodesChunk(chunk int) []K {
	return c.codes.chunk(chunk)
}

// CompactChunkedStringColumn converts a loaded ChunkedStringColumn into a
// dictionary-encoded chunked column when the data is repetitive enough to
// benefit, choosing the narrowest code width that fits. Thresholds and the
// key-column short-circuit are the same as CompactStringColumn's; the result
// keeps the source's chunk size, so table-wide chunk alignment is preserved.
//
// It reports whether compaction happened; when it returns false the original
// column is returned unchanged.
func CompactChunkedStringColumn(c *ChunkedStringColumn) (IDataColumn, bool) {
	n := c.Length()
	if n < minDictRows {
		return c, false
	}
	// A primary key is the guaranteed-worst case: every value distinct. Decline
	// up front instead of burning a full counting scan.
	if c.IsKey() {
		return c, false
	}

	// Count distinct values, bailing out as soon as the column exceeds the
	// cardinality cap.
	seen := make(map[string]struct{}, 1024)
	for ci := 0; ci < c.data.numChunks(); ci++ {
		for _, v := range c.data.chunk(ci) {
			seen[v] = struct{}{}
			if len(seen) > maxDictCardinality {
				return c, false
			}
		}
	}

	if d := len(seen); d <= 1<<8 {
		return buildChunkedDict[uint8](c), true
	}
	return buildChunkedDict[uint16](c), true
}

func buildChunkedDict[K Unsigned](c *ChunkedStringColumn) *ChunkedDictStringColumn[K] {
	dc := newChunkedDictStringColumn[K](c.columnDef, c.data.chunkSize())
	for ci := 0; ci < c.data.numChunks(); ci++ {
		for _, v := range c.data.chunk(ci) {
			dc.Append(v)
		}
	}
	dc.FinalizeColumn()
	return dc
}

// Compile-time checks for both code widths.
var (
	_ IDataColumn          = (*ChunkedDictStringColumn[uint8])(nil)
	_ IDataColumnT[string] = (*ChunkedDictStringColumn[uint8])(nil)
	_ IGroupOps            = (*ChunkedDictStringColumn[uint8])(nil)
	_ IChunkedColumn       = (*ChunkedDictStringColumn[uint8])(nil)
	_ IDataColumn          = (*ChunkedDictStringColumn[uint16])(nil)
	_ IGroupOps            = (*ChunkedDictStringColumn[uint16])(nil)
	_ IChunkedColumn       = (*ChunkedDictStringColumn[uint16])(nil)
)
