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
	"context"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"unsafe"
)

// arenaString returns the string view of b without copying. This is safe
// because arena blobs are append-only: bytes once written are never modified
// (columns are immutable after finalize, and Append only ever extends the
// blob past the previously written length), so the returned string is as
// immutable as any other Go string.
func arenaString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// arenaChunk is one row group of arena string storage: the strings' bytes
// concatenated in one blob, row j at blob[offsets[j]:offsets[j+1]]. Two flat,
// pointer-free arrays per chunk — the collector never traces a string header
// per row, which is the point (docs/scaling-to-1b-rows.md §5, §9). The uint32
// offsets cap a chunk's blob at 4 GiB, an average of 64 KiB per row at the
// default chunk size.
type arenaChunk struct {
	blob    []byte
	offsets []uint32 // len = rows+1, offsets[0] == 0
}

func (ch *arenaChunk) str(j uint32) string {
	return arenaString(ch.blob[ch.offsets[j]:ch.offsets[j+1]])
}

func (ch *arenaChunk) rows() int {
	return len(ch.offsets) - 1
}

// ChunkedArenaStringColumn stores strings in per-chunk byte arenas: one blob
// plus one offsets array per chunk instead of a Go string header per row
// (docs/scaling-to-1b-rows.md §5 — the high-cardinality fallback is never
// []string; the choice is dictionary versus arena). The column surface is
// that of ChunkedStringColumn: same grouping, filtering, key detection and
// sparse-index reverse lookup semantics, with reads served zero-copy as views
// into the blob.
type ChunkedArenaStringColumn struct {
	columnDef *ColumnDef
	shift     uint32 // log2(chunk size)
	mask      uint32 // chunk size - 1
	chunks    []arenaChunk
	n         int
	isKey     bool
	// valueIndex is the reverse-lookup map for key entity columns whose
	// storage is not sorted by this column; its keys alias the blobs. As in
	// chunkedColumn, a column recorded sortedBySelf never builds it — reverse
	// lookup binary-searches the chunks instead (§6).
	valueIndex   map[string]uint32
	sortedBySelf bool
	zones        *zoneMap[string]
}

// NewChunkedArenaStringColumn creates an empty arena-backed string column.
func NewChunkedArenaStringColumn(columnDef *ColumnDef) *ChunkedArenaStringColumn {
	return newChunkedArenaStringColumn(columnDef, DefaultChunkSize)
}

func newChunkedArenaStringColumn(columnDef *ColumnDef, chunkSize int) *ChunkedArenaStringColumn {
	if chunkSize <= 0 || chunkSize&(chunkSize-1) != 0 {
		panic(fmt.Sprintf("columns: chunk size %d is not a power of two", chunkSize))
	}
	return &ChunkedArenaStringColumn{
		columnDef: columnDef,
		shift:     uint32(bits.TrailingZeros(uint(chunkSize))),
		mask:      uint32(chunkSize - 1),
	}
}

func (c *ChunkedArenaStringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ChunkedArenaStringColumn) Length() int {
	return c.n
}

// Append adds a value, copying its bytes into the current chunk's blob. No
// Append after FinalizeColumn.
func (c *ChunkedArenaStringColumn) Append(value string) {
	if len(c.chunks) == 0 || c.chunks[len(c.chunks)-1].rows() == c.chunkSize() {
		c.chunks = append(c.chunks, arenaChunk{offsets: make([]uint32, 1, c.chunkSize()+1)})
	}
	ch := &c.chunks[len(c.chunks)-1]
	ch.blob = append(ch.blob, value...)
	ch.offsets = append(ch.offsets, uint32(len(ch.blob)))
	c.n++
}

func (c *ChunkedArenaStringColumn) chunkSize() int {
	return 1 << c.shift
}

// value returns the string at global row i without copying. The caller has
// already bounds-checked i.
func (c *ChunkedArenaStringColumn) value(i uint32) string {
	return c.chunks[i>>c.shift].str(i & c.mask)
}

func (c *ChunkedArenaStringColumn) GetValue(i uint32) (string, error) {
	if i >= uint32(c.n) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, c.n)
	}
	return c.value(i), nil
}

// GetString returns the string value at index i. No formatting, no copy.
func (c *ChunkedArenaStringColumn) GetString(i uint32) (string, error) {
	return c.GetValue(i)
}

// GetIndex returns the row index holding the given value (key entity columns
// only): a map probe on unsorted storage, a sparse-index binary search — over
// the chunk-first values, then within the one candidate chunk — when the
// storage is sorted by this column.
func (c *ChunkedArenaStringColumn) GetIndex(v string) (uint32, error) {
	if c.valueIndex != nil {
		if idx, exists := c.valueIndex[v]; exists {
			return idx, nil
		}
		return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
	}
	if c.sortedBySelf && c.isKey && c.columnDef.EntityType() != "" && c.n > 0 {
		ci := sort.Search(len(c.chunks), func(i int) bool {
			return c.chunks[i].str(0) > v
		}) - 1
		if ci >= 0 {
			ch := &c.chunks[ci]
			rows := ch.rows()
			j := sort.Search(rows, func(j int) bool {
				return ch.str(uint32(j)) >= v
			})
			if j < rows && ch.str(uint32(j)) == v {
				return uint32(ci)<<c.shift + uint32(j), nil
			}
		}
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

func (c *ChunkedArenaStringColumn) IsKey() bool {
	return c.isKey
}

// SortedBySelf reports whether the column's physical row order is its value
// order (non-decreasing), recorded by FinalizeColumn.
func (c *ChunkedArenaStringColumn) SortedBySelf() bool {
	return c.sortedBySelf
}

func (c *ChunkedArenaStringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedStringColumn(columnDef, joiner, c)
}

// FinalizeColumn builds the per-chunk zone maps, then detects uniqueness and
// the storage order, with the same outcomes as chunkedColumn: sorted storage
// records sortedBySelf and never builds a reverse-lookup map; only key entity
// columns on unsorted storage keep one (its keys alias the blobs, so it costs
// map overhead but no extra string content). No Append after FinalizeColumn.
func (c *ChunkedArenaStringColumn) FinalizeColumn() {
	c.zones = c.buildZones()
	sorted, unique := c.scanOrder()
	c.sortedBySelf = sorted
	if sorted {
		c.valueIndex = nil
		c.isKey = unique
		return
	}
	tempIndex := make(map[string]uint32)
	isUnique := true
scan:
	for ci := range c.chunks {
		base := uint32(ci) << c.shift
		for j := 0; j < c.chunks[ci].rows(); j++ {
			v := c.chunks[ci].str(uint32(j))
			if _, exists := tempIndex[v]; exists {
				isUnique = false
				break scan
			}
			tempIndex[v] = base + uint32(j)
		}
	}
	c.isKey = isUnique
	if isUnique && c.columnDef.EntityType() != "" {
		c.valueIndex = tempIndex
	} else {
		c.valueIndex = nil
	}
}

// scanOrder makes one comparison pass: sorted reports whether the values are
// non-decreasing, unique whether they are pairwise distinct. As in
// chunkedColumn, unique is only meaningful when sorted is true.
func (c *ChunkedArenaStringColumn) scanOrder() (sorted, unique bool) {
	unique = true
	var prev string
	first := true
	for ci := range c.chunks {
		for j := 0; j < c.chunks[ci].rows(); j++ {
			v := c.chunks[ci].str(uint32(j))
			if !first {
				switch {
				case prev > v:
					return false, false
				case prev == v:
					unique = false
				}
			}
			prev, first = v, false
		}
	}
	return true, unique
}

func (c *ChunkedArenaStringColumn) buildZones() *zoneMap[string] {
	n := len(c.chunks)
	z := &zoneMap[string]{
		cmp:       strings.Compare,
		mins:      make([]string, n),
		maxs:      make([]string, n),
		hasBounds: make([]bool, n),
	}
	for ci := range c.chunks {
		for j := 0; j < c.chunks[ci].rows(); j++ {
			v := c.chunks[ci].str(uint32(j))
			if !z.hasBounds[ci] {
				z.mins[ci], z.maxs[ci] = v, v
				z.hasBounds[ci] = true
				continue
			}
			if v < z.mins[ci] {
				z.mins[ci] = v
			} else if v > z.maxs[ci] {
				z.maxs[ci] = v
			}
		}
	}
	return z
}

// FilterSelection returns the rows whose value satisfies the predicate as a
// bitmap, scanning chunk by chunk. Values are passed to the predicate as
// zero-copy views into the arena.
func (c *ChunkedArenaStringColumn) FilterSelection(predicate func(string) bool) *Selection {
	s, _ := c.FilterSelectionContext(context.Background(), predicate)
	return s
}

// FilterSelectionContext is FilterSelection under a context: chunks are
// scanned in parallel on the executor pool. The predicate must be safe for
// concurrent calls. A cancelled scan returns (nil, ctx.Err()).
func (c *ChunkedArenaStringColumn) FilterSelectionContext(ctx context.Context, predicate func(string) bool) (*Selection, error) {
	s := NewSelection(c.n)
	err := forEachChunk(ctx, len(c.chunks), 1<<c.shift, func(ci int) {
		base := uint32(ci) << c.shift
		for j := 0; j < c.chunks[ci].rows(); j++ {
			if predicate(c.chunks[ci].str(uint32(j))) {
				s.Add(base + uint32(j))
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// FilterSelectionEqual returns the rows whose value is exactly v, skipping
// chunks whose zone map rules v out.
func (c *ChunkedArenaStringColumn) FilterSelectionEqual(v string) *Selection {
	s, _ := c.FilterSelectionEqualContext(context.Background(), v)
	return s
}

// FilterSelectionEqualContext is FilterSelectionEqual under a context, with
// the surviving chunks scanned in parallel on the executor pool; a cancelled
// scan returns (nil, ctx.Err()).
func (c *ChunkedArenaStringColumn) FilterSelectionEqualContext(ctx context.Context, v string) (*Selection, error) {
	s := NewSelection(c.n)
	err := forEachChunk(ctx, len(c.chunks), 1<<c.shift, func(ci int) {
		if !c.zones.mayContainPoint(ci, v) {
			return
		}
		base := uint32(ci) << c.shift
		for j := 0; j < c.chunks[ci].rows(); j++ {
			if c.chunks[ci].str(uint32(j)) == v {
				s.Add(base + uint32(j))
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// FilterSelectionIn returns the rows whose value equals any of the given
// values (the multi-value OR filter), with the same chunk pruning as
// FilterSelectionEqual.
func (c *ChunkedArenaStringColumn) FilterSelectionIn(values []string) *Selection {
	s, _ := c.FilterSelectionInContext(context.Background(), values)
	return s
}

// FilterSelectionInContext is FilterSelectionIn under a context, with the
// surviving chunks scanned in parallel on the executor pool; a cancelled
// scan returns (nil, ctx.Err()).
func (c *ChunkedArenaStringColumn) FilterSelectionInContext(ctx context.Context, values []string) (*Selection, error) {
	s := NewSelection(c.n)
	if len(values) == 0 {
		return s, nil
	}
	keys := make(map[string]struct{}, len(values))
	for _, v := range values {
		keys[v] = struct{}{}
	}
	err := forEachChunk(ctx, len(c.chunks), 1<<c.shift, func(ci int) {
		if !c.zones.mayContainAny(ci, values) {
			return
		}
		base := uint32(ci) << c.shift
		for j := 0; j < c.chunks[ci].rows(); j++ {
			if _, ok := keys[c.chunks[ci].str(uint32(j))]; ok {
				s.Add(base + uint32(j))
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// FilterSelectionRange returns the rows whose value lies in [lo, hi]
// (inclusive; a nil bound is unbounded), with chunk pruning.
func (c *ChunkedArenaStringColumn) FilterSelectionRange(lo, hi *string) *Selection {
	s, _ := c.FilterSelectionRangeContext(context.Background(), lo, hi)
	return s
}

// FilterSelectionRangeContext is FilterSelectionRange under a context, with
// the surviving chunks scanned in parallel on the executor pool; a cancelled
// scan returns (nil, ctx.Err()).
func (c *ChunkedArenaStringColumn) FilterSelectionRangeContext(ctx context.Context, lo, hi *string) (*Selection, error) {
	s := NewSelection(c.n)
	err := forEachChunk(ctx, len(c.chunks), 1<<c.shift, func(ci int) {
		if !c.zones.mayContainRange(ci, lo, hi) {
			return
		}
		base := uint32(ci) << c.shift
		for j := 0; j < c.chunks[ci].rows(); j++ {
			v := c.chunks[ci].str(uint32(j))
			if lo != nil && v < *lo {
				continue
			}
			if hi != nil && v > *hi {
				continue
			}
			s.Add(base + uint32(j))
		}
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// ChunkBounds returns chunk ci's zone-map bounds (min and max value). ok is
// false when the column was not finalized.
func (c *ChunkedArenaStringColumn) ChunkBounds(ci int) (min, max string, ok bool) {
	return c.zones.bounds(ci)
}

// GroupIndices buckets the given indices by value, keyed in order of first
// appearance.
//
// Deprecated: membership-returning grouping is O(rows) in output and does not
// scale; use the narrow IGroupOps operations instead.
func (c *ChunkedArenaStringColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[string]uint32{}
	for _, i := range indices {
		if i >= uint32(c.n) {
			continue
		}
		v := c.value(i)
		groupKey, ok := valueToGroupKey[v]
		if !ok {
			groupKey = uint32(len(valueToGroupKey))
			valueToGroupKey[v] = groupKey
		}
		groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
	}
	return groupedIndices, nil
}

// CompareRows compares the values at rows i and j, satisfying RowComparator.
func (c *ChunkedArenaStringColumn) CompareRows(i, j uint32) int {
	return CompareStrings(c.columnDef.collation, c.value(i), c.value(j))
}

// --- IGroupOps ---

func (c *ChunkedArenaStringColumn) groupKeyAt(i uint32) (string, bool) {
	if i >= uint32(c.n) {
		return "", false
	}
	return c.value(i), true
}

func (c *ChunkedArenaStringColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ChunkedArenaStringColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ChunkedArenaStringColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

// partitionGroups implements groupPartitioner: the grouping pass as per-chunk
// partials on the executor pool. The partial maps key zero-copy views into the
// blobs, which is safe transiently for the same reason valueIndex's keys are.
func (c *ChunkedArenaStringColumn) partitionGroups(ctx context.Context, sel RowSet) (*GroupPartition, bool, error) {
	rs, ok := sel.(rangeRowSet)
	nc := len(c.chunks)
	chunkSize := 1 << c.shift
	if !ok || nc < 2 || chunkSize%64 != 0 || disableParallelScan {
		return nil, false, nil
	}
	part, err := parallelHashPartition(ctx, nc, chunkSize, c.groupKeyAt, rs)
	if err != nil {
		return nil, false, err
	}
	return part, true, nil
}

// --- IChunkedColumn ---

func (c *ChunkedArenaStringColumn) ChunkSize() int {
	return c.chunkSize()
}

func (c *ChunkedArenaStringColumn) NumChunks() int {
	return len(c.chunks)
}

func (c *ChunkedArenaStringColumn) ChunkLen(chunk int) int {
	return c.chunks[chunk].rows()
}

// --- Reorderable ---

// Reorder returns a finalized arena copy with rows permuted, like the other
// chunked columns.
func (c *ChunkedArenaStringColumn) Reorder(perm []uint32) IDataColumn {
	checkPermLength(c.columnDef.Name(), len(perm), c.n)
	n := newChunkedArenaStringColumn(c.columnDef, c.chunkSize())
	for _, p := range perm {
		n.Append(c.value(p))
	}
	n.FinalizeColumn()
	return n
}

// ArenaEncodeChunkedStringColumn copies a loaded ChunkedStringColumn into
// arena storage, preserving the chunk size and the source's state: a
// finalized source yields a finalized arena column (zone maps, key detection,
// sparse index or reverse-lookup map — recomputed over the identical data), a
// never-finalized source yields a never-finalized arena column, so encoding
// selection changes the representation and nothing else.
func ArenaEncodeChunkedStringColumn(src *ChunkedStringColumn) *ChunkedArenaStringColumn {
	dst := newChunkedArenaStringColumn(src.columnDef, src.data.chunkSize())
	for ci := 0; ci < src.data.numChunks(); ci++ {
		for _, v := range src.data.chunk(ci) {
			dst.Append(v)
		}
	}
	if src.zones != nil {
		dst.FinalizeColumn()
	}
	return dst
}

// Compile-time checks: the arena column carries the full column surface.
var (
	_ IDataColumn          = (*ChunkedArenaStringColumn)(nil)
	_ IDataColumnT[string] = (*ChunkedArenaStringColumn)(nil)
	_ IGroupOps            = (*ChunkedArenaStringColumn)(nil)
	_ IChunkedColumn       = (*ChunkedArenaStringColumn)(nil)
	_ RowComparator        = (*ChunkedArenaStringColumn)(nil)
	_ Reorderable          = (*ChunkedArenaStringColumn)(nil)
)
