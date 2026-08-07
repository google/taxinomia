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
	"cmp"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// chunkedColumn is the shared machinery of the chunked column types: values
// in fixed-size heap chunks (chunkedData), plus the per-type hooks that vary.
//
// K is the canonical grouping/uniqueness key for a value: identical to T for
// types whose == is the grouping equality, uint64 for float64 (all NaNs one
// group, -0 with +0 — see float64GroupKey). Uniqueness detection and reverse
// lookup use the same canonical key, so a chunked column's IsKey/GetIndex
// agree with its grouping semantics by construction.
type chunkedColumn[T any, K comparable] struct {
	columnDef *ColumnDef
	data      chunkedData[T]
	isKey     bool
	// valueIndex is the reverse-lookup map for key entity columns whose
	// storage is NOT sorted by this column. When sortedBySelf is recorded at
	// finalize the map is never built: reverse lookup is a binary search over
	// the chunks instead (docs/scaling-to-1b-rows.md §6 — a sparse index
	// replaces the map; at 10^9 rows the map is impossible).
	valueIndex   map[K]uint32
	sortedBySelf bool        // physical order == value order; recorded by FinalizeColumn
	zones        *zoneMap[T] // per-chunk min/max, built by FinalizeColumn

	canon  func(T) K
	format func(T) string
	// compare is the value ordering used by the zone maps.
	compare func(T, T) int
	// neverKey marks values that disqualify the column from being a key
	// regardless of uniqueness (NaN for floats, mirroring Float64Column).
	// The same values have no position in the ordering, so it doubles as the
	// zone maps' unordered test.
	neverKey func(T) bool
	// autoKey: whether FinalizeColumn detects uniqueness at all. False for
	// bool, mirroring BoolColumn.
	autoKey bool
}

func newChunkedColumn[T any, K comparable](
	columnDef *ColumnDef, chunkSize int,
	canon func(T) K, format func(T) string, compare func(T, T) int,
	neverKey func(T) bool, autoKey bool,
) chunkedColumn[T, K] {
	return chunkedColumn[T, K]{
		columnDef: columnDef,
		data:      newChunkedData[T](chunkSize),
		canon:     canon,
		format:    format,
		compare:   compare,
		neverKey:  neverKey,
		autoKey:   autoKey,
	}
}

func (c *chunkedColumn[T, K]) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *chunkedColumn[T, K]) Length() int {
	return c.data.len()
}

// Append adds a value. No Append after FinalizeColumn.
func (c *chunkedColumn[T, K]) Append(value T) {
	c.data.append(value)
}

func (c *chunkedColumn[T, K]) GetValue(i uint32) (T, error) {
	if i >= uint32(c.data.len()) {
		var zero T
		return zero, fmt.Errorf("index %d out of bounds (length: %d)", i, c.data.len())
	}
	return c.data.at(i), nil
}

func (c *chunkedColumn[T, K]) GetString(i uint32) (string, error) {
	if i >= uint32(c.data.len()) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, c.data.len())
	}
	return c.format(c.data.at(i)), nil
}

// GetIndex returns the row index holding the given value (key entity columns
// only). Lookup is by canonical key: a hash probe when the reverse-lookup map
// exists, a sparse-index binary search when the storage is sorted by this
// column — first over the chunks (one comparison per chunk boundary), then
// within the one chunk that can hold the value.
func (c *chunkedColumn[T, K]) GetIndex(v T) (uint32, error) {
	if c.valueIndex != nil {
		if idx, exists := c.valueIndex[c.canon(v)]; exists {
			return idx, nil
		}
		return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
	}
	if c.sortedBySelf && c.isKey && c.columnDef.EntityType() != "" && c.data.len() > 0 {
		// The last chunk whose first value is <= v is the only chunk that can
		// hold v: chunk-first values are the sparse index.
		ci := sort.Search(c.data.numChunks(), func(i int) bool {
			return c.compare(c.data.chunk(i)[0], v) > 0
		}) - 1
		if ci >= 0 {
			chunk := c.data.chunk(ci)
			j := sort.Search(len(chunk), func(j int) bool {
				return c.compare(chunk[j], v) >= 0
			})
			if j < len(chunk) && c.canon(chunk[j]) == c.canon(v) {
				return uint32(ci)<<c.data.shift + uint32(j), nil
			}
		}
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

func (c *chunkedColumn[T, K]) IsKey() bool {
	return c.isKey
}

// SortedBySelf reports whether the column's physical row order is its value
// order (non-decreasing). It is recorded by FinalizeColumn — after table-level
// sorting the leading sort-key column is finalized in sorted order — and is
// what lets a key column serve reverse lookups without a reverse-lookup map.
func (c *chunkedColumn[T, K]) SortedBySelf() bool {
	return c.sortedBySelf
}

// FinalizeColumn builds the per-chunk zone maps, then detects uniqueness (by
// canonical key) and the storage order. When the rows are in value order the
// sortedness is recorded and no reverse-lookup map is built — reverse lookup
// binary-searches the chunks instead, which is what makes a sorted primary
// key column viable at scale (the map is ~50 bytes per row; the search needs
// nothing). Only key entity columns whose storage is NOT sorted by this
// column keep the map, mirroring the plain columns. No Append after
// FinalizeColumn.
func (c *chunkedColumn[T, K]) FinalizeColumn() {
	if c.compare != nil {
		c.zones = buildZoneMap(&c.data, c.compare, c.neverKey)
		sorted, unique := c.scanOrder()
		c.sortedBySelf = sorted
		if sorted {
			c.valueIndex = nil
			if c.autoKey {
				c.isKey = unique
			}
			return
		}
	}
	if !c.autoKey {
		return
	}
	tempIndex := make(map[K]uint32)
	isUnique := true

scan:
	for ci := 0; ci < c.data.numChunks(); ci++ {
		base := uint32(ci) << c.data.shift
		for j, v := range c.data.chunk(ci) {
			if c.neverKey != nil && c.neverKey(v) {
				isUnique = false
				break scan
			}
			k := c.canon(v)
			if _, exists := tempIndex[k]; exists {
				isUnique = false
				break scan
			}
			tempIndex[k] = base + uint32(j)
		}
	}

	c.isKey = isUnique
	if isUnique && c.columnDef.EntityType() != "" {
		c.valueIndex = tempIndex
	} else {
		c.valueIndex = nil
	}
}

// scanOrder makes one comparison pass over the values: sorted reports whether
// they are non-decreasing in the column's value ordering, unique whether they
// are pairwise distinct by canonical key and free of key-disqualifying values
// (NaN). unique is only meaningful when sorted is true — for sorted data
// canon-equal values are adjacent (compare returns 0 exactly when the
// canonical keys are equal: -0 orders with +0, all NaNs order together), so
// adjacent comparisons see every duplicate. The pass allocates nothing and
// exits at the first order violation.
func (c *chunkedColumn[T, K]) scanOrder() (sorted, unique bool) {
	unique = true
	var prev T
	first := true
	for ci := 0; ci < c.data.numChunks(); ci++ {
		for _, v := range c.data.chunk(ci) {
			if c.neverKey != nil && c.neverKey(v) {
				unique = false
			}
			if !first {
				switch cc := c.compare(prev, v); {
				case cc > 0:
					return false, false
				case cc == 0:
					unique = false
				}
			}
			prev, first = v, false
		}
	}
	return true, unique
}

// FilterSelection returns the rows whose value satisfies the predicate as a
// bitmap, scanning chunk by chunk. A chunk's bit range is word-aligned
// whenever the chunk size is a multiple of 64, which every public constructor
// guarantees — that is what lets the executor pool fill the same bitmap from
// concurrent per-chunk scans without locking. The predicate must be safe for
// concurrent calls (a pure function of its argument, as every in-repo
// predicate is).
func (c *chunkedColumn[T, K]) FilterSelection(predicate func(T) bool) *Selection {
	s, _ := c.FilterSelectionContext(context.Background(), predicate)
	return s
}

// FilterSelectionContext is FilterSelection under a context: chunks are
// scanned in parallel on the executor pool, no new chunk is started once ctx
// is cancelled, and a cancelled scan returns (nil, ctx.Err()) —
// docs/scaling-to-1b-rows.md §3, "everything is cancellable".
func (c *chunkedColumn[T, K]) FilterSelectionContext(ctx context.Context, predicate func(T) bool) (*Selection, error) {
	s := NewSelection(c.data.len())
	err := forEachChunk(ctx, c.data.numChunks(), c.data.chunkSize(), func(ci int) {
		base := uint32(ci) << c.data.shift
		for j, v := range c.data.chunk(ci) {
			if predicate(v) {
				s.Add(base + uint32(j))
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// FilterSelectionEqual returns the rows whose value equals v, equality being
// the column's grouping equality (for floats: NaN matches NaN, -0 matches
// +0). Chunks whose zone map proves they cannot contain v are skipped without
// touching a row — on data sorted by this column that prunes the scan to the
// chunks actually holding v (docs/scaling-to-1b-rows.md §3.A).
//
// On a column that was never finalized there are no zone maps and every chunk
// is scanned; the result is identical either way.
func (c *chunkedColumn[T, K]) FilterSelectionEqual(v T) *Selection {
	s, _ := c.FilterSelectionEqualContext(context.Background(), v)
	return s
}

// FilterSelectionEqualContext is FilterSelectionEqual under a context, with
// the surviving chunks scanned in parallel on the executor pool; a cancelled
// scan returns (nil, ctx.Err()).
func (c *chunkedColumn[T, K]) FilterSelectionEqualContext(ctx context.Context, v T) (*Selection, error) {
	s := NewSelection(c.data.len())
	key := c.canon(v)
	err := forEachChunk(ctx, c.data.numChunks(), c.data.chunkSize(), func(ci int) {
		if !c.zones.mayContainPoint(ci, v) {
			return
		}
		base := uint32(ci) << c.data.shift
		for j, x := range c.data.chunk(ci) {
			if c.canon(x) == key {
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
// values (the multi-value OR filter), with the same chunk pruning and
// equality semantics as FilterSelectionEqual.
func (c *chunkedColumn[T, K]) FilterSelectionIn(values []T) *Selection {
	s, _ := c.FilterSelectionInContext(context.Background(), values)
	return s
}

// FilterSelectionInContext is FilterSelectionIn under a context, with the
// surviving chunks scanned in parallel on the executor pool; a cancelled
// scan returns (nil, ctx.Err()).
func (c *chunkedColumn[T, K]) FilterSelectionInContext(ctx context.Context, values []T) (*Selection, error) {
	s := NewSelection(c.data.len())
	if len(values) == 0 {
		return s, nil
	}
	keys := make(map[K]struct{}, len(values))
	for _, v := range values {
		keys[c.canon(v)] = struct{}{}
	}
	err := forEachChunk(ctx, c.data.numChunks(), c.data.chunkSize(), func(ci int) {
		if !c.zones.mayContainAny(ci, values) {
			return
		}
		base := uint32(ci) << c.data.shift
		for j, x := range c.data.chunk(ci) {
			if _, ok := keys[c.canon(x)]; ok {
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
// (inclusive; a nil bound is unbounded), with chunk pruning. Values outside
// the ordering (NaN) never match a range.
func (c *chunkedColumn[T, K]) FilterSelectionRange(lo, hi *T) *Selection {
	s, _ := c.FilterSelectionRangeContext(context.Background(), lo, hi)
	return s
}

// FilterSelectionRangeContext is FilterSelectionRange under a context, with
// the surviving chunks scanned in parallel on the executor pool; a cancelled
// scan returns (nil, ctx.Err()).
func (c *chunkedColumn[T, K]) FilterSelectionRangeContext(ctx context.Context, lo, hi *T) (*Selection, error) {
	s := NewSelection(c.data.len())
	err := forEachChunk(ctx, c.data.numChunks(), c.data.chunkSize(), func(ci int) {
		if !c.zones.mayContainRange(ci, lo, hi) {
			return
		}
		base := uint32(ci) << c.data.shift
		for j, x := range c.data.chunk(ci) {
			if c.neverKey != nil && c.neverKey(x) {
				continue
			}
			if lo != nil && c.compare(x, *lo) < 0 {
				continue
			}
			if hi != nil && c.compare(x, *hi) > 0 {
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

// ChunkBounds returns chunk ci's zone-map bounds. ok is false when no zone
// map exists (the column was not finalized) or the chunk holds no ordered
// values (all NaN). The native file format serializes these; engines prune
// with the FilterSelection* methods instead of reading bounds directly.
func (c *chunkedColumn[T, K]) ChunkBounds(ci int) (min, max T, ok bool) {
	return c.zones.bounds(ci)
}

// GroupIndices buckets the given indices by value, keyed in order of first
// appearance. It exists to satisfy IDataColumn; new callers use the IGroupOps
// operations, which never materialise membership.
//
// Deprecated: membership-returning grouping is O(rows) in output and does not
// scale; use the narrow IGroupOps operations instead.
func (c *chunkedColumn[T, K]) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := map[uint32][]uint32{}
	valueToGroupKey := map[K]uint32{}
	for _, i := range indices {
		if i >= uint32(c.data.len()) {
			continue
		}
		k := c.canon(c.data.at(i))
		groupKey, ok := valueToGroupKey[k]
		if !ok {
			groupKey = uint32(len(valueToGroupKey))
			valueToGroupKey[k] = groupKey
		}
		groupedIndices[groupKey] = append(groupedIndices[groupKey], i)
	}
	return groupedIndices, nil
}

// CompareRows compares the values at rows i and j in the column's value
// ordering, satisfying RowComparator so sorts use value order rather than the
// formatted-string fallback. Values outside the ordering (NaN) sort after
// everything, matching compareFloat64s on the plain column.
func (c *chunkedColumn[T, K]) CompareRows(i, j uint32) int {
	a, b := c.data.at(i), c.data.at(j)
	if c.neverKey != nil {
		aOut, bOut := c.neverKey(a), c.neverKey(b)
		switch {
		case aOut && bOut:
			return 0
		case aOut:
			return 1
		case bOut:
			return -1
		}
	}
	return c.compare(a, b)
}

// --- IGroupOps ---

func (c *chunkedColumn[T, K]) groupKeyAt(i uint32) (K, bool) {
	if i >= uint32(c.data.len()) {
		var zero K
		return zero, false
	}
	return c.canon(c.data.at(i)), true
}

func (c *chunkedColumn[T, K]) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *chunkedColumn[T, K]) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *chunkedColumn[T, K]) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

// partitionGroups implements groupPartitioner: the grouping pass as per-chunk
// partials on the executor pool, with the same first-appearance code
// assignment as the sequential IGroupOps operations.
func (c *chunkedColumn[T, K]) partitionGroups(ctx context.Context, sel RowSet) (*GroupPartition, bool, error) {
	rs, ok := sel.(rangeRowSet)
	nc := c.data.numChunks()
	chunkSize := c.data.chunkSize()
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

func (c *chunkedColumn[T, K]) ChunkSize() int {
	return c.data.chunkSize()
}

func (c *chunkedColumn[T, K]) NumChunks() int {
	return c.data.numChunks()
}

func (c *chunkedColumn[T, K]) ChunkLen(chunk int) int {
	return c.data.chunkLen(chunk)
}

// Chunk returns chunk c's values for chunk-granularity kernels (zone-map
// construction, per-chunk partials). The slice aliases the column's storage
// and must not be modified: columns are immutable after finalize.
func (c *chunkedColumn[T, K]) Chunk(chunk int) []T {
	return c.data.chunk(chunk)
}

// --- per-type hooks ---

func identityKey[T comparable](v T) T { return v }

// compareBool orders false before true, matching the sort order of the
// formatted values ("False" < "True").
func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

func formatString(v string) string { return v }

func formatBool(v bool) string {
	if v {
		return "True"
	}
	return "False"
}

func formatInt64(v int64) string   { return strconv.FormatInt(v, 10) }
func formatUint64(v uint64) string { return strconv.FormatUint(v, 10) }
func formatUint32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

// --- the chunked column types ---
//
// Chunked counterparts of the plain storage columns: same column surface
// (IDataColumn, typed access, FilterSelection, IGroupOps), values held in
// fixed-size heap chunks (IChunkedColumn). High-cardinality strings and
// per-role encodings are later phases; ChunkedStringColumn stores plain Go
// strings exactly as StringColumn does.
//
// Datetime and duration counterparts are deliberately absent until a loader
// needs them: they carry per-column display state (location, format) that
// the loader phase decides how to thread.

// ChunkedStringColumn is StringColumn with chunked storage.
type ChunkedStringColumn struct {
	chunkedColumn[string, string]
}

// NewChunkedStringColumn creates an empty chunked string column.
func NewChunkedStringColumn(columnDef *ColumnDef) *ChunkedStringColumn {
	return newChunkedStringColumn(columnDef, DefaultChunkSize)
}

func newChunkedStringColumn(columnDef *ColumnDef, chunkSize int) *ChunkedStringColumn {
	return &ChunkedStringColumn{newChunkedColumn[string, string](
		columnDef, chunkSize, identityKey[string], formatString, strings.Compare, nil, true)}
}

func (c *ChunkedStringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedStringColumn(columnDef, joiner, c)
}

// ChunkedBoolColumn is BoolColumn with chunked storage.
type ChunkedBoolColumn struct {
	chunkedColumn[bool, bool]
}

// NewChunkedBoolColumn creates an empty chunked bool column.
func NewChunkedBoolColumn(columnDef *ColumnDef) *ChunkedBoolColumn {
	return newChunkedBoolColumn(columnDef, DefaultChunkSize)
}

func newChunkedBoolColumn(columnDef *ColumnDef, chunkSize int) *ChunkedBoolColumn {
	return &ChunkedBoolColumn{newChunkedColumn[bool, bool](
		columnDef, chunkSize, identityKey[bool], formatBool, compareBool, nil, false)}
}

func (c *ChunkedBoolColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedBoolColumn(columnDef, joiner, c)
}

// ChunkedInt64Column is Int64Column with chunked storage.
type ChunkedInt64Column struct {
	chunkedColumn[int64, int64]
}

// NewChunkedInt64Column creates an empty chunked int64 column.
func NewChunkedInt64Column(columnDef *ColumnDef) *ChunkedInt64Column {
	return newChunkedInt64Column(columnDef, DefaultChunkSize)
}

func newChunkedInt64Column(columnDef *ColumnDef, chunkSize int) *ChunkedInt64Column {
	return &ChunkedInt64Column{newChunkedColumn[int64, int64](
		columnDef, chunkSize, identityKey[int64], formatInt64, cmp.Compare[int64], nil, true)}
}

func (c *ChunkedInt64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedInt64Column(columnDef, joiner, c)
}

// ChunkedUint64Column is Uint64Column with chunked storage.
type ChunkedUint64Column struct {
	chunkedColumn[uint64, uint64]
}

// NewChunkedUint64Column creates an empty chunked uint64 column.
func NewChunkedUint64Column(columnDef *ColumnDef) *ChunkedUint64Column {
	return newChunkedUint64Column(columnDef, DefaultChunkSize)
}

func newChunkedUint64Column(columnDef *ColumnDef, chunkSize int) *ChunkedUint64Column {
	return &ChunkedUint64Column{newChunkedColumn[uint64, uint64](
		columnDef, chunkSize, identityKey[uint64], formatUint64, cmp.Compare[uint64], nil, true)}
}

func (c *ChunkedUint64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedUint64Column(columnDef, joiner, c)
}

// ChunkedUint32Column is Uint32Column with chunked storage.
type ChunkedUint32Column struct {
	chunkedColumn[uint32, uint32]
}

// NewChunkedUint32Column creates an empty chunked uint32 column.
func NewChunkedUint32Column(columnDef *ColumnDef) *ChunkedUint32Column {
	return newChunkedUint32Column(columnDef, DefaultChunkSize)
}

func newChunkedUint32Column(columnDef *ColumnDef, chunkSize int) *ChunkedUint32Column {
	return &ChunkedUint32Column{newChunkedColumn[uint32, uint32](
		columnDef, chunkSize, identityKey[uint32], formatUint32, cmp.Compare[uint32], nil, true)}
}

func (c *ChunkedUint32Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedUint32Column(columnDef, joiner, c)
}

// ChunkedFloat64Column is Float64Column with chunked storage. Grouping,
// uniqueness and reverse lookup use the canonical float key (all NaNs equal,
// -0 equal to +0); a column containing NaN is never a key, mirroring
// Float64Column.
type ChunkedFloat64Column struct {
	chunkedColumn[float64, uint64]
}

// NewChunkedFloat64Column creates an empty chunked float64 column.
func NewChunkedFloat64Column(columnDef *ColumnDef) *ChunkedFloat64Column {
	return newChunkedFloat64Column(columnDef, DefaultChunkSize)
}

func newChunkedFloat64Column(columnDef *ColumnDef, chunkSize int) *ChunkedFloat64Column {
	return &ChunkedFloat64Column{newChunkedColumn[float64, uint64](
		columnDef, chunkSize, float64GroupKey, FormatFloat64, cmp.Compare[float64], math.IsNaN, true)}
}

func (c *ChunkedFloat64Column) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedFloat64Column(columnDef, joiner, c)
}

// Compile-time checks: every chunked column type carries the full column
// surface plus chunk access.
var (
	_ IDataColumn          = (*ChunkedStringColumn)(nil)
	_ IDataColumnT[string] = (*ChunkedStringColumn)(nil)
	_ IGroupOps            = (*ChunkedStringColumn)(nil)
	_ IChunkedColumn       = (*ChunkedStringColumn)(nil)

	_ IDataColumn        = (*ChunkedBoolColumn)(nil)
	_ IDataColumnT[bool] = (*ChunkedBoolColumn)(nil)
	_ IGroupOps          = (*ChunkedBoolColumn)(nil)
	_ IChunkedColumn     = (*ChunkedBoolColumn)(nil)

	_ IDataColumn         = (*ChunkedInt64Column)(nil)
	_ IDataColumnT[int64] = (*ChunkedInt64Column)(nil)
	_ IGroupOps           = (*ChunkedInt64Column)(nil)
	_ IChunkedColumn      = (*ChunkedInt64Column)(nil)

	_ IDataColumn          = (*ChunkedUint64Column)(nil)
	_ IDataColumnT[uint64] = (*ChunkedUint64Column)(nil)
	_ IGroupOps            = (*ChunkedUint64Column)(nil)
	_ IChunkedColumn       = (*ChunkedUint64Column)(nil)

	_ IDataColumn          = (*ChunkedUint32Column)(nil)
	_ IDataColumnT[uint32] = (*ChunkedUint32Column)(nil)
	_ IGroupOps            = (*ChunkedUint32Column)(nil)
	_ IChunkedColumn       = (*ChunkedUint32Column)(nil)

	_ IDataColumn           = (*ChunkedFloat64Column)(nil)
	_ IDataColumnT[float64] = (*ChunkedFloat64Column)(nil)
	_ IGroupOps             = (*ChunkedFloat64Column)(nil)
	_ IChunkedColumn        = (*ChunkedFloat64Column)(nil)

	// Sorts compare chunked columns by value, not by formatted string.
	_ RowComparator = (*ChunkedStringColumn)(nil)
	_ RowComparator = (*ChunkedFloat64Column)(nil)
	_ RowComparator = (*ChunkedDatetimeColumn)(nil)
	_ RowComparator = (*ChunkedDictStringColumn[uint16])(nil)
)
