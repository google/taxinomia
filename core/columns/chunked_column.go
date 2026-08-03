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
	"math"
	"strconv"
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
	columnDef  *ColumnDef
	data       chunkedData[T]
	isKey      bool
	valueIndex map[K]uint32 // canon(value) -> row, key entity columns only

	canon  func(T) K
	format func(T) string
	// neverKey marks values that disqualify the column from being a key
	// regardless of uniqueness (NaN for floats, mirroring Float64Column).
	neverKey func(T) bool
	// autoKey: whether FinalizeColumn detects uniqueness at all. False for
	// bool, mirroring BoolColumn.
	autoKey bool
}

func newChunkedColumn[T any, K comparable](
	columnDef *ColumnDef, chunkSize int,
	canon func(T) K, format func(T) string, neverKey func(T) bool, autoKey bool,
) chunkedColumn[T, K] {
	return chunkedColumn[T, K]{
		columnDef: columnDef,
		data:      newChunkedData[T](chunkSize),
		canon:     canon,
		format:    format,
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
// only). Lookup is by canonical key.
func (c *chunkedColumn[T, K]) GetIndex(v T) (uint32, error) {
	if idx, exists := c.valueIndex[c.canon(v)]; exists {
		return idx, nil
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

func (c *chunkedColumn[T, K]) IsKey() bool {
	return c.isKey
}

// FinalizeColumn detects uniqueness (by canonical key) and keeps the reverse
// index when the column is a unique entity column, mirroring the plain
// columns. No Append after FinalizeColumn.
func (c *chunkedColumn[T, K]) FinalizeColumn() {
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

// FilterSelection returns the rows whose value satisfies the predicate as a
// bitmap, scanning chunk by chunk. A chunk's bit range is word-aligned
// whenever the chunk size is a multiple of 64, which every public constructor
// guarantees — that is what lets a later parallel executor fill the same
// bitmap from concurrent per-chunk scans without locking.
func (c *chunkedColumn[T, K]) FilterSelection(predicate func(T) bool) *Selection {
	s := NewSelection(c.data.len())
	for ci := 0; ci < c.data.numChunks(); ci++ {
		base := uint32(ci) << c.data.shift
		for j, v := range c.data.chunk(ci) {
			if predicate(v) {
				s.Add(base + uint32(j))
			}
		}
	}
	return s
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
		columnDef, chunkSize, identityKey[string], formatString, nil, true)}
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
		columnDef, chunkSize, identityKey[bool], formatBool, nil, false)}
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
		columnDef, chunkSize, identityKey[int64], formatInt64, nil, true)}
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
		columnDef, chunkSize, identityKey[uint64], formatUint64, nil, true)}
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
		columnDef, chunkSize, identityKey[uint32], formatUint32, nil, true)}
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
		columnDef, chunkSize, float64GroupKey, FormatFloat64, math.IsNaN, true)}
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
)
