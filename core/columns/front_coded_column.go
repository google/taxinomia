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
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
	"strings"
)

// fcRestartInterval is the front-coding restart stride: every
// fcRestartInterval-th row within a chunk stores its full bytes (prefix
// length zero), so random access decodes at most fcRestartInterval-1 entries
// forward from the nearest restart. A power of two, so the restart row is a
// bit mask away.
const fcRestartInterval = 16

// fcChunk is one row group of front-coded string storage. Entry j occupies
// blob[offsets[j]:offsets[j+1]] and holds uvarint(prefixLen) followed by the
// suffix bytes; the string at j is the first prefixLen bytes of the string at
// j-1 plus the suffix. Rows at restart positions store prefixLen 0.
type fcChunk struct {
	blob    []byte
	offsets []uint32 // len = rows+1, offsets[0] == 0
}

func (ch *fcChunk) rows() int {
	return len(ch.offsets) - 1
}

// entry returns row j's prefix length and suffix bytes.
func (ch *fcChunk) entry(j uint32) (prefixLen int, suffix []byte) {
	e := ch.blob[ch.offsets[j]:ch.offsets[j+1]]
	pl, m := binary.Uvarint(e)
	return int(pl), e[m:]
}

// ChunkedFrontCodedStringColumn is the string primary key representation on
// sorted storage (docs/scaling-to-1b-rows.md §5: front-coded arena + sparse
// index). Each value is stored as the length of the prefix it shares with its
// predecessor plus the differing suffix — for sorted keys, which share long
// prefixes by construction, a few bytes per row instead of the full string —
// with periodic full-value restarts for random access. The column is built
// only from a finalized column that is unique and stored in value order, so
// IsKey and SortedBySelf are true by construction, reverse lookup is a binary
// search (chunk firsts, then restarts, then at most one restart span), and no
// reverse-lookup map exists at any size.
type ChunkedFrontCodedStringColumn struct {
	columnDef *ColumnDef
	shift     uint32 // log2(chunk size)
	mask      uint32 // chunk size - 1
	chunks    []fcChunk
	n         int
	zones     *zoneMap[string]
}

func (c *ChunkedFrontCodedStringColumn) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *ChunkedFrontCodedStringColumn) Length() int {
	return c.n
}

func (c *ChunkedFrontCodedStringColumn) chunkSize() int {
	return 1 << c.shift
}

// value returns the decoded string at global row i. Restart rows are served
// zero-copy from the blob; other rows are reconstructed from the nearest
// restart, at most fcRestartInterval-1 entries away, into one allocation.
func (c *ChunkedFrontCodedStringColumn) value(i uint32) string {
	ch := &c.chunks[i>>c.shift]
	j := i & c.mask
	r := j &^ (fcRestartInterval - 1)
	_, suffix := ch.entry(r)
	if j == r {
		return arenaString(suffix)
	}
	buf := append(make([]byte, 0, 2*len(suffix)), suffix...)
	for k := r + 1; k <= j; k++ {
		pl, sfx := ch.entry(k)
		buf = append(buf[:pl], sfx...)
	}
	return string(buf)
}

func (c *ChunkedFrontCodedStringColumn) GetValue(i uint32) (string, error) {
	if i >= uint32(c.n) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, c.n)
	}
	return c.value(i), nil
}

func (c *ChunkedFrontCodedStringColumn) GetString(i uint32) (string, error) {
	return c.GetValue(i)
}

// IsKey is true by construction: the column is only built from unique data.
func (c *ChunkedFrontCodedStringColumn) IsKey() bool {
	return true
}

// SortedBySelf is true by construction: the column is only built from data in
// value order.
func (c *ChunkedFrontCodedStringColumn) SortedBySelf() bool {
	return true
}

func (c *ChunkedFrontCodedStringColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedStringColumn(columnDef, joiner, c)
}

// restartValue returns the string at restart rr of chunk ci, zero-copy.
func (c *ChunkedFrontCodedStringColumn) restartValue(ci int, rr int) string {
	_, suffix := c.chunks[ci].entry(uint32(rr * fcRestartInterval))
	return arenaString(suffix)
}

// findRow locates the row holding v: binary search over the chunk-first
// values (the sparse index — one entry per chunk), then over the candidate
// chunk's restarts, then a decode of at most fcRestartInterval-1 entries with
// early exit. No probe decodes more than one restart span.
func (c *ChunkedFrontCodedStringColumn) findRow(v string) (uint32, bool) {
	if c.n == 0 {
		return 0, false
	}
	ci := sort.Search(len(c.chunks), func(i int) bool {
		return c.restartValue(i, 0) > v
	}) - 1
	if ci < 0 {
		return 0, false
	}
	ch := &c.chunks[ci]
	numRestarts := (ch.rows() + fcRestartInterval - 1) / fcRestartInterval
	rr := sort.Search(numRestarts, func(r int) bool {
		return c.restartValue(ci, r) > v
	}) - 1
	// ci was chosen so its first value is <= v, hence restart 0 qualifies.
	r := uint32(rr * fcRestartInterval)
	_, suffix := ch.entry(r)
	if arenaString(suffix) == v {
		return uint32(ci)<<c.shift + r, true
	}
	buf := append(make([]byte, 0, 2*len(suffix)), suffix...)
	end := r + fcRestartInterval
	if last := uint32(ch.rows()); end > last {
		end = last
	}
	for k := r + 1; k < end; k++ {
		pl, sfx := ch.entry(k)
		buf = append(buf[:pl], sfx...)
		switch cc := strings.Compare(arenaString(buf), v); {
		case cc == 0:
			return uint32(ci)<<c.shift + k, true
		case cc > 0:
			return 0, false
		}
	}
	return 0, false
}

// GetIndex returns the row index holding the given value. As with the other
// storage columns, reverse lookup is served for key entity columns; the
// column is always a key, so only the entity type gates it.
func (c *ChunkedFrontCodedStringColumn) GetIndex(v string) (uint32, error) {
	if c.columnDef.EntityType() != "" {
		if i, ok := c.findRow(v); ok {
			return i, nil
		}
	}
	return 0, fmt.Errorf("value %v not found in column %q", v, c.columnDef.Name())
}

// rowLowerBound returns the first row whose value is >= v, or Length() when
// none is. A binary search over decoded values; the row order is the value
// order by construction.
func (c *ChunkedFrontCodedStringColumn) rowLowerBound(v string) uint32 {
	return uint32(sort.Search(c.n, func(i int) bool {
		return c.value(uint32(i)) >= v
	}))
}

// FilterSelection returns the rows whose value satisfies the predicate,
// decoding each chunk in one sequential pass. Each row's value is passed as
// its own string (the decode buffer is reused, so a copy per row is the price
// of an opaque predicate; the structured filters below never decode per row).
func (c *ChunkedFrontCodedStringColumn) FilterSelection(predicate func(string) bool) *Selection {
	s := NewSelection(c.n)
	var buf []byte
	for ci := range c.chunks {
		base := uint32(ci) << c.shift
		for j := 0; j < c.chunks[ci].rows(); j++ {
			pl, sfx := c.chunks[ci].entry(uint32(j))
			buf = append(buf[:pl], sfx...)
			if predicate(string(buf)) {
				s.Add(base + uint32(j))
			}
		}
	}
	return s
}

// FilterSelectionEqual returns the rows whose value is exactly v: at most one
// row, found by the sparse-index search — no chunk is scanned.
func (c *ChunkedFrontCodedStringColumn) FilterSelectionEqual(v string) *Selection {
	s := NewSelection(c.n)
	if i, ok := c.findRow(v); ok {
		s.Add(i)
	}
	return s
}

// FilterSelectionIn returns the rows whose value equals any of the given
// values: one sparse-index search per value.
func (c *ChunkedFrontCodedStringColumn) FilterSelectionIn(values []string) *Selection {
	s := NewSelection(c.n)
	for _, v := range values {
		if i, ok := c.findRow(v); ok {
			s.Add(i)
		}
	}
	return s
}

// FilterSelectionRange returns the rows whose value lies in [lo, hi]
// (inclusive; a nil bound is unbounded): a contiguous row range on sorted
// storage, found by two binary searches.
func (c *ChunkedFrontCodedStringColumn) FilterSelectionRange(lo, hi *string) *Selection {
	s := NewSelection(c.n)
	start := uint32(0)
	if lo != nil {
		start = c.rowLowerBound(*lo)
	}
	end := uint32(c.n)
	if hi != nil {
		end = c.rowLowerBound(*hi)
		if end < uint32(c.n) && c.value(end) == *hi {
			end++
		}
	}
	for i := start; i < end; i++ {
		s.Add(i)
	}
	return s
}

// ChunkBounds returns chunk ci's zone-map bounds (its first and last value —
// the storage is sorted).
func (c *ChunkedFrontCodedStringColumn) ChunkBounds(ci int) (min, max string, ok bool) {
	return c.zones.bounds(ci)
}

// GroupIndices buckets the given indices by value. Every value is distinct,
// so every index forms its own group, keyed in input order.
//
// Deprecated: membership-returning grouping is O(rows) in output and does not
// scale; use the narrow IGroupOps operations instead.
func (c *ChunkedFrontCodedStringColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	groupedIndices := make(map[uint32][]uint32, len(indices))
	next := uint32(0)
	for _, i := range indices {
		if i >= uint32(c.n) {
			continue
		}
		groupedIndices[next] = []uint32{i}
		next++
	}
	return groupedIndices, nil
}

// CompareRows compares the values at rows i and j. Row order is value order
// and values are distinct, so the row comparison is the value comparison —
// nothing is decoded.
func (c *ChunkedFrontCodedStringColumn) CompareRows(i, j uint32) int {
	switch {
	case i < j:
		return -1
	case i > j:
		return 1
	default:
		return 0
	}
}

// --- IGroupOps ---
//
// Every value is distinct, so each selected row is its own group and codes
// are positions in selection order. All three operations are pure integer
// work: no value is decoded, no map is built.

func (c *ChunkedFrontCodedStringColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	var counts, firsts []uint32
	sel.ForEachRow(func(i uint32) bool {
		if i < uint32(c.n) {
			counts = append(counts, 1)
			firsts = append(firsts, i)
		}
		return true
	})
	return counts, firsts
}

func (c *ChunkedFrontCodedStringColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	code := uint32(0)
	sel.ForEachRow(func(i uint32) bool {
		if i < uint32(c.n) {
			acc.Add(code, i)
			code++
		}
		return true
	})
}

func (c *ChunkedFrontCodedStringColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	if n == 0 || offset >= 1 {
		return nil
	}
	var member []uint32
	k := uint32(0)
	sel.ForEachRow(func(i uint32) bool {
		if i >= uint32(c.n) {
			return true
		}
		if k == code {
			member = []uint32{i}
			return false
		}
		k++
		return true
	})
	return member
}

// --- IChunkedColumn ---

func (c *ChunkedFrontCodedStringColumn) ChunkSize() int {
	return c.chunkSize()
}

func (c *ChunkedFrontCodedStringColumn) NumChunks() int {
	return len(c.chunks)
}

func (c *ChunkedFrontCodedStringColumn) ChunkLen(chunk int) int {
	return c.chunks[chunk].rows()
}

// --- Reorderable ---

// Reorder returns the values permuted as a finalized ChunkedArenaStringColumn:
// front coding requires rows in value order, which an arbitrary permutation
// destroys, so the copy takes the general arena representation (which records
// sortedness and the key property itself if the permutation restores them).
func (c *ChunkedFrontCodedStringColumn) Reorder(perm []uint32) IDataColumn {
	checkPermLength(c.columnDef.Name(), len(perm), c.n)
	n := newChunkedArenaStringColumn(c.columnDef, c.chunkSize())
	for _, p := range perm {
		n.Append(c.value(p))
	}
	n.FinalizeColumn()
	return n
}

// FrontCodeChunkedStringColumn converts a string column into front-coded
// storage. It applies only to the string-primary-key role: the source must be
// finalized, unique (IsKey) and stored in value order (SortedBySelf) — the
// properties the representation's lookups and grouping shortcuts assume. Any
// other column is returned unchanged with ok false. The chunk size is
// preserved.
func FrontCodeChunkedStringColumn(src *ChunkedStringColumn) (*ChunkedFrontCodedStringColumn, bool) {
	if !src.IsKey() || !src.SortedBySelf() {
		return nil, false
	}
	chunkSize := src.data.chunkSize()
	dst := &ChunkedFrontCodedStringColumn{
		columnDef: src.columnDef,
		shift:     uint32(bits.TrailingZeros(uint(chunkSize))),
		mask:      uint32(chunkSize - 1),
		n:         src.Length(),
	}
	numChunks := src.data.numChunks()
	dst.chunks = make([]fcChunk, numChunks)
	dst.zones = &zoneMap[string]{
		cmp:       strings.Compare,
		mins:      make([]string, numChunks),
		maxs:      make([]string, numChunks),
		hasBounds: make([]bool, numChunks),
	}
	var varintBuf [binary.MaxVarintLen64]byte
	for ci := 0; ci < numChunks; ci++ {
		values := src.data.chunk(ci)
		ch := &dst.chunks[ci]
		ch.offsets = make([]uint32, 1, len(values)+1)
		var prev string
		for j, v := range values {
			pl := 0
			if j&(fcRestartInterval-1) != 0 {
				pl = commonPrefixLen(prev, v)
			}
			ch.blob = append(ch.blob, varintBuf[:binary.PutUvarint(varintBuf[:], uint64(pl))]...)
			ch.blob = append(ch.blob, v[pl:]...)
			ch.offsets = append(ch.offsets, uint32(len(ch.blob)))
			prev = v
		}
		if len(values) > 0 {
			dst.zones.mins[ci] = values[0]
			dst.zones.maxs[ci] = values[len(values)-1]
			dst.zones.hasBounds[ci] = true
		}
	}
	return dst, true
}

func commonPrefixLen(a, b string) int {
	max := len(a)
	if len(b) < max {
		max = len(b)
	}
	i := 0
	for i < max && a[i] == b[i] {
		i++
	}
	return i
}

// Compile-time checks: the front-coded column carries the full column
// surface.
var (
	_ IDataColumn          = (*ChunkedFrontCodedStringColumn)(nil)
	_ IDataColumnT[string] = (*ChunkedFrontCodedStringColumn)(nil)
	_ IGroupOps            = (*ChunkedFrontCodedStringColumn)(nil)
	_ IChunkedColumn       = (*ChunkedFrontCodedStringColumn)(nil)
	_ RowComparator        = (*ChunkedFrontCodedStringColumn)(nil)
	_ Reorderable          = (*ChunkedFrontCodedStringColumn)(nil)
)
