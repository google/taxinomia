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
	"math"
	"sort"
	"time"
)

// GroupAccumulator receives one (group code, row index) pair per resolvable
// selected row during IGroupOps.GroupAggregates. Implementations hold
// O(distinct) state indexed by code — for example one aggregate state per
// group — so aggregating never materialises per-group membership lists.
type GroupAccumulator interface {
	Add(code uint32, row uint32)
}

// IGroupOps is the narrow grouping interface that replaces the deprecated
// membership-returning GroupIndices (docs/scaling-to-1b-rows.md §7). It is an
// optional interface in the io.ReaderAt tradition: engines type-assert for it
// and fall back to GroupOpsFor's shim, so external IDataColumn implementations
// keep working unchanged.
//
// The selection is a RowSet — a Selection bitmap, an AllRows universe, or an
// explicit RowIndices list — and is never materialised by the operations, so
// no selection allocation scales with the match count.
//
// Group codes are dense uint32 values in [0, len(counts)). For a fixed column
// state and a fixed selection, the three operations assign identical codes, so
// results can be combined across calls. Codes are not otherwise specified:
// dictionary-encoded columns use their dictionary codes (and may therefore
// report zero-count groups, which callers must skip); hash-grouped columns
// assign codes in order of first appearance in the selection.
//
// Rows whose value cannot be resolved (failed join lookups, computed-column
// errors, out-of-range indices) are omitted from all three operations, exactly
// as GroupIndices dropped them into its unmapped return.
type IGroupOps interface {
	// GroupCounts returns, per group code, the number of selected rows in the
	// group and the first selected row carrying it (the group's representative
	// row, valid wherever counts[code] > 0). Output is O(distinct).
	GroupCounts(sel RowSet) (counts []uint32, firsts []uint32)

	// GroupAggregates streams every resolvable selected row to acc as a
	// (code, row) pair, in selection order. The accumulator owns the
	// aggregation, so this replaces iterating per-group membership lists.
	GroupAggregates(sel RowSet, acc GroupAccumulator)

	// GroupMembers returns one page of one group: the selected rows carrying
	// code, in selection order, skipping the first offset of them and
	// returning at most n (n < 0 means all remaining).
	GroupMembers(sel RowSet, code uint32, offset, n int) []uint32
}

// GroupOpsFor returns col's native IGroupOps when it implements one, or a
// correct-but-slow shim built on the deprecated GroupIndices otherwise. The
// shim exists for external column implementations; every in-repo column type
// implements IGroupOps natively.
func GroupOpsFor(col IDataColumn, view *ColumnView) IGroupOps {
	if ops, ok := col.(IGroupOps); ok {
		return ops
	}
	return &groupIndicesShim{col: col, view: view}
}

// --- generic hash-grouping engine ---
//
// All non-dictionary columns group the same way: resolve a comparable group
// key per row, drop unresolvable rows, assign dense codes in order of first
// appearance in the selection. The three operations share the key getter so
// their code assignment is identical for the same selection.

func groupCountsByKey[T comparable](sel RowSet, key func(uint32) (T, bool)) (counts, firsts []uint32) {
	codeOf := make(map[T]uint32)
	sel.ForEachRow(func(i uint32) bool {
		v, ok := key(i)
		if !ok {
			return true
		}
		code, seen := codeOf[v]
		if !seen {
			code = uint32(len(codeOf))
			codeOf[v] = code
			counts = append(counts, 0)
			firsts = append(firsts, i)
		}
		counts[code]++
		return true
	})
	return counts, firsts
}

func groupAggregatesByKey[T comparable](sel RowSet, key func(uint32) (T, bool), acc GroupAccumulator) {
	codeOf := make(map[T]uint32)
	sel.ForEachRow(func(i uint32) bool {
		v, ok := key(i)
		if !ok {
			return true
		}
		code, seen := codeOf[v]
		if !seen {
			code = uint32(len(codeOf))
			codeOf[v] = code
		}
		acc.Add(code, i)
		return true
	})
}

func groupMembersByKey[T comparable](sel RowSet, key func(uint32) (T, bool), code uint32, offset, n int) []uint32 {
	if n == 0 {
		return nil
	}
	codeOf := make(map[T]uint32)
	var members []uint32
	skipped := 0
	sel.ForEachRow(func(i uint32) bool {
		v, ok := key(i)
		if !ok {
			return true
		}
		c, seen := codeOf[v]
		if !seen {
			c = uint32(len(codeOf))
			codeOf[v] = c
		}
		if c != code {
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

// float64GroupKey canonicalises a float64 into a comparable grouping key with
// the same equality Float64Column's GroupIndices used: all NaNs form one
// group, and -0 groups with +0.
func float64GroupKey(v float64) uint64 {
	if math.IsNaN(v) {
		return math.Float64bits(math.NaN())
	}
	if v == 0 {
		v = 0 // collapse -0 into +0, matching map[float64] equality
	}
	return math.Float64bits(v)
}

// --- fallback shim over the deprecated GroupIndices ---

type groupIndicesShim struct {
	col  IDataColumn
	view *ColumnView
}

// partition runs GroupIndices and orders its groups by ascending group key,
// which is deterministic for a fixed column and selection regardless of the
// wrapped implementation's key scheme. It materialises the selection —
// GroupIndices consumes an index list — which is the price of the
// compatibility fallback, not of the RowSet paths.
func (s *groupIndicesShim) partition(sel RowSet) [][]uint32 {
	grouped, _ := s.col.GroupIndices(rowSetIndices(sel), s.view)
	keys := make([]uint32, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a] < keys[b] })
	parts := make([][]uint32, len(keys))
	for code, k := range keys {
		parts[code] = grouped[k]
	}
	return parts
}

func (s *groupIndicesShim) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	parts := s.partition(sel)
	counts := make([]uint32, len(parts))
	firsts := make([]uint32, len(parts))
	for code, members := range parts {
		counts[code] = uint32(len(members))
		firsts[code] = members[0]
	}
	return counts, firsts
}

func (s *groupIndicesShim) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	for code, members := range s.partition(sel) {
		for _, i := range members {
			acc.Add(uint32(code), i)
		}
	}
}

func (s *groupIndicesShim) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	parts := s.partition(sel)
	if int(code) >= len(parts) {
		return nil
	}
	members := parts[code]
	if offset >= len(members) {
		return nil
	}
	members = members[offset:]
	if n >= 0 && n < len(members) {
		members = members[:n]
	}
	return members
}

// --- native implementations: plain columns ---

func (c *StringColumn) groupKeyAt(i uint32) (string, bool) {
	if i >= uint32(len(c.data)) {
		return "", false
	}
	return c.data[i], true
}

func (c *StringColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *StringColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *StringColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *Uint32Column) groupKeyAt(i uint32) (uint32, bool) {
	if int(i) >= len(c.data) {
		return 0, false
	}
	return c.data[i], true
}

func (c *Uint32Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *Uint32Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *Uint32Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *Int64Column) groupKeyAt(i uint32) (int64, bool) {
	if int(i) >= len(c.data) {
		return 0, false
	}
	return c.data[i], true
}

func (c *Int64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *Int64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *Int64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *Uint64Column) groupKeyAt(i uint32) (uint64, bool) {
	if int(i) >= len(c.data) {
		return 0, false
	}
	return c.data[i], true
}

func (c *Uint64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *Uint64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *Uint64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *Float64Column) groupKeyAt(i uint32) (uint64, bool) {
	if int(i) >= len(c.data) {
		return 0, false
	}
	return float64GroupKey(c.data[i]), true
}

func (c *Float64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *Float64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *Float64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *BoolColumn) groupKeyAt(i uint32) (bool, bool) {
	if int(i) >= len(c.data) {
		return false, false
	}
	return c.data[i], true
}

func (c *BoolColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *BoolColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *BoolColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *DatetimeColumn) groupKeyAt(i uint32) (int64, bool) {
	if i >= uint32(len(c.data)) {
		return 0, false
	}
	return c.data[i].UnixNano(), true
}

func (c *DatetimeColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *DatetimeColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *DatetimeColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *DurationColumn) groupKeyAt(i uint32) (int64, bool) {
	if int(i) >= len(c.data) {
		return 0, false
	}
	return int64(c.data[i]), true
}

func (c *DurationColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *DurationColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *DurationColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

// --- native implementation: dictionary column ---
//
// The dictionary code is the group code, so counting is a dense array pass
// with no hashing. For a small selection over a large dictionary the dense
// arrays would dwarf the input (the same trade-off GroupIndices makes), so
// the operations fall back to hash grouping over the codes; the cutover
// depends only on (column, selection), keeping code assignment consistent
// across the three operations.

func (c *DictStringColumn[K]) smallGroupSubset(sel RowSet) bool {
	return sel.NumRows() < len(c.dict)/8
}

func (c *DictStringColumn[K]) groupKeyAt(i uint32) (K, bool) {
	if i >= uint32(len(c.codes)) {
		return 0, false
	}
	return c.codes[i], true
}

func (c *DictStringColumn[K]) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	if c.smallGroupSubset(sel) {
		return groupCountsByKey(sel, c.groupKeyAt)
	}
	counts := make([]uint32, len(c.dict))
	firsts := make([]uint32, len(c.dict))
	sel.ForEachRow(func(i uint32) bool {
		code := c.codes[i]
		if counts[code] == 0 {
			firsts[code] = i
		}
		counts[code]++
		return true
	})
	return counts, firsts
}

func (c *DictStringColumn[K]) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	if c.smallGroupSubset(sel) {
		groupAggregatesByKey(sel, c.groupKeyAt, acc)
		return
	}
	sel.ForEachRow(func(i uint32) bool {
		acc.Add(uint32(c.codes[i]), i)
		return true
	})
}

func (c *DictStringColumn[K]) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	if c.smallGroupSubset(sel) {
		return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
	}
	if n == 0 {
		return nil
	}
	var members []uint32
	skipped := 0
	sel.ForEachRow(func(i uint32) bool {
		if uint32(c.codes[i]) != code {
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

// --- native implementations: joined columns ---

func (c *JoinedStringColumn) groupKeyAt(i uint32) (string, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return "", false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return "", false
	}
	return v, true
}

func (c *JoinedStringColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedStringColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedStringColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedUint32Column) groupKeyAt(i uint32) (uint32, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (c *JoinedUint32Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedUint32Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedUint32Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedDatetimeColumn) groupKeyAt(i uint32) (int64, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return 0, false
	}
	return v.UnixNano(), true
}

func (c *JoinedDatetimeColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedDatetimeColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedDatetimeColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedDurationColumn) groupKeyAt(i uint32) (int64, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return 0, false
	}
	return int64(v), true
}

func (c *JoinedDurationColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedDurationColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedDurationColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedBoolColumn) groupKeyAt(i uint32) (bool, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return false, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return false, false
	}
	return v, true
}

func (c *JoinedBoolColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedBoolColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedBoolColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedFloat64Column) groupKeyAt(i uint32) (uint64, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return 0, false
	}
	return float64GroupKey(v), true
}

func (c *JoinedFloat64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedFloat64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedFloat64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedInt64Column) groupKeyAt(i uint32) (int64, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (c *JoinedInt64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedInt64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedInt64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *JoinedUint64Column) groupKeyAt(i uint32) (uint64, bool) {
	targetIndex, err := c.joiner.Lookup(i)
	if err != nil {
		return 0, false
	}
	v, err := c.sourceColumn.GetValue(targetIndex)
	if err != nil {
		return 0, false
	}
	return v, true
}

func (c *JoinedUint64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *JoinedUint64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *JoinedUint64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

// --- native implementations: computed columns ---

func (c *ComputedStringColumn) groupKeyAt(i uint32) (string, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedStringColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedStringColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedStringColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *ComputedUint32Column) groupKeyAt(i uint32) (uint32, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedUint32Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedUint32Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedUint32Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

// ComputedFloat64Column intentionally uses the raw float64 as the key: its
// GroupIndices used map[float64] directly, under which every NaN row is its
// own group. Go maps give the same equality for float64 keys, so the quirk is
// preserved bit-for-bit.
func (c *ComputedFloat64Column) groupKeyAt(i uint32) (float64, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedFloat64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedFloat64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedFloat64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *ComputedInt64Column) groupKeyAt(i uint32) (int64, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedInt64Column) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedInt64Column) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedInt64Column) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *ComputedDatetimeColumn) groupKeyAt(i uint32) (int64, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedDatetimeColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedDatetimeColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedDatetimeColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *ComputedDurationColumn) groupKeyAt(i uint32) (time.Duration, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedDurationColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedDurationColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedDurationColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

func (c *ComputedBoolColumn) groupKeyAt(i uint32) (bool, bool) {
	v, err := c.GetValue(i)
	return v, err == nil
}

func (c *ComputedBoolColumn) GroupCounts(sel RowSet) ([]uint32, []uint32) {
	return groupCountsByKey(sel, c.groupKeyAt)
}

func (c *ComputedBoolColumn) GroupAggregates(sel RowSet, acc GroupAccumulator) {
	groupAggregatesByKey(sel, c.groupKeyAt, acc)
}

func (c *ComputedBoolColumn) GroupMembers(sel RowSet, code uint32, offset, n int) []uint32 {
	return groupMembersByKey(sel, c.groupKeyAt, code, offset, n)
}

// Compile-time checks: every in-repo column type implements IGroupOps.
var (
	_ IGroupOps = (*StringColumn)(nil)
	_ IGroupOps = (*DictStringColumn[uint8])(nil)
	_ IGroupOps = (*DictStringColumn[uint16])(nil)
	_ IGroupOps = (*DictStringColumn[uint32])(nil)
	_ IGroupOps = (*Uint32Column)(nil)
	_ IGroupOps = (*Int64Column)(nil)
	_ IGroupOps = (*Uint64Column)(nil)
	_ IGroupOps = (*Float64Column)(nil)
	_ IGroupOps = (*BoolColumn)(nil)
	_ IGroupOps = (*DatetimeColumn)(nil)
	_ IGroupOps = (*DurationColumn)(nil)
	_ IGroupOps = (*JoinedStringColumn)(nil)
	_ IGroupOps = (*JoinedUint32Column)(nil)
	_ IGroupOps = (*JoinedDatetimeColumn)(nil)
	_ IGroupOps = (*JoinedDurationColumn)(nil)
	_ IGroupOps = (*JoinedBoolColumn)(nil)
	_ IGroupOps = (*JoinedFloat64Column)(nil)
	_ IGroupOps = (*JoinedInt64Column)(nil)
	_ IGroupOps = (*JoinedUint64Column)(nil)
	_ IGroupOps = (*ComputedStringColumn)(nil)
	_ IGroupOps = (*ComputedUint32Column)(nil)
	_ IGroupOps = (*ComputedFloat64Column)(nil)
	_ IGroupOps = (*ComputedInt64Column)(nil)
	_ IGroupOps = (*ComputedDatetimeColumn)(nil)
	_ IGroupOps = (*ComputedDurationColumn)(nil)
	_ IGroupOps = (*ComputedBoolColumn)(nil)
)
