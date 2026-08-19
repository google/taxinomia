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
	"math"

	"github.com/google/taxinomia/core/executor"
)

// GroupPartition is the complete result of the level-0 grouping pass over one
// selection: per-code counts and representative rows (exactly GroupCounts'
// results), plus every resolvable selected row laid out contiguously by group.
// Group code c's rows are Backing[Offsets[c] : Offsets[c]+Counts[c]], in
// selection order. Codes follow the column's IGroupOps assignment for the same
// (column, selection), so they remain valid inputs to GroupMembers afterwards.
type GroupPartition struct {
	Counts  []uint32
	Firsts  []uint32
	Offsets []uint32
	Backing []uint32
}

// groupPartitioner is the internal seam chunked columns implement to run the
// grouping pass as per-chunk partials merged on the executor pool
// (docs/scaling-to-1b-rows.md §8). handled=false means the column declines for
// this input (unrangeable selection, too few chunks, misaligned chunk size)
// and the caller must use the sequential path; the two paths produce identical
// partitions.
type groupPartitioner interface {
	partitionGroups(ctx context.Context, sel RowSet) (part *GroupPartition, handled bool, err error)
}

// PartitionGroups partitions sel by col's grouping: one GroupCounts-equivalent
// pass plus a scatter of every selected row into a contiguous per-group layout.
// On chunked columns both passes run as per-chunk partials on the global
// executor pool — merging dictionary-coded partials is array addition, merging
// hash-grouped partials preserves first-appearance code order — and observe
// ctx at chunk granularity. Other columns (and explicit RowIndices selections,
// whose order is not chunk-decomposable) run sequentially via the column's
// IGroupOps. A cancelled ctx returns (nil, ctx.Err()).
func PartitionGroups(ctx context.Context, col IDataColumn, view *ColumnView, sel RowSet) (*GroupPartition, error) {
	if p, ok := col.(groupPartitioner); ok {
		part, handled, err := p.partitionGroups(ctx, sel)
		if err != nil {
			return nil, err
		}
		if handled {
			return part, nil
		}
	}
	ops := GroupOpsFor(col, view)
	// Per-row-keyed columns without chunk geometry (computed and joined —
	// the virtual columns) parallelize over synthetic spans of the row
	// range instead of chunks; the same partial-merge discipline keeps the
	// result bit-identical to the sequential pass.
	if rs, ok := sel.(rangeRowSet); ok {
		if pr, ok := ops.(perRowPartitioner); ok {
			part, handled, err := pr.perRowPartition(ctx, rs)
			if err != nil {
				return nil, err
			}
			if handled {
				return part, nil
			}
		}
	}
	return sequentialPartition(ctx, ops, sel)
}

// scatterAccumulator lays group members out contiguously in one backing
// array, one cursor per group code. It implements GroupAccumulator.
type scatterAccumulator struct {
	backing []uint32
	cursor  []uint32
}

func (a *scatterAccumulator) Add(code uint32, row uint32) {
	a.backing[a.cursor[code]] = row
	a.cursor[code]++
}

// offsetsOf returns the exclusive prefix sums of counts and their total: the
// start of each group's region in a contiguous per-group layout.
func offsetsOf(counts []uint32) (offsets []uint32, total uint32) {
	offsets = make([]uint32, len(counts))
	for code, n := range counts {
		offsets[code] = total
		total += n
	}
	return offsets, total
}

// sequentialPartition is the single-goroutine partition every column supports:
// GroupCounts, then one scatter pass through GroupAggregates. ctx is observed
// between the passes only — the fallback's honest granularity.
func sequentialPartition(ctx context.Context, ops IGroupOps, sel RowSet) (*GroupPartition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	counts, firsts := ops.GroupCounts(sel)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	offsets, total := offsetsOf(counts)
	backing := make([]uint32, total)
	cursor := make([]uint32, len(counts))
	copy(cursor, offsets)
	ops.GroupAggregates(sel, &scatterAccumulator{backing: backing, cursor: cursor})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &GroupPartition{Counts: counts, Firsts: firsts, Offsets: offsets, Backing: backing}, nil
}

// rangeRowSet is a RowSet whose rows can be iterated restricted to a row
// range, which is what decomposes a selection into per-chunk work. Selection
// and AllRows implement it; RowIndices cannot (its order is not positional).
type rangeRowSet interface {
	RowSet
	// forEachRowIn calls f for each row in the set with lo <= row < hi, in
	// set order, until f returns false.
	forEachRowIn(lo, hi int, f func(i uint32) bool)
	// universeRows is the size of the row universe the set selects from
	// (not the selected count) — the range synthetic spans must cover.
	universeRows() int
}

// perRowPartitioner is the seam per-row-keyed columns implement to run the
// hash partition as synthetic-span partials on the executor pool. Virtual
// columns (computed, joined) have no chunk geometry, so groupPartitioner
// cannot cover them; their per-row key getters read only immutable state
// (computed closures evaluate pure expressions over finalized columns,
// joiners memoize under sync.Once), which is what makes the concurrent
// spans sound. handled=false means the input is too small to parallelize
// and the caller must use the sequential path; the two paths produce
// identical partitions.
type perRowPartitioner interface {
	perRowPartition(ctx context.Context, sel rangeRowSet) (part *GroupPartition, handled bool, err error)
}

// perRowSpanRows is the synthetic span length for per-row partitions: the
// chunk size, so span boundaries stay multiples of 64 (selection words never
// straddle spans) and span counts match the chunked paths' granularity.
const perRowSpanRows = 1 << 16

// perRowHashPartition partitions any per-row-keyed column over synthetic
// spans of the row range. Unlike scans, the pass is compute-bound (the key
// getter runs an expression interpreter or a join lookup per row), so
// parallelism scales with cores rather than memory bandwidth.
func perRowHashPartition[K comparable](ctx context.Context, key func(uint32) (K, bool), sel rangeRowSet) (*GroupPartition, bool, error) {
	nc := (sel.universeRows() + perRowSpanRows - 1) / perRowSpanRows
	if nc < 2 {
		return nil, false, nil
	}
	part, err := parallelHashPartition(ctx, nc, perRowSpanRows, key, sel)
	if err != nil {
		return nil, false, err
	}
	return part, true, nil
}

// partitionSpans splits nc chunks into contiguous spans for one partition
// pass. Spans are few and large — a small multiple of the pool's parallelism,
// not one per chunk — because every span carries O(distinct) partial state
// that the merge walks; cancellation stays prompt because span bodies observe
// ctx between chunks, not between spans.
func partitionSpans(nc int) (spanLen, spans int) {
	target := 4 * executor.Default().Parallelism()
	spanLen = (nc + target - 1) / target
	spans = (nc + spanLen - 1) / spanLen
	return spanLen, spans
}

// hashSpanPartial is one span's partial state in a hash partition: local
// dense codes in local first-appearance order, plus the translation and
// backing-start tables the merge fills in.
type hashSpanPartial[K comparable] struct {
	codeOf map[K]uint32
	keys   []K      // local code -> key, in local first-appearance order
	counts []uint32 // local code -> selected rows in this span
	firsts []uint32 // local code -> first selected row in this span
	trans  []uint32 // local code -> global code (merge pass)
	starts []uint32 // local code -> next backing slot (cursor pass, then pass 2 cursor)
}

// parallelHashPartition partitions a hash-grouped column (key resolved per
// row, dense codes in first-appearance order) as per-span partials plus a
// merge. Spans cover disjoint ascending row ranges and the merge visits them
// in span order, so global codes, counts, firsts and the per-group row order
// are identical to the sequential first-appearance pass over the same
// selection.
func parallelHashPartition[K comparable](ctx context.Context, nc, chunkSize int, key func(uint32) (K, bool), sel rangeRowSet) (*GroupPartition, error) {
	spanLen, spans := partitionSpans(nc)
	partials := make([]*hashSpanPartial[K], spans)
	pool := executor.Default()

	// Pass 1: per-span counts, firsts and local code assignment.
	err := pool.Run(ctx, spans, func(si int) {
		p := &hashSpanPartial[K]{codeOf: make(map[K]uint32)}
		partials[si] = p
		for ck := si * spanLen; ck < (si+1)*spanLen && ck < nc; ck++ {
			if ctx.Err() != nil {
				return
			}
			sel.forEachRowIn(ck*chunkSize, (ck+1)*chunkSize, func(i uint32) bool {
				v, ok := key(i)
				if !ok {
					return true
				}
				code, seen := p.codeOf[v]
				if !seen {
					code = uint32(len(p.keys))
					p.codeOf[v] = code
					p.keys = append(p.keys, v)
					p.counts = append(p.counts, 0)
					p.firsts = append(p.firsts, i)
				}
				p.counts[code]++
				return true
			})
		}
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Merge in span order: spans hold disjoint ascending row ranges, so
	// first appearance across spans in order is first appearance in the
	// selection.
	globalCode := make(map[K]uint32)
	var counts, firsts []uint32
	for _, p := range partials {
		p.trans = make([]uint32, len(p.keys))
		for l, k := range p.keys {
			g, seen := globalCode[k]
			if !seen {
				g = uint32(len(counts))
				globalCode[k] = g
				counts = append(counts, 0)
				firsts = append(firsts, p.firsts[l])
			}
			p.trans[l] = g
			counts[g] += p.counts[l]
		}
	}
	offsets, total := offsetsOf(counts)

	// Cursor pass: each span's slice of each group's backing region starts
	// after the same group's rows from earlier spans.
	next := make([]uint32, len(counts))
	copy(next, offsets)
	for _, p := range partials {
		p.starts = make([]uint32, len(p.keys))
		for l := range p.keys {
			g := p.trans[l]
			p.starts[l] = next[g]
			next[g] += p.counts[l]
		}
	}

	// Pass 2: scatter. Spans write disjoint backing slots, so no locking.
	backing := make([]uint32, total)
	err = pool.Run(ctx, spans, func(si int) {
		p := partials[si]
		for ck := si * spanLen; ck < (si+1)*spanLen && ck < nc; ck++ {
			if ctx.Err() != nil {
				return
			}
			sel.forEachRowIn(ck*chunkSize, (ck+1)*chunkSize, func(i uint32) bool {
				v, ok := key(i)
				if !ok {
					return true
				}
				l := p.codeOf[v]
				backing[p.starts[l]] = i
				p.starts[l]++
				return true
			})
		}
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &GroupPartition{Counts: counts, Firsts: firsts, Offsets: offsets, Backing: backing}, nil
}

// maxDensePartialBytes bounds the transient per-span dense arrays of a
// dictionary partition (counts + firsts, 8 bytes per dictionary entry per
// span). When the budget cannot fit two spans the caller declines to the
// sequential path rather than switching code schemes.
const maxDensePartialBytes = 64 << 20

// parallelDensePartition partitions a dictionary-coded column whose codes are
// global across chunks: partial counts merge by array addition — the §8 case
// the global dictionary exists for. Codes are the dictionary codes, zero-count
// groups included, exactly as the dense GroupCounts path reports them.
// handled=false means the dictionary is too large for the partial budget.
func parallelDensePartition[K Unsigned](ctx context.Context, codes *chunkedData[K], dictSize int, sel rangeRowSet) (*GroupPartition, bool, error) {
	nc := codes.numChunks()
	chunkSize := codes.chunkSize()
	spanLen, spans := partitionSpans(nc)
	if perSpan := 8 * dictSize; perSpan > 0 && spans*perSpan > maxDensePartialBytes {
		spans = maxDensePartialBytes / perSpan
		if spans < 2 {
			return nil, false, nil
		}
		spanLen = (nc + spans - 1) / spans
		spans = (nc + spanLen - 1) / spanLen
	}

	type densePartial struct {
		counts []uint32
		firsts []uint32
	}
	partials := make([]*densePartial, spans)
	pool := executor.Default()

	// Pass 1: per-span dense counts over the global dictionary.
	err := pool.Run(ctx, spans, func(si int) {
		p := &densePartial{counts: make([]uint32, dictSize), firsts: make([]uint32, dictSize)}
		partials[si] = p
		for ck := si * spanLen; ck < (si+1)*spanLen && ck < nc; ck++ {
			if ctx.Err() != nil {
				return
			}
			chunk := codes.chunk(ck)
			base := ck * chunkSize
			if ar, ok := sel.(allRows); ok && base+len(chunk) <= int(ar) {
				// Full universe: scan the code chunk directly instead of
				// paying the (chunkID, offset) split per row.
				for j, code := range chunk {
					if p.counts[code] == 0 {
						p.firsts[code] = uint32(base + j)
					}
					p.counts[code]++
				}
				continue
			}
			sel.forEachRowIn(base, base+chunkSize, func(i uint32) bool {
				code := codes.at(i)
				if p.counts[code] == 0 {
					p.firsts[code] = i
				}
				p.counts[code]++
				return true
			})
		}
	})
	if err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Merge: array addition, firsts from the earliest span that saw the code.
	counts := make([]uint32, dictSize)
	firsts := make([]uint32, dictSize)
	for _, p := range partials {
		for code, n := range p.counts {
			if n == 0 {
				continue
			}
			if counts[code] == 0 {
				firsts[code] = p.firsts[code]
			}
			counts[code] += n
		}
	}
	offsets, total := offsetsOf(counts)

	// Cursor pass, reusing each partial's counts array as its cursor: each
	// span's slice of a group's backing region starts after the same group's
	// rows from earlier spans.
	next := make([]uint32, dictSize)
	copy(next, offsets)
	for _, p := range partials {
		for code, n := range p.counts {
			p.counts[code] = next[code]
			next[code] += n
		}
	}

	// Pass 2: scatter. Spans write disjoint backing slots, so no locking.
	backing := make([]uint32, total)
	err = pool.Run(ctx, spans, func(si int) {
		p := partials[si]
		for ck := si * spanLen; ck < (si+1)*spanLen && ck < nc; ck++ {
			if ctx.Err() != nil {
				return
			}
			chunk := codes.chunk(ck)
			base := ck * chunkSize
			if ar, ok := sel.(allRows); ok && base+len(chunk) <= int(ar) {
				for j, code := range chunk {
					backing[p.counts[code]] = uint32(base + j)
					p.counts[code]++
				}
				continue
			}
			sel.forEachRowIn(base, base+chunkSize, func(i uint32) bool {
				code := codes.at(i)
				backing[p.counts[code]] = i
				p.counts[code]++
				return true
			})
		}
	})
	if err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	return &GroupPartition{Counts: counts, Firsts: firsts, Offsets: offsets, Backing: backing}, true, nil
}

// --- perRowPartitioner wiring for the virtual columns ---
//
// One delegation per virtual column type: each reuses its own groupKeyAt, so
// key semantics (error rows dropped, joined unmatched rows dropped, float
// normalization where the type defines one) are identical to the sequential
// IGroupOps pass by construction.

func (c *ComputedStringColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *ComputedUint32Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

// computedFloatSpanKey reproduces map[float64] equality as a canonical
// comparable key: -0 collapses into +0 (Go's == on floats), while each NaN
// row keys uniquely by its row — the pinned historical semantics of computed
// float grouping (every NaN row is its own group). A raw float64 key cannot
// be used here: NaN != NaN would make the scatter pass miss its own pass-1
// map entries.
type computedFloatSpanKey struct {
	bits   uint64
	nanRow uint32
}

func (c *ComputedFloat64Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	key := func(i uint32) (computedFloatSpanKey, bool) {
		v, err := c.GetValue(i)
		if err != nil {
			return computedFloatSpanKey{}, false
		}
		if math.IsNaN(v) {
			// Any NaN payload maps to the canonical NaN bits plus the row,
			// which no non-NaN float can produce — no collisions.
			return computedFloatSpanKey{bits: math.Float64bits(math.NaN()), nanRow: i}, true
		}
		if v == 0 {
			v = 0 // collapse -0 into +0, matching map[float64] equality
		}
		return computedFloatSpanKey{bits: math.Float64bits(v)}, true
	}
	return perRowHashPartition(ctx, key, sel)
}

func (c *ComputedInt64Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *ComputedDatetimeColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *ComputedDurationColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *ComputedBoolColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedStringColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedUint32Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedDatetimeColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedDurationColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedBoolColumn) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedFloat64Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedInt64Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}

func (c *JoinedUint64Column) perRowPartition(ctx context.Context, sel rangeRowSet) (*GroupPartition, bool, error) {
	return perRowHashPartition(ctx, c.groupKeyAt, sel)
}
