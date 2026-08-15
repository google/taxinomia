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
	"sync"
	"sync/atomic"
	"time"
)

// Per-chunk pre-aggregation (docs/scaling-to-1b-rows.md §4): for a
// dictionary-encoded grouping dimension, the aggregate contribution of one
// storage chunk to one group is a small mergeable partial — count, sum, sum of
// squares, min, max — keyed by the global dictionary code. Summarizing every
// chunk once per (dimension, measure) pair turns the level-0 aggregate pass
// into a merge of chunk summaries: a selection that covers a chunk entirely
// takes the cached partial without touching a row, a pruned-out chunk
// contributes nothing, and only chunks the selection cuts through are
// scanned. On storage sorted by the dimension a chunk holds very few distinct
// codes, so the retained summaries are O(chunks + distinct), not O(rows).

// NumericCodeAgg is the mergeable per-code partial for a numeric measure. Its
// fields and accumulation semantics mirror aggregates.NumericAggState add for
// add and combine for combine, so states assembled from partials format
// identically to per-row accumulation (float sums may differ in the last ulp
// because chunk subtotals associate differently).
type NumericCodeAgg struct {
	Count      int64
	Sum, SumSq float64
	Min, Max   float64
}

func initNumericCodeAgg() NumericCodeAgg {
	return NumericCodeAgg{Min: math.MaxFloat64, Max: -math.MaxFloat64}
}

func (a *NumericCodeAgg) add(v float64) {
	a.Count++
	a.Sum += v
	a.SumSq += v * v
	if v < a.Min {
		a.Min = v
	}
	if v > a.Max {
		a.Max = v
	}
}

func (a *NumericCodeAgg) combine(o *NumericCodeAgg) {
	if o.Count == 0 {
		return
	}
	a.Count += o.Count
	a.Sum += o.Sum
	a.SumSq += o.SumSq
	if o.Min < a.Min {
		a.Min = o.Min
	}
	if o.Max > a.Max {
		a.Max = o.Max
	}
}

// BoolCodeAgg is the mergeable per-code partial for a boolean measure.
type BoolCodeAgg struct {
	Count, True int64
}

func initBoolCodeAgg() BoolCodeAgg { return BoolCodeAgg{} }

func (a *BoolCodeAgg) add(v bool) {
	a.Count++
	if v {
		a.True++
	}
}

func (a *BoolCodeAgg) combine(o *BoolCodeAgg) {
	a.Count += o.Count
	a.True += o.True
}

// DatetimeCodeAgg is the mergeable per-code partial for a datetime measure,
// over epoch nanoseconds exactly as aggregates.DatetimeAggState holds them.
type DatetimeCodeAgg struct {
	Count      int64
	Sum, SumSq float64 // epoch nanoseconds
	Min, Max   int64   // epoch nanoseconds
}

func initDatetimeCodeAgg() DatetimeCodeAgg {
	return DatetimeCodeAgg{Min: math.MaxInt64, Max: math.MinInt64}
}

func (a *DatetimeCodeAgg) add(v time.Time) {
	nanos := v.UnixNano()
	a.Count++
	a.Sum += float64(nanos)
	a.SumSq += float64(nanos) * float64(nanos)
	if nanos < a.Min {
		a.Min = nanos
	}
	if nanos > a.Max {
		a.Max = nanos
	}
}

func (a *DatetimeCodeAgg) combine(o *DatetimeCodeAgg) {
	if o.Count == 0 {
		return
	}
	a.Count += o.Count
	a.Sum += o.Sum
	a.SumSq += o.SumSq
	if o.Min < a.Min {
		a.Min = o.Min
	}
	if o.Max > a.Max {
		a.Max = o.Max
	}
}

// CodeAggs is the result of a per-code aggregate pass: one partial per
// dictionary code, dense over the dictionary. Exactly one slice is non-nil,
// matching the measure column's value kind. Codes with no selected rows hold
// the initial partial (Count 0), exactly what per-row accumulation into a
// fresh state would leave.
type CodeAggs struct {
	Numeric  []NumericCodeAgg
	Bool     []BoolCodeAgg
	Datetime []DatetimeCodeAgg
}

// PerCodeAggSource is implemented by dictionary-encoded columns that can
// compute per-code aggregate partials of a measure column by merging cached
// per-chunk summaries instead of scanning per row (the io.ReaderAt pattern —
// IDataColumn is not widened).
//
// handled reports whether the column could take the fast path for this
// (selection, measure) input; on false the caller must aggregate per row.
// Callers may index the result by the group keys of the same (column,
// selection) grouping pass: handled is false whenever group keys would not be
// dictionary codes (the small-subset hash cutover).
type PerCodeAggSource interface {
	PerCodeAggs(ctx context.Context, sel RowSet, measure IDataColumn) (aggs *CodeAggs, handled bool, err error)
}

// maxPreAggEntries bounds one cached summary's total (code, partial) entries
// across all chunks — the analogue of 5b's dense-partial budget. A dimension
// whose chunks hold too many distinct codes each (high cardinality in random
// order) declines pre-aggregation permanently rather than retaining
// O(chunks × distinct) state; declared dimensions on sorted storage stay far
// inside the budget (few codes per chunk). A var only so tests can exercise
// the decline without building a gigabyte of data.
var maxPreAggEntries = int64(1 << 20)

// preAggBuilds counts summary builds, for tests asserting that concurrent
// queries share one build and repeated queries rebuild nothing.
var preAggBuilds atomic.Int64

// codeAggChunk is one chunk's summary: the codes present in the chunk and one
// partial per present code, in first-appearance order.
type codeAggChunk[A any] struct {
	codes []uint32
	aggs  []A
}

// preAggEntry is the cached summary for one (dimension, measure) pair.
// summary holds a []codeAggChunk[A] (nil means the build declined
// permanently); done guards it. The entry mutex also serializes builds, so
// concurrent first queries share one scan; a build cancelled by its context
// leaves done false and the next query rebuilds.
type preAggEntry struct {
	mu      sync.Mutex
	done    bool
	summary any
}

// preAggEntryFor returns (creating if needed) the cache entry for measure.
func (c *ChunkedDictStringColumn[K]) preAggEntryFor(measure IDataColumn) *preAggEntry {
	c.preAggMu.Lock()
	defer c.preAggMu.Unlock()
	if c.preAggs == nil {
		c.preAggs = make(map[IDataColumn]*preAggEntry)
	}
	e := c.preAggs[measure]
	if e == nil {
		e = &preAggEntry{}
		c.preAggs[measure] = e
	}
	return e
}

// rangeClass classifies a row range against a selection.
type rangeClass uint8

const (
	rangeEmpty rangeClass = iota // no row in the range is selected
	rangeFull                    // every row in the range is selected
	rangeMixed                   // some rows are selected
)

// rangeClassifier reports how much of a row range a selection covers, so a
// chunk merge can take the cached partial (full), skip (empty), or scan only
// the selected rows (mixed). Implemented by the chunk-decomposable RowSets.
type rangeClassifier interface {
	rangeRowSet
	classifyRange(lo, hi int) rangeClass
}

func (a allRows) classifyRange(lo, hi int) rangeClass {
	n := int(a)
	switch {
	case lo >= n:
		return rangeEmpty
	case hi <= n:
		return rangeFull
	default:
		return rangeMixed
	}
}

func (s *Selection) classifyRange(lo, hi int) rangeClass {
	if hi > s.n {
		hi = s.n
	}
	if lo >= hi {
		return rangeEmpty
	}
	wLo, wLast := lo/64, (hi-1)/64
	sawFull, sawEmpty := false, false
	for w := wLo; w <= wLast; w++ {
		mask := ^uint64(0)
		if w == wLo && lo%64 != 0 {
			mask &= ^uint64(0) << uint(lo%64)
		}
		if w == wLast && hi%64 != 0 {
			mask &= (uint64(1) << uint(hi%64)) - 1
		}
		switch s.words[w] & mask {
		case mask:
			sawFull = true
		case 0:
			sawEmpty = true
		default:
			return rangeMixed
		}
		if sawFull && sawEmpty {
			return rangeMixed
		}
	}
	if sawFull {
		return rangeFull
	}
	return rangeEmpty
}

// PerCodeAggs implements PerCodeAggSource: per-dictionary-code aggregate
// partials of measure over sel, from per-chunk summaries built once per
// (dimension, measure) pair — under the entry lock, so concurrent queries
// share one build — and merged per the selection's chunk coverage.
//
// It declines (handled=false) when group keys for sel would not be dictionary
// codes (the smallGroupSubset hash cutover, where the selection is small
// enough that per-row work is cheap anyway), when sel is not
// chunk-decomposable (RowIndices), when measure is not a chunk-aligned typed
// storage column of a supported kind, or when the summary would exceed the
// entry budget.
func (c *ChunkedDictStringColumn[K]) PerCodeAggs(ctx context.Context, sel RowSet, measure IDataColumn) (*CodeAggs, bool, error) {
	if c.smallGroupSubset(sel) {
		return nil, false, nil
	}
	cls, ok := sel.(rangeClassifier)
	if !ok {
		return nil, false, nil
	}
	if mc, ok := measure.(IChunkedColumn); !ok ||
		mc.ChunkSize() != c.codes.chunkSize() ||
		mc.NumChunks() != c.codes.numChunks() ||
		measure.Length() != c.codes.len() {
		return nil, false, nil
	}
	d := len(c.dict)
	entry := c.preAggEntryFor(measure)
	switch m := measure.(type) {
	case interface{ Chunk(int) []float64 }:
		out, handled, err := perCodeAggsTyped(ctx, c, entry, m.Chunk, cls, d, initNumericCodeAgg,
			func(a *NumericCodeAgg, v float64) { a.add(v) }, (*NumericCodeAgg).combine)
		if !handled || err != nil {
			return nil, false, err
		}
		return &CodeAggs{Numeric: out}, true, nil
	case interface{ Chunk(int) []int64 }:
		out, handled, err := perCodeAggsTyped(ctx, c, entry, m.Chunk, cls, d, initNumericCodeAgg,
			func(a *NumericCodeAgg, v int64) { a.add(float64(v)) }, (*NumericCodeAgg).combine)
		if !handled || err != nil {
			return nil, false, err
		}
		return &CodeAggs{Numeric: out}, true, nil
	case interface{ Chunk(int) []uint64 }:
		out, handled, err := perCodeAggsTyped(ctx, c, entry, m.Chunk, cls, d, initNumericCodeAgg,
			func(a *NumericCodeAgg, v uint64) { a.add(float64(v)) }, (*NumericCodeAgg).combine)
		if !handled || err != nil {
			return nil, false, err
		}
		return &CodeAggs{Numeric: out}, true, nil
	case interface{ Chunk(int) []uint32 }:
		out, handled, err := perCodeAggsTyped(ctx, c, entry, m.Chunk, cls, d, initNumericCodeAgg,
			func(a *NumericCodeAgg, v uint32) { a.add(float64(v)) }, (*NumericCodeAgg).combine)
		if !handled || err != nil {
			return nil, false, err
		}
		return &CodeAggs{Numeric: out}, true, nil
	case interface{ Chunk(int) []bool }:
		out, handled, err := perCodeAggsTyped(ctx, c, entry, m.Chunk, cls, d, initBoolCodeAgg,
			func(a *BoolCodeAgg, v bool) { a.add(v) }, (*BoolCodeAgg).combine)
		if !handled || err != nil {
			return nil, false, err
		}
		return &CodeAggs{Bool: out}, true, nil
	case interface{ Chunk(int) []time.Time }:
		out, handled, err := perCodeAggsTyped(ctx, c, entry, m.Chunk, cls, d, initDatetimeCodeAgg,
			func(a *DatetimeCodeAgg, v time.Time) { a.add(v) }, (*DatetimeCodeAgg).combine)
		if !handled || err != nil {
			return nil, false, err
		}
		return &CodeAggs{Datetime: out}, true, nil
	default:
		return nil, false, nil
	}
}

// perCodeAggsTyped is the typed worker behind PerCodeAggs: it resolves (or
// builds) the cached per-chunk summary for one measure and merges it over the
// selection. handled=false means the summary build declined (budget).
func perCodeAggsTyped[K Unsigned, V any, A any](
	ctx context.Context,
	c *ChunkedDictStringColumn[K],
	entry *preAggEntry,
	chunkOf func(int) []V,
	cls rangeClassifier,
	d int,
	initA func() A,
	addA func(*A, V),
	combineA func(*A, *A),
) ([]A, bool, error) {
	summary, handled, err := summaryFor(ctx, entry, &c.codes, chunkOf, initA, addA)
	if !handled || err != nil {
		return nil, handled, err
	}

	out := make([]A, d)
	for i := range out {
		out[i] = initA()
	}
	chunkSize := c.codes.chunkSize()
	n := c.codes.len()
	for ci := range summary {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		lo := ci * chunkSize
		hi := lo + chunkSize
		if hi > n {
			hi = n
		}
		switch cls.classifyRange(lo, hi) {
		case rangeEmpty:
		case rangeFull:
			ch := &summary[ci]
			for k, code := range ch.codes {
				combineA(&out[code], &ch.aggs[k])
			}
		default:
			vals := chunkOf(ci)
			cls.forEachRowIn(lo, hi, func(i uint32) bool {
				addA(&out[c.codes.at(i)], vals[int(i)-lo])
				return true
			})
		}
	}
	return out, true, nil
}

// summaryFor returns entry's summary, building it on first use. The entry
// lock serializes builds and protects done/summary; a context error during
// the build leaves the entry unbuilt so a later query retries. handled=false
// (with done=true, summary=nil) records a permanent budget decline.
func summaryFor[K Unsigned, V any, A any](
	ctx context.Context,
	entry *preAggEntry,
	codes *chunkedData[K],
	chunkOf func(int) []V,
	initA func() A,
	addA func(*A, V),
) ([]codeAggChunk[A], bool, error) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !entry.done {
		summary, ok, err := buildSummary(ctx, codes, chunkOf, initA, addA)
		if err != nil {
			return nil, false, err
		}
		entry.done = true
		if ok {
			entry.summary = summary
		}
		preAggBuilds.Add(1)
	}
	if entry.summary == nil {
		return nil, false, nil
	}
	return entry.summary.([]codeAggChunk[A]), true, nil
}

// buildSummary computes every chunk's (code, partial) summary in one parallel
// pass over the dimension's codes and the measure's values — the "computed
// once" of §4. ok=false means the entry budget was exceeded and the pair
// should decline permanently.
func buildSummary[K Unsigned, V any, A any](
	ctx context.Context,
	codes *chunkedData[K],
	chunkOf func(int) []V,
	initA func() A,
	addA func(*A, V),
) ([]codeAggChunk[A], bool, error) {
	nc := codes.numChunks()
	summary := make([]codeAggChunk[A], nc)
	var entries atomic.Int64
	err := forEachChunk(ctx, nc, codes.chunkSize(), func(ci int) {
		if entries.Load() > maxPreAggEntries {
			return
		}
		cc := codes.chunk(ci)
		vals := chunkOf(ci)
		// Run-tracking accumulation: on storage sorted by the dimension a
		// chunk is a handful of runs, so the map is consulted only at run
		// breaks; random order degrades to one map probe per row, paid once
		// at build time.
		var outCodes []uint32
		var outAggs []A
		lookup := make(map[uint32]int, 8)
		idx := -1
		for j, ck := range cc {
			code := uint32(ck)
			if idx < 0 || outCodes[idx] != code {
				k, ok := lookup[code]
				if !ok {
					k = len(outCodes)
					outCodes = append(outCodes, code)
					outAggs = append(outAggs, initA())
					lookup[code] = k
				}
				idx = k
			}
			addA(&outAggs[idx], vals[j])
		}
		summary[ci] = codeAggChunk[A]{codes: outCodes, aggs: outAggs}
		entries.Add(int64(len(outCodes)))
	})
	if err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if entries.Load() > maxPreAggEntries {
		return nil, false, nil
	}
	return summary, true, nil
}
