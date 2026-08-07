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
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
)

// parallelTestChunkSize is word-aligned (a multiple of 64), so forEachChunk
// takes the parallel executor path — the same path DefaultChunkSize columns
// take — while keeping the test data small enough to build many chunks.
const parallelTestChunkSize = 64

// sequentially runs f with the parallel scan path disabled and restores it.
func sequentially(f func()) {
	disableParallelScan = true
	defer func() { disableParallelScan = false }()
	f()
}

// TestForEachChunkVisitsEveryChunkOnce: the span batching hands every chunk
// index to exactly one body call, for sizes around every boundary.
func TestForEachChunkVisitsEveryChunkOnce(t *testing.T) {
	for _, nc := range []int{1, 2, 3, 16, 17, 64, 1000} {
		visits := make([]int32, nc)
		if err := forEachChunk(context.Background(), nc, parallelTestChunkSize, func(ci int) {
			atomic.AddInt32(&visits[ci], 1)
		}); err != nil {
			t.Fatalf("nc=%d: %v", nc, err)
		}
		for ci, v := range visits {
			if v != 1 {
				t.Fatalf("nc=%d: chunk %d visited %d times, want 1", nc, ci, v)
			}
		}
	}
}

// buildParityStringSource builds a finalized multi-chunk ChunkedStringColumn
// whose values repeat (so equality filters match many rows across chunks).
func buildParityStringSource(rows int) *ChunkedStringColumn {
	c := newChunkedStringColumn(NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		c.Append(fmt.Sprintf("val-%03d", i%97))
	}
	c.FinalizeColumn()
	return c
}

// stringFilterable is the shared surface of the four string-valued column
// representations under test.
type stringFilterable interface {
	FilterSelection(func(string) bool) *Selection
	FilterSelectionEqual(string) *Selection
	FilterSelectionIn([]string) *Selection
	FilterSelectionRange(lo, hi *string) *Selection
}

// TestParallelFilterParityString: for every string column representation,
// the parallel scan produces bit-identical selections to the sequential
// scan, for all four filter operations.
func TestParallelFilterParityString(t *testing.T) {
	const rows = 40*parallelTestChunkSize + 17

	src := buildParityStringSource(rows)
	arena := ArenaEncodeChunkedStringColumn(src)

	dict := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		v, _ := src.GetString(uint32(i))
		dict.Append(v)
	}
	dict.FinalizeColumn()

	// Front-coding needs unique sorted values: a separate sorted source.
	fcSrc := newChunkedStringColumn(NewColumnDef("pk", "PK", "id"), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		fcSrc.Append(fmt.Sprintf("key-%08d", i))
	}
	fcSrc.FinalizeColumn()
	fc, ok := FrontCodeChunkedStringColumn(fcSrc)
	if !ok {
		t.Fatal("FrontCodeChunkedStringColumn declined a sorted unique source")
	}

	lo, hi := "val-020", "val-060"
	fcLo, fcHi := "key-00000100", "key-00002000"
	cases := []struct {
		name   string
		col    stringFilterable
		eq     string
		in     []string
		lo, hi string
	}{
		{"chunked", src, "val-042", []string{"val-001", "val-090", "absent"}, lo, hi},
		{"arena", arena, "val-042", []string{"val-001", "val-090", "absent"}, lo, hi},
		{"dict", dict, "val-042", []string{"val-001", "val-090", "absent"}, lo, hi},
		{"front-coded", fc, "key-00000123", []string{"key-00000001", "key-00033000", "absent"}, fcLo, fcHi},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wantPred, wantEq, wantIn, wantRange *Selection
			sequentially(func() {
				wantPred = tc.col.FilterSelection(func(v string) bool { return v > tc.eq })
				wantEq = tc.col.FilterSelectionEqual(tc.eq)
				wantIn = tc.col.FilterSelectionIn(tc.in)
				wantRange = tc.col.FilterSelectionRange(&tc.lo, &tc.hi)
			})
			if got := tc.col.FilterSelection(func(v string) bool { return v > tc.eq }); !selectionsEqual(got, wantPred) {
				t.Error("FilterSelection: parallel != sequential")
			}
			if got := tc.col.FilterSelectionEqual(tc.eq); !selectionsEqual(got, wantEq) {
				t.Error("FilterSelectionEqual: parallel != sequential")
			}
			if got := tc.col.FilterSelectionIn(tc.in); !selectionsEqual(got, wantIn) {
				t.Error("FilterSelectionIn: parallel != sequential")
			}
			if got := tc.col.FilterSelectionRange(&tc.lo, &tc.hi); !selectionsEqual(got, wantRange) {
				t.Error("FilterSelectionRange: parallel != sequential")
			}
		})
	}
}

// TestParallelFilterParityNumeric: parallel/sequential parity on a numeric
// column including NaN semantics, finalized (zone maps prune) and
// unfinalized (every chunk scanned).
func TestParallelFilterParityNumeric(t *testing.T) {
	const rows = 40*parallelTestChunkSize + 17
	build := func(finalize bool) *ChunkedFloat64Column {
		c := newChunkedFloat64Column(NewColumnDef("v", "V", ""), parallelTestChunkSize)
		for i := 0; i < rows; i++ {
			switch i % 11 {
			case 3:
				c.Append(math.NaN())
			case 5:
				c.Append(math.Copysign(0, -1))
			default:
				c.Append(float64(i % 199))
			}
		}
		if finalize {
			c.FinalizeColumn()
		}
		return c
	}
	for _, finalized := range []bool{true, false} {
		c := build(finalized)
		lo, hi := 20.0, 60.0
		var wantEq, wantNaN, wantIn, wantRange *Selection
		sequentially(func() {
			wantEq = c.FilterSelectionEqual(42)
			wantNaN = c.FilterSelectionEqual(math.NaN())
			wantIn = c.FilterSelectionIn([]float64{0, 7, 198, 1e9})
			wantRange = c.FilterSelectionRange(&lo, &hi)
		})
		if got := c.FilterSelectionEqual(42); !selectionsEqual(got, wantEq) {
			t.Errorf("finalized=%v Equal: parallel != sequential", finalized)
		}
		if got := c.FilterSelectionEqual(math.NaN()); !selectionsEqual(got, wantNaN) {
			t.Errorf("finalized=%v Equal(NaN): parallel != sequential", finalized)
		}
		if got := c.FilterSelectionIn([]float64{0, 7, 198, 1e9}); !selectionsEqual(got, wantIn) {
			t.Errorf("finalized=%v In: parallel != sequential", finalized)
		}
		if got := c.FilterSelectionRange(&lo, &hi); !selectionsEqual(got, wantRange) {
			t.Errorf("finalized=%v Range: parallel != sequential", finalized)
		}
	}
}

// TestFilterContextPreCancelled: every Context filter variant returns
// (nil, context.Canceled) under an already-cancelled context, for every
// column representation.
func TestFilterContextPreCancelled(t *testing.T) {
	const rows = 4*parallelTestChunkSize + 5
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ic := newChunkedInt64Column(NewColumnDef("v", "V", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		ic.Append(int64(i % 50))
	}
	ic.FinalizeColumn()

	src := buildParityStringSource(rows)
	arena := ArenaEncodeChunkedStringColumn(src)
	fcSrc := newChunkedStringColumn(NewColumnDef("pk", "PK", "id"), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		fcSrc.Append(fmt.Sprintf("key-%08d", i))
	}
	fcSrc.FinalizeColumn()
	fc, _ := FrontCodeChunkedStringColumn(fcSrc)
	dict := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		v, _ := src.GetString(uint32(i))
		dict.Append(v)
	}
	dict.FinalizeColumn()

	check := func(name string, sel *Selection, err error) {
		t.Helper()
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", name, err)
		}
		if sel != nil {
			t.Errorf("%s: returned a selection alongside the error", name)
		}
	}
	var lo, hi = "a", "z"
	var ilo, ihi int64 = 1, 40

	s, err := ic.FilterSelectionContext(ctx, func(int64) bool { return true })
	check("int64 predicate", s, err)
	s, err = ic.FilterSelectionEqualContext(ctx, 7)
	check("int64 equal", s, err)
	s, err = ic.FilterSelectionInContext(ctx, []int64{1, 2})
	check("int64 in", s, err)
	s, err = ic.FilterSelectionRangeContext(ctx, &ilo, &ihi)
	check("int64 range", s, err)

	s, err = src.FilterSelectionEqualContext(ctx, "val-001")
	check("string equal", s, err)
	s, err = arena.FilterSelectionEqualContext(ctx, "val-001")
	check("arena equal", s, err)
	s, err = arena.FilterSelectionInContext(ctx, []string{"val-001"})
	check("arena in", s, err)
	s, err = arena.FilterSelectionRangeContext(ctx, &lo, &hi)
	check("arena range", s, err)
	s, err = arena.FilterSelectionContext(ctx, func(string) bool { return true })
	check("arena predicate", s, err)
	s, err = dict.FilterSelectionEqualContext(ctx, "val-001")
	check("dict equal", s, err)
	s, err = dict.FilterSelectionInContext(ctx, []string{"val-001"})
	check("dict in", s, err)
	s, err = dict.FilterSelectionRangeContext(ctx, &lo, &hi)
	check("dict range", s, err)
	s, err = dict.FilterSelectionContext(ctx, func(string) bool { return true })
	check("dict predicate", s, err)
	s, err = fc.FilterSelectionEqualContext(ctx, "key-00000001")
	check("front-coded equal", s, err)
	s, err = fc.FilterSelectionInContext(ctx, []string{"key-00000001"})
	check("front-coded in", s, err)
	s, err = fc.FilterSelectionRangeContext(ctx, &lo, &hi)
	check("front-coded range", s, err)
	s, err = fc.FilterSelectionContext(ctx, func(string) bool { return true })
	check("front-coded predicate", s, err)
}

// TestSequentialFallbackObservesContext: on a column whose chunk size is not
// word-aligned (the sequential path), a context cancelled mid-scan stops the
// scan at the next chunk boundary with ctx.Err().
func TestSequentialFallbackObservesContext(t *testing.T) {
	c := newChunkedInt64Column(NewColumnDef("v", "V", ""), 8)
	for i := 0; i < 80; i++ {
		c.Append(int64(i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	sel, err := c.FilterSelectionContext(ctx, func(v int64) bool {
		calls++
		if calls == 1 {
			cancel()
		}
		return true
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if sel != nil {
		t.Fatal("returned a selection alongside the error")
	}
	if calls > 8 {
		t.Fatalf("predicate ran %d times after cancellation, want at most one chunk (8)", calls)
	}
}

// BenchmarkParallelFilter compares the sequential and parallel scan paths on
// a 1M-row column at DefaultChunkSize (16 chunks): the opaque-predicate scan
// (unpruned worst case) and unsorted equality (unpruned structured scan).
func BenchmarkParallelFilter(b *testing.B) {
	const rows = 1 << 20
	c := newChunkedInt64Column(NewColumnDef("v", "V", ""), DefaultChunkSize)
	for i := 0; i < rows; i++ {
		c.Append(int64((i * 2654435761) % 1000003))
	}
	c.FinalizeColumn()

	run := func(name string, f func()) {
		b.Run(name+"/sequential", func(b *testing.B) {
			sequentially(func() {
				for i := 0; i < b.N; i++ {
					f()
				}
			})
		})
		b.Run(name+"/parallel", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f()
			}
		})
	}
	run("Predicate1M", func() { c.FilterSelection(func(v int64) bool { return v < 500000 }) })
	run("EqualUnsorted1M", func() { c.FilterSelectionEqual(42) })
}
