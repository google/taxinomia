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
	"math"
	"sync"
	"testing"
	"time"
)

// preAggChunkSize keeps the parallel build path eligible (multiple chunks,
// size divisible by 64) at test row counts.
const preAggChunkSize = 64

// preAggDims returns a chunked dict dimension over n rows cycling through d
// values in runs of runLen (runLen 1 interleaves every code in every chunk).
func preAggDim(n, d, runLen int) *ChunkedDictStringColumn[uint16] {
	c := newChunkedDictStringColumn[uint16](NewColumnDef("dim", "Dim", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		c.Append(fmt.Sprintf("v%03d", (i/runLen)%d))
	}
	return c
}

// floatsEqual treats two NaNs as equal; everything else is exact equality
// (test data uses integer-valued floats, so sums are exact in any order).
func floatsEqual(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return a == b
}

// floatsNear allows last-ulp association differences for sums whose terms
// exceed 2^53 (epoch nanoseconds).
func floatsNear(a, b float64) bool {
	if floatsEqual(a, b) {
		return true
	}
	return math.Abs(a-b) <= 1e-12*math.Max(math.Abs(a), math.Abs(b))
}

func checkNumericAggs(t *testing.T, got, want []NumericCodeAgg) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length %d, want %d", len(got), len(want))
	}
	for code := range want {
		g, w := got[code], want[code]
		// Sum/SumSq allow last-ulp association differences (terms above 2^53
		// stop being exact — the documented caveat); everything else is exact.
		if g.Count != w.Count || !floatsNear(g.Sum, w.Sum) || !floatsNear(g.SumSq, w.SumSq) ||
			!floatsEqual(g.Min, w.Min) || !floatsEqual(g.Max, w.Max) {
			t.Errorf("code %d: got %+v, want %+v", code, g, w)
		}
	}
}

// refPerCodeAggs is the per-row reference: one selection pass through the
// public per-row accessors, accumulating with the same partial semantics.
func refPerCodeAggs[A any](c *ChunkedDictStringColumn[uint16], sel RowSet, d int, initA func() A, add func(*A, uint32)) []A {
	out := make([]A, d)
	for i := range out {
		out[i] = initA()
	}
	sel.ForEachRow(func(i uint32) bool {
		add(&out[c.GetCode(i)], i)
		return true
	})
	return out
}

// preAggSelections builds the selection shapes the merge distinguishes:
// full universe, all-set bitmap, whole chunks deselected, chunks cut through,
// and a mix of all three classes.
func preAggSelections(n int) map[string]RowSet {
	full := NewSelection(n)
	for i := 0; i < n; i++ {
		full.Add(uint32(i))
	}
	wholeChunksOut := NewSelection(n)
	for i := 0; i < n; i++ {
		if (i/preAggChunkSize)%2 == 0 {
			wholeChunksOut.Add(uint32(i))
		}
	}
	partialChunks := NewSelection(n)
	for i := 0; i < n; i += 3 {
		partialChunks.Add(uint32(i))
	}
	mixed := NewSelection(n)
	for i := 0; i < n; i++ {
		switch (i / preAggChunkSize) % 3 {
		case 0:
			mixed.Add(uint32(i))
		case 1:
			// chunk left empty
		default:
			if i%5 == 0 {
				mixed.Add(uint32(i))
			}
		}
	}
	return map[string]RowSet{
		"all rows":         AllRows(n),
		"full bitmap":      full,
		"whole chunks out": wholeChunksOut,
		"partial chunks":   partialChunks,
		"mixed classes":    mixed,
	}
}

func TestPerCodeAggsParityFloat64(t *testing.T) {
	const n, d = 1000, 6
	for _, runLen := range []int{1, 7, 200} {
		dim := preAggDim(n, d, runLen)
		measure := newChunkedFloat64Column(NewColumnDef("m", "M", ""), preAggChunkSize)
		specials := []float64{math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1), 0}
		for i := 0; i < n; i++ {
			if i%97 == 0 {
				measure.Append(specials[(i/97)%len(specials)])
			} else {
				measure.Append(float64(i%50 - 25))
			}
		}
		get := func(i uint32) float64 { v, _ := measure.GetValue(i); return v }
		for name, sel := range preAggSelections(n) {
			aggs, handled, err := dim.PerCodeAggs(context.Background(), sel, measure)
			if err != nil || !handled {
				t.Fatalf("runLen %d, %s: handled=%v err=%v", runLen, name, handled, err)
			}
			want := refPerCodeAggs(dim, sel, dim.Cardinality(), initNumericCodeAgg,
				func(a *NumericCodeAgg, i uint32) { a.add(get(i)) })
			checkNumericAggs(t, aggs.Numeric, want)
		}
	}
}

func TestPerCodeAggsParityIntWidths(t *testing.T) {
	const n, d = 700, 5
	dim := preAggDim(n, d, 13)
	i64 := newChunkedInt64Column(NewColumnDef("i64", "", ""), preAggChunkSize)
	u64 := newChunkedUint64Column(NewColumnDef("u64", "", ""), preAggChunkSize)
	u32 := newChunkedUint32Column(NewColumnDef("u32", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		i64.Append(int64(i*11 - 3000))
		u64.Append(uint64(i) * 1e9)
		u32.Append(uint32(i % 100))
	}
	geti64 := func(i uint32) float64 { v, _ := i64.GetValue(i); return float64(v) }
	getu64 := func(i uint32) float64 { v, _ := u64.GetValue(i); return float64(v) }
	getu32 := func(i uint32) float64 { v, _ := u32.GetValue(i); return float64(v) }
	for name, sel := range preAggSelections(n) {
		for _, m := range []struct {
			col IDataColumn
			get func(uint32) float64
		}{{i64, geti64}, {u64, getu64}, {u32, getu32}} {
			aggs, handled, err := dim.PerCodeAggs(context.Background(), sel, m.col)
			if err != nil || !handled {
				t.Fatalf("%s/%s: handled=%v err=%v", name, m.col.ColumnDef().Name(), handled, err)
			}
			want := refPerCodeAggs(dim, sel, d, initNumericCodeAgg,
				func(a *NumericCodeAgg, i uint32) { a.add(m.get(i)) })
			checkNumericAggs(t, aggs.Numeric, want)
		}
	}
}

func TestPerCodeAggsParityBool(t *testing.T) {
	const n, d = 500, 4
	dim := preAggDim(n, d, 9)
	measure := newChunkedBoolColumn(NewColumnDef("b", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(i%3 == 1)
	}
	for name, sel := range preAggSelections(n) {
		aggs, handled, err := dim.PerCodeAggs(context.Background(), sel, measure)
		if err != nil || !handled {
			t.Fatalf("%s: handled=%v err=%v", name, handled, err)
		}
		want := refPerCodeAggs(dim, sel, d, initBoolCodeAgg,
			func(a *BoolCodeAgg, i uint32) { v, _ := measure.GetValue(i); a.add(v) })
		if len(aggs.Bool) != len(want) {
			t.Fatalf("%s: length %d, want %d", name, len(aggs.Bool), len(want))
		}
		for code := range want {
			if aggs.Bool[code] != want[code] {
				t.Errorf("%s code %d: got %+v, want %+v", name, code, aggs.Bool[code], want[code])
			}
		}
	}
}

func TestPerCodeAggsParityDatetime(t *testing.T) {
	const n, d = 600, 5
	dim := preAggDim(n, d, 17)
	measure := newChunkedDatetimeColumn(NewColumnDef("t", "", ""), preAggChunkSize)
	base := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		if i%53 == 0 {
			measure.Append(time.Time{}) // zero time aggregates like any other value
		} else {
			measure.Append(base.Add(time.Duration(i) * time.Minute))
		}
	}
	for name, sel := range preAggSelections(n) {
		aggs, handled, err := dim.PerCodeAggs(context.Background(), sel, measure)
		if err != nil || !handled {
			t.Fatalf("%s: handled=%v err=%v", name, handled, err)
		}
		want := refPerCodeAggs(dim, sel, d, initDatetimeCodeAgg,
			func(a *DatetimeCodeAgg, i uint32) { v, _ := measure.GetValue(i); a.add(v) })
		if len(aggs.Datetime) != len(want) {
			t.Fatalf("%s: length %d, want %d", name, len(aggs.Datetime), len(want))
		}
		for code := range want {
			g, w := aggs.Datetime[code], want[code]
			// Epoch-nanosecond sums exceed 2^53, so chunk subtotals associate
			// with last-ulp differences — the documented caveat. Count and
			// the int64 bounds are exact; the formatted output (minute
			// granularity) is unaffected, as the tables-side parity pins.
			if g.Count != w.Count || !floatsNear(g.Sum, w.Sum) || !floatsNear(g.SumSq, w.SumSq) ||
				g.Min != w.Min || g.Max != w.Max {
				t.Errorf("%s code %d: got %+v, want %+v", name, code, g, w)
			}
		}
	}
}

// TestPerCodeAggsFinalizedParity pins that finalized columns (zone maps,
// sparse indexes) take the same path — the summary needs neither.
func TestPerCodeAggsFinalizedParity(t *testing.T) {
	const n, d = 400, 4
	dim := preAggDim(n, d, 25)
	dim.FinalizeColumn()
	measure := newChunkedInt64Column(NewColumnDef("m", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(int64(i))
	}
	measure.FinalizeColumn()
	aggs, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure)
	if err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	want := refPerCodeAggs(dim, AllRows(n), d, initNumericCodeAgg,
		func(a *NumericCodeAgg, i uint32) { v, _ := measure.GetValue(i); a.add(float64(v)) })
	checkNumericAggs(t, aggs.Numeric, want)
}

func TestPerCodeAggsDeclines(t *testing.T) {
	const n = 640
	dim := preAggDim(n, 4, 10)
	measure := newChunkedInt64Column(NewColumnDef("m", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(int64(i))
	}

	assertDeclined := func(name string, sel RowSet, m IDataColumn) {
		t.Helper()
		aggs, handled, err := dim.PerCodeAggs(context.Background(), sel, m)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", name, err)
		}
		if handled || aggs != nil {
			t.Errorf("%s: expected decline, got handled=%v aggs=%v", name, handled, aggs)
		}
	}

	// RowIndices selections are not chunk-decomposable.
	assertDeclined("row indices", RowIndices{1, 2, 3, 100, 200}, measure)

	// A plain (non-chunked) measure has no chunk geometry.
	plain := NewInt64Column(NewColumnDef("p", "", ""))
	for i := 0; i < n; i++ {
		plain.Append(int64(i))
	}
	assertDeclined("plain measure", AllRows(n), plain)

	// String measures have no mergeable partial (unique counts don't merge).
	str := newChunkedStringColumn(NewColumnDef("s", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		str.Append(fmt.Sprintf("s%d", i%7))
	}
	assertDeclined("string measure", AllRows(n), str)

	// Chunk-geometry mismatch.
	misaligned := newChunkedInt64Column(NewColumnDef("m2", "", ""), 2*preAggChunkSize)
	for i := 0; i < n; i++ {
		misaligned.Append(int64(i))
	}
	assertDeclined("chunk size mismatch", AllRows(n), misaligned)

	// Length mismatch.
	short := newChunkedInt64Column(NewColumnDef("m3", "", ""), preAggChunkSize)
	for i := 0; i < n/2; i++ {
		short.Append(int64(i))
	}
	assertDeclined("length mismatch", AllRows(n), short)

	// Small subset of a high-cardinality dictionary: group keys would be
	// hash-assigned, not dictionary codes.
	big := preAggDim(n, 320, 1)
	bigMeasure := newChunkedInt64Column(NewColumnDef("m4", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		bigMeasure.Append(int64(i))
	}
	tiny := NewSelection(n)
	for i := 0; i < 10; i++ {
		tiny.Add(uint32(i * 7))
	}
	if tiny.NumRows() >= big.Cardinality()/8 {
		t.Fatal("test setup: selection not small enough for the cutover")
	}
	if aggs, handled, err := big.PerCodeAggs(context.Background(), tiny, bigMeasure); err != nil || handled || aggs != nil {
		t.Errorf("small subset: expected decline, got handled=%v aggs=%v err=%v", handled, aggs, err)
	}
}

// TestPerCodeAggsBudgetDecline pins the entry budget: a dimension whose
// chunks each hold many distinct codes declines permanently, without
// rebuilding on later queries.
func TestPerCodeAggsBudgetDecline(t *testing.T) {
	old := maxPreAggEntries
	maxPreAggEntries = 8
	defer func() { maxPreAggEntries = old }()

	const n, d = 640, 16
	dim := preAggDim(n, d, 1) // every code in every chunk: 10 chunks x 16 codes >> 8
	measure := newChunkedInt64Column(NewColumnDef("m", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(int64(i))
	}
	before := preAggBuilds.Load()
	for call := 0; call < 2; call++ {
		aggs, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure)
		if err != nil {
			t.Fatalf("call %d: %v", call, err)
		}
		if handled || aggs != nil {
			t.Fatalf("call %d: expected budget decline", call)
		}
	}
	if got := preAggBuilds.Load() - before; got != 1 {
		t.Errorf("summary built %d times across a declined pair, want 1", got)
	}
}

// TestPerCodeAggsBuildOnce is the acceptance criterion: the per-chunk
// summaries are computed once per (dimension, measure) pair — later queries,
// whatever their selection, merge without rescanning fully-covered chunks.
func TestPerCodeAggsBuildOnce(t *testing.T) {
	const n, d = 960, 6
	dim := preAggDim(n, d, 31)
	m1 := newChunkedInt64Column(NewColumnDef("m1", "", ""), preAggChunkSize)
	m2 := newChunkedFloat64Column(NewColumnDef("m2", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		m1.Append(int64(i))
		m2.Append(float64(i % 10))
	}
	before := preAggBuilds.Load()
	for name, sel := range preAggSelections(n) {
		if _, handled, err := dim.PerCodeAggs(context.Background(), sel, m1); err != nil || !handled {
			t.Fatalf("%s: handled=%v err=%v", name, handled, err)
		}
	}
	if got := preAggBuilds.Load() - before; got != 1 {
		t.Errorf("summary built %d times for one (dim, measure) pair, want 1", got)
	}
	if _, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), m2); err != nil || !handled {
		t.Fatalf("second measure: handled=%v err=%v", handled, err)
	}
	if got := preAggBuilds.Load() - before; got != 2 {
		t.Errorf("summaries built %d times for two measures, want 2", got)
	}
}

// TestPerCodeAggsConcurrent runs concurrent first queries under -race:
// they must share one summary build and all see correct results.
func TestPerCodeAggsConcurrent(t *testing.T) {
	const n, d, workers = 1280, 5, 8
	dim := preAggDim(n, d, 41)
	measure := newChunkedInt64Column(NewColumnDef("m", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(int64(i * 3))
	}
	want := refPerCodeAggs(dim, AllRows(n), d, initNumericCodeAgg,
		func(a *NumericCodeAgg, i uint32) { v, _ := measure.GetValue(i); a.add(float64(v)) })

	before := preAggBuilds.Load()
	var wg sync.WaitGroup
	results := make([][]NumericCodeAgg, workers)
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			aggs, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure)
			if err != nil || !handled {
				errs[w] = fmt.Errorf("handled=%v err=%v", handled, err)
				return
			}
			results[w] = aggs.Numeric
		}(w)
	}
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", w, err)
		}
	}
	for w := range results {
		checkNumericAggs(t, results[w], want)
	}
	if got := preAggBuilds.Load() - before; got != 1 {
		t.Errorf("summary built %d times under concurrency, want 1", got)
	}
}

// TestPerCodeAggsCancellation: a cancelled build caches nothing and a later
// query rebuilds; a cancelled merge fails without invalidating the summary.
func TestPerCodeAggsCancellation(t *testing.T) {
	const n, d = 640, 4
	dim := preAggDim(n, d, 19)
	measure := newChunkedInt64Column(NewColumnDef("m", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(int64(i))
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := dim.PerCodeAggs(cancelled, AllRows(n), measure); err == nil {
		t.Fatal("cancelled build: expected error")
	}
	before := preAggBuilds.Load()
	aggs, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure)
	if err != nil || !handled {
		t.Fatalf("retry after cancelled build: handled=%v err=%v", handled, err)
	}
	if got := preAggBuilds.Load() - before; got != 1 {
		t.Errorf("retry built %d summaries, want 1 (cancelled build must cache nothing)", got)
	}
	want := refPerCodeAggs(dim, AllRows(n), d, initNumericCodeAgg,
		func(a *NumericCodeAgg, i uint32) { v, _ := measure.GetValue(i); a.add(float64(v)) })
	checkNumericAggs(t, aggs.Numeric, want)

	// Merge of the now-built summary under a cancelled context fails cleanly
	// and leaves the summary usable.
	if _, _, err := dim.PerCodeAggs(cancelled, AllRows(n), measure); err == nil {
		t.Fatal("cancelled merge: expected error")
	}
	if got := preAggBuilds.Load() - before; got != 1 {
		t.Errorf("cancelled merge triggered %d extra builds, want 0", got-1)
	}
	if _, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure); err != nil || !handled {
		t.Fatalf("after cancelled merge: handled=%v err=%v", handled, err)
	}
}

// TestPerCodeAggsSummaryIsSparse pins the retained-state shape of §4: on
// storage sorted by the dimension the summary holds O(chunks + distinct)
// entries, not O(rows).
func TestPerCodeAggsSummaryIsSparse(t *testing.T) {
	const n, d = 4096, 8
	dim := preAggDim(n, d, n/d) // sorted-by-dimension shape: long runs
	measure := newChunkedInt64Column(NewColumnDef("m", "", ""), preAggChunkSize)
	for i := 0; i < n; i++ {
		measure.Append(int64(i))
	}
	if _, handled, err := dim.PerCodeAggs(context.Background(), AllRows(n), measure); err != nil || !handled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
	entry := dim.preAggEntryFor(measure)
	summary := entry.summary.([]codeAggChunk[NumericCodeAgg])
	total := 0
	for _, ch := range summary {
		total += len(ch.codes)
	}
	nc := n / preAggChunkSize
	if maxEntries := nc + d; total > maxEntries {
		t.Errorf("summary holds %d entries for %d chunks x %d codes in runs, want <= %d", total, nc, d, maxEntries)
	}
}

func TestSelectionClassifyRange(t *testing.T) {
	n := 200
	s := NewSelection(n)
	// Words: [0,64) full, [64,128) empty, [128,192) mixed, tail [192,200) full.
	for i := 0; i < 64; i++ {
		s.Add(uint32(i))
	}
	for i := 128; i < 192; i += 2 {
		s.Add(uint32(i))
	}
	for i := 192; i < 200; i++ {
		s.Add(uint32(i))
	}
	cases := []struct {
		lo, hi int
		want   rangeClass
	}{
		{0, 64, rangeFull},
		{64, 128, rangeEmpty},
		{128, 192, rangeMixed},
		{0, 128, rangeMixed},  // full word + empty word
		{192, 200, rangeFull}, // tail word, hi == n
		{192, 256, rangeFull}, // hi clamped to n
		{200, 264, rangeEmpty},
		{0, 63, rangeFull}, // unaligned hi inside a full word
		{65, 128, rangeEmpty},
		{1, 2, rangeFull},
		{100, 101, rangeEmpty},
	}
	for _, c := range cases {
		if got := s.classifyRange(c.lo, c.hi); got != c.want {
			t.Errorf("classifyRange(%d, %d) = %d, want %d", c.lo, c.hi, got, c.want)
		}
	}

	ar := AllRows(100).(rangeClassifier)
	if got := ar.classifyRange(0, 64); got != rangeFull {
		t.Errorf("allRows [0,64) = %d, want full", got)
	}
	if got := ar.classifyRange(64, 128); got != rangeMixed {
		t.Errorf("allRows [64,128) = %d, want mixed", got)
	}
	if got := ar.classifyRange(128, 192); got != rangeEmpty {
		t.Errorf("allRows [128,192) = %d, want empty", got)
	}
}
