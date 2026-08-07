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
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// --- forEachRowIn ---

// rangeViaForEachRow is the reference: full iteration filtered to [lo, hi).
func rangeViaForEachRow(sel RowSet, lo, hi int) []uint32 {
	var got []uint32
	sel.ForEachRow(func(i uint32) bool {
		if int(i) >= lo && int(i) < hi {
			got = append(got, i)
		}
		return true
	})
	return got
}

// TestForEachRowIn: ranged iteration equals full iteration filtered to the
// range, for Selection and AllRows, across word-boundary edges, and stops
// early when f returns false.
func TestForEachRowIn(t *testing.T) {
	const n = 300
	sel := NewSelection(n)
	for i := 0; i < n; i += 3 {
		sel.Add(uint32(i))
	}
	sel.Add(63)
	sel.Add(64)
	sel.Add(65)

	sets := []struct {
		name string
		set  rangeRowSet
	}{
		{"selection", sel},
		{"allrows", allRows(n)},
	}
	ranges := [][2]int{{0, n}, {0, 64}, {64, 128}, {63, 65}, {1, 63}, {65, 300}, {128, 128}, {250, 400}, {-5, 10}}
	for _, s := range sets {
		for _, r := range ranges {
			var got []uint32
			s.set.forEachRowIn(r[0], r[1], func(i uint32) bool {
				got = append(got, i)
				return true
			})
			want := rangeViaForEachRow(s.set, r[0], r[1])
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s forEachRowIn(%d, %d) = %v, want %v", s.name, r[0], r[1], got, want)
			}
		}
		// Early stop: f returning false ends the iteration.
		count := 0
		s.set.forEachRowIn(0, n, func(i uint32) bool {
			count++
			return count < 5
		})
		if count != 5 {
			t.Errorf("%s forEachRowIn early stop visited %d rows, want 5", s.name, count)
		}
	}
}

// --- PartitionGroups parity ---

// partitionCases builds one column per representation over the same value
// sequence: 197 distinct values cycling over multiple word-aligned chunks so
// every span sees most groups and several groups first appear mid-span.
func partitionCases(t *testing.T, rows int) []struct {
	name string
	col  IDataColumn
} {
	t.Helper()
	value := func(i int) string { return fmt.Sprintf("val-%03d", i%197) }

	str := newChunkedStringColumn(NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		str.Append(value(i))
	}
	str.FinalizeColumn()

	arena := ArenaEncodeChunkedStringColumn(str)

	dict := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		dict.Append(value(i))
	}
	dict.FinalizeColumn()

	i64 := newChunkedInt64Column(NewColumnDef("n", "N", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		i64.Append(int64(i % 151))
	}
	i64.FinalizeColumn()

	f64 := newChunkedFloat64Column(NewColumnDef("f", "F", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		switch i % 7 {
		case 0:
			f64.Append(math.NaN())
		case 1:
			f64.Append(math.Copysign(0, -1))
		case 2:
			f64.Append(0)
		default:
			f64.Append(float64(i % 43))
		}
	}
	f64.FinalizeColumn()

	fcSrc := newChunkedStringColumn(NewColumnDef("pk", "PK", "id"), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		fcSrc.Append(fmt.Sprintf("key-%08d", i))
	}
	fcSrc.FinalizeColumn()
	fc, ok := FrontCodeChunkedStringColumn(fcSrc)
	if !ok {
		t.Fatal("FrontCodeChunkedStringColumn declined a sorted unique source")
	}

	plain := NewStringColumn(NewColumnDef("s", "S", ""))
	for i := 0; i < rows; i++ {
		plain.Append(value(i))
	}
	plain.FinalizeColumn()

	return []struct {
		name string
		col  IDataColumn
	}{
		{"chunked-string", str},
		{"arena", arena},
		{"dict", dict},
		{"chunked-int64", i64},
		{"chunked-float64", f64},
		{"front-coded", fc},
		{"plain-string-fallback", plain},
	}
}

// partitionSelections returns the selection shapes a partition must handle:
// the full universe, a patterned bitmap, a bitmap with empty chunks, and an
// explicit index list (which always takes the sequential path).
func partitionSelections(rows int) []struct {
	name string
	sel  RowSet
} {
	patterned := NewSelection(rows)
	for i := 0; i < rows; i++ {
		if i%3 == 0 || (i%parallelTestChunkSize) < 5 {
			patterned.Add(uint32(i))
		}
	}
	gappy := NewSelection(rows)
	for i := 0; i < rows; i++ {
		// Chunks 3..6 entirely empty; others dense.
		if ck := i / parallelTestChunkSize; ck < 3 || ck > 6 {
			gappy.Add(uint32(i))
		}
	}
	indices := make(RowIndices, 0, rows/5)
	for i := 0; i < rows; i += 5 {
		indices = append(indices, uint32(i))
	}
	return []struct {
		name string
		sel  RowSet
	}{
		{"all-rows", AllRows(rows)},
		{"patterned", patterned},
		{"empty-chunks", gappy},
		{"row-indices", indices},
	}
}

// TestPartitionGroupsParity: for every column representation and selection
// shape, the parallel partition is identical — counts, firsts, offsets and
// the full backing layout — to the sequential partition, and both agree with
// the column's own GroupCounts and GroupMembers (the contract that keeps
// partition codes valid for later GroupMembers calls).
func TestPartitionGroupsParity(t *testing.T) {
	const rows = 40*parallelTestChunkSize + 17
	ctx := context.Background()
	for _, tc := range partitionCases(t, rows) {
		for _, sc := range partitionSelections(rows) {
			t.Run(tc.name+"/"+sc.name, func(t *testing.T) {
				var want *GroupPartition
				var err error
				sequentially(func() {
					want, err = PartitionGroups(ctx, tc.col, nil, sc.sel)
				})
				if err != nil {
					t.Fatalf("sequential PartitionGroups: %v", err)
				}
				got, err := PartitionGroups(ctx, tc.col, nil, sc.sel)
				if err != nil {
					t.Fatalf("parallel PartitionGroups: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("parallel partition differs from sequential\ngot  counts=%v firsts=%v\nwant counts=%v firsts=%v",
						got.Counts, got.Firsts, want.Counts, want.Firsts)
				}

				ops := GroupOpsFor(tc.col, nil)
				var counts, firsts []uint32
				sequentially(func() {
					counts, firsts = ops.GroupCounts(sc.sel)
				})
				if !reflect.DeepEqual(got.Counts, counts) || !reflect.DeepEqual(got.Firsts, firsts) {
					t.Fatal("partition counts/firsts differ from GroupCounts")
				}
				// Spot-check the backing layout against GroupMembers for a
				// few codes, including the first and last non-empty ones.
				checked := 0
				for code := range got.Counts {
					if got.Counts[code] == 0 || (checked > 2 && code != len(got.Counts)-1) {
						continue
					}
					checked++
					start := got.Offsets[code]
					members := got.Backing[start : start+got.Counts[code]]
					var wantMembers []uint32
					sequentially(func() {
						wantMembers = ops.GroupMembers(sc.sel, uint32(code), 0, -1)
					})
					if !reflect.DeepEqual(members, wantMembers) {
						t.Fatalf("code %d: backing members %v != GroupMembers %v", code, members, wantMembers)
					}
				}
			})
		}
	}
}

// TestPartitionGroupsDictSubsetParity: a selection small enough to trip the
// dictionary's hash fallback uses first-appearance codes; the parallel
// partition must follow the same cutover and scheme.
func TestPartitionGroupsDictSubsetParity(t *testing.T) {
	const rows = 40 * parallelTestChunkSize
	dict := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		dict.Append(fmt.Sprintf("val-%04d", i%1200))
	}
	dict.FinalizeColumn()

	// Fewer selected rows than len(dict)/8 = 150, spread across chunks.
	sel := NewSelection(rows)
	for i := 0; i < rows; i += rows / 100 {
		sel.Add(uint32(i))
	}
	if !dict.smallGroupSubset(sel) {
		t.Fatal("test selection does not trip the small-subset fallback")
	}

	ctx := context.Background()
	var want *GroupPartition
	var err error
	sequentially(func() {
		want, err = PartitionGroups(ctx, dict, nil, sel)
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := PartitionGroups(ctx, dict, nil, sel)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("subset partition differs\ngot  counts=%v\nwant counts=%v", got.Counts, want.Counts)
	}
	if len(got.Counts) >= len(dict.dict) {
		t.Fatalf("subset partition has %d codes, want first-appearance codes (< %d dictionary entries)", len(got.Counts), len(dict.dict))
	}
}

// TestPartitionGroupsSingleChunkSequential: a single-chunk column declines the
// parallel path and still partitions correctly.
func TestPartitionGroupsSingleChunkSequential(t *testing.T) {
	c := newChunkedInt64Column(NewColumnDef("n", "N", ""), parallelTestChunkSize)
	for i := 0; i < 40; i++ {
		c.Append(int64(i % 4))
	}
	c.FinalizeColumn()
	part, err := PartitionGroups(context.Background(), c, nil, AllRows(40))
	if err != nil {
		t.Fatal(err)
	}
	if len(part.Counts) != 4 || part.Counts[0] != 10 {
		t.Fatalf("counts = %v, want four groups of 10", part.Counts)
	}
}

// TestPartitionGroupsEmptySelection: no rows selected, no groups.
func TestPartitionGroupsEmptySelection(t *testing.T) {
	const rows = 4 * parallelTestChunkSize
	c := newChunkedInt64Column(NewColumnDef("n", "N", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		c.Append(int64(i % 4))
	}
	c.FinalizeColumn()
	part, err := PartitionGroups(context.Background(), c, nil, NewSelection(rows))
	if err != nil {
		t.Fatal(err)
	}
	if len(part.Counts) != 0 || len(part.Backing) != 0 {
		t.Fatalf("partition of empty selection: counts=%v backing=%v, want empty", part.Counts, part.Backing)
	}
}

// TestPartitionGroupsPreCancelled: a cancelled context is reported by every
// representation, native and fallback, without producing a partition.
func TestPartitionGroupsPreCancelled(t *testing.T) {
	const rows = 8 * parallelTestChunkSize
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range partitionCases(t, rows) {
		part, err := PartitionGroups(ctx, tc.col, nil, AllRows(rows))
		if part != nil || err != context.Canceled {
			t.Errorf("%s: PartitionGroups on cancelled ctx = (%v, %v), want (nil, Canceled)", tc.name, part, err)
		}
	}
}

// countingCtx is a context whose Err flips to Canceled after a fixed number
// of Err calls, making mid-partition cancellation deterministic: span bodies
// and the executor both poll Err, so the flip lands while chunks remain.
type countingCtx struct {
	context.Context
	calls  atomic.Int64
	budget int64
}

func (c *countingCtx) Err() error {
	if c.calls.Add(1) > c.budget {
		return context.Canceled
	}
	return nil
}

func (c *countingCtx) Done() <-chan struct{} { return nil }

func (c *countingCtx) Deadline() (time.Time, bool) { return time.Time{}, false }

// TestPartitionGroupsMidRunCancellation: a context cancelled partway through
// the partition surfaces as an error for both the hash and the dense path.
func TestPartitionGroupsMidRunCancellation(t *testing.T) {
	const rows = 200 * parallelTestChunkSize
	i64 := newChunkedInt64Column(NewColumnDef("n", "N", ""), parallelTestChunkSize)
	dict := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), parallelTestChunkSize)
	for i := 0; i < rows; i++ {
		i64.Append(int64(i % 100))
		dict.Append(fmt.Sprintf("v%02d", i%100))
	}
	i64.FinalizeColumn()
	dict.FinalizeColumn()

	for _, tc := range []struct {
		name string
		col  IDataColumn
	}{{"hash", i64}, {"dense-dict", dict}} {
		ctx := &countingCtx{Context: context.Background(), budget: 20}
		part, err := PartitionGroups(ctx, tc.col, nil, AllRows(rows))
		if part != nil || err != context.Canceled {
			t.Errorf("%s: mid-run cancel = (%v, %v), want (nil, Canceled)", tc.name, part, err)
		}
	}
}

// BenchmarkPartitionGroups compares the sequential and parallel partition on
// a 1M-row column, for the hash path (int64, 1000 groups) and the dense
// dictionary path (1000 distinct values).
func BenchmarkPartitionGroups(b *testing.B) {
	const rows = 1 << 20
	i64 := NewChunkedInt64Column(NewColumnDef("n", "N", ""))
	dict := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), DefaultChunkSize)
	for i := 0; i < rows; i++ {
		i64.Append(int64(i % 1000))
		dict.Append(fmt.Sprintf("val-%04d", i%1000))
	}
	i64.FinalizeColumn()
	dict.FinalizeColumn()
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		col  IDataColumn
	}{{"hash-int64", i64}, {"dense-dict", dict}} {
		b.Run(tc.name+"/sequential", func(b *testing.B) {
			sequentially(func() {
				for i := 0; i < b.N; i++ {
					if _, err := PartitionGroups(ctx, tc.col, nil, AllRows(rows)); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
		b.Run(tc.name+"/parallel", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := PartitionGroups(ctx, tc.col, nil, AllRows(rows)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
