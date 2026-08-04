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
	"reflect"
	"testing"
)

func selectionsEqual(a, b *Selection) bool {
	return a.Len() == b.Len() && reflect.DeepEqual(a.ToIndices(), b.ToIndices())
}

// --- bounds ---

func TestChunkBoundsInt64(t *testing.T) {
	c := newChunkedInt64Column(NewColumnDef("v", "V", ""), testChunkSize)
	// Chunk 0: 0..7 in order; chunk 1: descending; chunk 2: partial.
	for i := 0; i < 8; i++ {
		c.Append(int64(i))
	}
	for i := 0; i < 8; i++ {
		c.Append(int64(100 - i))
	}
	c.Append(42)
	c.Append(-3)

	if _, _, ok := c.ChunkBounds(0); ok {
		t.Fatal("ChunkBounds before FinalizeColumn should report ok=false")
	}
	c.FinalizeColumn()

	want := [][2]int64{{0, 7}, {93, 100}, {-3, 42}}
	for ci, w := range want {
		min, max, ok := c.ChunkBounds(ci)
		if !ok || min != w[0] || max != w[1] {
			t.Fatalf("ChunkBounds(%d) = (%d, %d, %v), want (%d, %d, true)", ci, min, max, ok, w[0], w[1])
		}
	}
}

func TestChunkBoundsAllNaNChunk(t *testing.T) {
	c := newChunkedFloat64Column(NewColumnDef("v", "V", ""), 4)
	// Chunk 0: all NaN. Chunk 1: NaN mixed with numbers. Chunk 2: numbers.
	for i := 0; i < 4; i++ {
		c.Append(math.NaN())
	}
	c.Append(math.NaN())
	c.Append(2.5)
	c.Append(-1.5)
	c.Append(math.NaN())
	c.Append(7)
	c.Append(8)
	c.FinalizeColumn()

	if _, _, ok := c.ChunkBounds(0); ok {
		t.Fatal("all-NaN chunk should have no bounds")
	}
	if min, max, ok := c.ChunkBounds(1); !ok || min != -1.5 || max != 2.5 {
		t.Fatalf("ChunkBounds(1) = (%v, %v, %v), want (-1.5, 2.5, true)", min, max, ok)
	}

	// A NaN target matches exactly the NaN rows, including those in the
	// all-NaN chunk (which range queries must never visit).
	nan := c.FilterSelectionEqual(math.NaN())
	if want := c.FilterSelection(math.IsNaN); !selectionsEqual(want, nan) {
		t.Fatalf("Equal(NaN) = %v, want %v", nan.ToIndices(), want.ToIndices())
	}
	all := c.FilterSelectionRange(nil, nil)
	if want := c.FilterSelection(func(x float64) bool { return !math.IsNaN(x) }); !selectionsEqual(want, all) {
		t.Fatalf("Range(nil, nil) = %v, want %v", all.ToIndices(), want.ToIndices())
	}
}

// --- parity: pruned filters must agree with the naive predicate scan ---

type prunedFilterColumn[T any] interface {
	FilterSelection(func(T) bool) *Selection
	FilterSelectionEqual(T) *Selection
	FilterSelectionIn([]T) *Selection
	FilterSelectionRange(lo, hi *T) *Selection
}

// checkPrunedFilterParity checks Equal/In/Range against FilterSelection with
// the equivalent predicate. eq is the column's grouping equality; cmpf its
// ordering; unordered (may be nil) marks values outside the ordering, and
// bounds lists the values usable as range endpoints.
func checkPrunedFilterParity[T any](t *testing.T, col prunedFilterColumn[T],
	targets []T, bounds []T, eq func(T, T) bool, cmpf func(T, T) int, unordered func(T) bool) {
	t.Helper()

	for i, v := range targets {
		v := v
		want := col.FilterSelection(func(x T) bool { return eq(x, v) })
		if got := col.FilterSelectionEqual(v); !selectionsEqual(want, got) {
			t.Fatalf("FilterSelectionEqual(target %d = %v): got %v, want %v", i, v, got.ToIndices(), want.ToIndices())
		}
	}

	wantIn := col.FilterSelection(func(x T) bool {
		for _, v := range targets {
			if eq(x, v) {
				return true
			}
		}
		return false
	})
	if got := col.FilterSelectionIn(targets); !selectionsEqual(wantIn, got) {
		t.Fatalf("FilterSelectionIn(%v): got %v, want %v", targets, got.ToIndices(), wantIn.ToIndices())
	}
	if got := col.FilterSelectionIn(nil); got.Count() != 0 {
		t.Fatalf("FilterSelectionIn(nil): got %d rows, want 0", got.Count())
	}

	inRange := func(x T, lo, hi *T) bool {
		if unordered != nil && unordered(x) {
			return false
		}
		return (lo == nil || cmpf(x, *lo) >= 0) && (hi == nil || cmpf(x, *hi) <= 0)
	}
	var rangePairs [][2]*T
	rangePairs = append(rangePairs, [2]*T{nil, nil})
	for i := range bounds {
		lo, hi := &bounds[i], &bounds[i]
		rangePairs = append(rangePairs, [2]*T{lo, nil}, [2]*T{nil, hi}, [2]*T{lo, hi})
		for j := range bounds {
			if i != j {
				rangePairs = append(rangePairs, [2]*T{&bounds[i], &bounds[j]})
			}
		}
	}
	for _, p := range rangePairs {
		lo, hi := p[0], p[1]
		want := col.FilterSelection(func(x T) bool { return inRange(x, lo, hi) })
		if got := col.FilterSelectionRange(lo, hi); !selectionsEqual(want, got) {
			t.Fatalf("FilterSelectionRange(%v, %v): got %v, want %v", lo, hi, got.ToIndices(), want.ToIndices())
		}
	}
}

func TestPrunedFilterParity(t *testing.T) {
	const n = 53 // odd: a partial final chunk at chunk size 8
	def := NewColumnDef("v", "V", "")

	t.Run("int64", func(t *testing.T) {
		for _, sorted := range []bool{false, true} {
			c := newChunkedInt64Column(def, testChunkSize)
			for i := 0; i < n; i++ {
				if sorted {
					c.Append(int64(i / 3))
				} else {
					c.Append(int64((i*37)%17) - 5)
				}
			}
			c.FinalizeColumn()
			targets := []int64{-5, 0, 5, 11, 99}
			checkPrunedFilterParity[int64](t, c, targets, targets,
				func(a, b int64) bool { return a == b },
				func(a, b int64) int { return int(a - b) }, nil)
		}
	})

	t.Run("uint64", func(t *testing.T) {
		c := newChunkedUint64Column(def, testChunkSize)
		for i := 0; i < n; i++ {
			c.Append(uint64((i * 37) % 17))
		}
		c.FinalizeColumn()
		targets := []uint64{0, 7, 16, 99}
		checkPrunedFilterParity[uint64](t, c, targets, targets,
			func(a, b uint64) bool { return a == b },
			func(a, b uint64) int { return int(int64(a) - int64(b)) }, nil)
	})

	t.Run("uint32", func(t *testing.T) {
		c := newChunkedUint32Column(def, testChunkSize)
		for i := 0; i < n; i++ {
			c.Append(uint32((i * 37) % 17))
		}
		c.FinalizeColumn()
		targets := []uint32{0, 7, 16, 99}
		checkPrunedFilterParity[uint32](t, c, targets, targets,
			func(a, b uint32) bool { return a == b },
			func(a, b uint32) int { return int(int64(a) - int64(b)) }, nil)
	})

	t.Run("string", func(t *testing.T) {
		c := newChunkedStringColumn(def, testChunkSize)
		for i := 0; i < n; i++ {
			c.Append(fmt.Sprintf("v%02d", (i*37)%13))
		}
		c.FinalizeColumn()
		targets := []string{"v00", "v05", "v12", "zz", ""}
		checkPrunedFilterParity[string](t, c, targets, targets,
			func(a, b string) bool { return a == b },
			func(a, b string) int {
				switch {
				case a < b:
					return -1
				case a > b:
					return 1
				}
				return 0
			}, nil)
	})

	t.Run("bool", func(t *testing.T) {
		c := newChunkedBoolColumn(def, testChunkSize)
		for i := 0; i < n; i++ {
			c.Append(i%3 == 0)
		}
		c.FinalizeColumn()
		targets := []bool{false, true}
		checkPrunedFilterParity[bool](t, c, targets, targets,
			func(a, b bool) bool { return a == b }, compareBool, nil)
	})

	t.Run("float64", func(t *testing.T) {
		c := newChunkedFloat64Column(def, testChunkSize)
		for i := 0; i < n; i++ {
			switch i % 9 {
			case 0:
				c.Append(math.NaN())
			case 1:
				c.Append(math.Inf(1))
			case 2:
				c.Append(math.Inf(-1))
			case 3:
				c.Append(math.Copysign(0, -1))
			case 4:
				c.Append(0)
			default:
				c.Append(float64((i*37)%17) - 5.5)
			}
		}
		c.FinalizeColumn()
		eq := func(a, b float64) bool {
			if math.IsNaN(a) && math.IsNaN(b) {
				return true
			}
			return a == b
		}
		targets := []float64{math.NaN(), 0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), 3.5, 99.5}
		bounds := []float64{math.Inf(-1), -2.5, 0, 3.5, math.Inf(1)}
		checkPrunedFilterParity[float64](t, c, targets, bounds, eq,
			func(a, b float64) int {
				switch {
				case a < b:
					return -1
				case a > b:
					return 1
				}
				return 0
			}, math.IsNaN)
	})

	t.Run("dict", func(t *testing.T) {
		check := func(t *testing.T, c prunedFilterColumn[string]) {
			targets := []string{"g00", "g04", "g11", "absent", ""}
			checkPrunedFilterParity[string](t, c, targets, targets,
				func(a, b string) bool { return a == b },
				func(a, b string) int {
					switch {
					case a < b:
						return -1
					case a > b:
						return 1
					}
					return 0
				}, nil)
		}
		t.Run("uint8", func(t *testing.T) {
			c := newChunkedDictStringColumn[uint8](def, testChunkSize)
			for i := 0; i < n; i++ {
				c.Append(fmt.Sprintf("g%02d", (i*37)%12))
			}
			c.FinalizeColumn()
			check(t, c)
		})
		t.Run("uint16 key", func(t *testing.T) {
			// Every value distinct: the column becomes a key and retains its
			// interning map, exercising the index path of lookupCode.
			c := newChunkedDictStringColumn[uint16](def, testChunkSize)
			for i := 0; i < n; i++ {
				c.Append(fmt.Sprintf("g%02d", i))
			}
			c.FinalizeColumn()
			if !c.IsKey() {
				t.Fatal("distinct dict column should be a key")
			}
			check(t, c)
		})
	})
}

// --- pruning actually skips chunks ---
//
// Zone maps are recorded at FinalizeColumn and the columns are immutable
// afterwards, so pruning is semantically invisible. To observe it, these
// tests break that immutability on purpose: they plant a matching value into
// a chunk whose recorded bounds exclude it. The pruned filter must not see
// the planted row — proof the chunk was skipped, not scanned — while the
// naive predicate scan finds it.

func TestZonePruningSkipsChunksInt64(t *testing.T) {
	build := func() *ChunkedInt64Column {
		c := newChunkedInt64Column(NewColumnDef("v", "V", ""), testChunkSize)
		for i := 0; i < 4*testChunkSize; i++ {
			c.Append(int64(i))
		}
		return c
	}

	c := build()
	c.FinalizeColumn()
	// Chunk 3's bounds are [24, 31]; row 24 now holds 5, outside them.
	c.chunkedColumn.data.chunks[3][0] = 5

	naive := c.FilterSelection(func(v int64) bool { return v == 5 })
	if got := naive.ToIndices(); !reflect.DeepEqual(got, []uint32{5, 24}) {
		t.Fatalf("naive scan: got %v, want [5 24]", got)
	}
	if got := c.FilterSelectionEqual(5).ToIndices(); !reflect.DeepEqual(got, []uint32{5}) {
		t.Fatalf("Equal(5): got %v, want [5] — chunk 3 was scanned, not pruned", got)
	}
	if got := c.FilterSelectionIn([]int64{5}).ToIndices(); !reflect.DeepEqual(got, []uint32{5}) {
		t.Fatalf("In([5]): got %v, want [5]", got)
	}
	lo, hi := int64(4), int64(6)
	if got := c.FilterSelectionRange(&lo, &hi).ToIndices(); !reflect.DeepEqual(got, []uint32{4, 5, 6}) {
		t.Fatalf("Range(4, 6): got %v, want [4 5 6]", got)
	}

	// Without FinalizeColumn there are no zone maps: every chunk is scanned
	// and the planted row is found. Pruning is an optimization, never a
	// requirement.
	u := build()
	u.chunkedColumn.data.chunks[3][0] = 5
	if got := u.FilterSelectionEqual(5).ToIndices(); !reflect.DeepEqual(got, []uint32{5, 24}) {
		t.Fatalf("unfinalized Equal(5): got %v, want [5 24]", got)
	}
}

func TestZonePruningSkipsChunksBool(t *testing.T) {
	c := newChunkedBoolColumn(NewColumnDef("v", "V", ""), testChunkSize)
	for i := 0; i < 2*testChunkSize; i++ {
		c.Append(i >= testChunkSize) // chunk 0 all false, chunk 1 all true
	}
	c.FinalizeColumn()
	c.chunkedColumn.data.chunks[0][3] = true

	if got := c.FilterSelection(func(v bool) bool { return v }).Count(); got != testChunkSize+1 {
		t.Fatalf("naive scan: got %d rows, want %d", got, testChunkSize+1)
	}
	if got := c.FilterSelectionEqual(true).Count(); got != testChunkSize {
		t.Fatalf("Equal(true): got %d rows, want %d — all-false chunk was scanned, not pruned", got, testChunkSize)
	}
}

func TestZonePruningSkipsChunksDict(t *testing.T) {
	c := newChunkedDictStringColumn[uint8](NewColumnDef("v", "V", ""), testChunkSize)
	values := []string{"aa", "bb", "cc", "dd"}
	for _, v := range values {
		for i := 0; i < testChunkSize; i++ {
			c.Append(v)
		}
	}
	c.FinalizeColumn()

	// Chunk 3's bounds are ["dd", "dd"]; plant the code of "aa" into it.
	code, ok := c.lookupCode("aa")
	if !ok {
		t.Fatal("aa not in dictionary")
	}
	c.codes.chunks[3][2] = code

	if got := c.FilterSelection(func(s string) bool { return s == "aa" }).Count(); got != testChunkSize+1 {
		t.Fatalf("naive scan: got %d rows, want %d", got, testChunkSize+1)
	}
	if got := c.FilterSelectionEqual("aa").Count(); got != testChunkSize {
		t.Fatalf("Equal(aa): got %d rows, want %d — chunk 3 was scanned, not pruned", got, testChunkSize)
	}
	if got := c.FilterSelectionIn([]string{"aa"}).Count(); got != testChunkSize {
		t.Fatalf("In([aa]): got %d rows, want %d", got, testChunkSize)
	}
	lo, hi := "a", "b"
	if got := c.FilterSelectionRange(&lo, &hi).Count(); got != testChunkSize {
		t.Fatalf("Range(a, b): got %d rows, want %d", got, testChunkSize)
	}

	// A value absent from the dictionary needs no scan at all — the planted
	// inconsistency stays invisible simply because no code exists for it.
	if got := c.FilterSelectionEqual("zz").Count(); got != 0 {
		t.Fatalf("Equal(zz): got %d rows, want 0", got)
	}
}

// TestPrunedFilterDefaultChunkSize exercises pruning at the real chunk size
// across chunk boundaries: sorted unique values, so the column is a key and
// every equality hits exactly one chunk.
func TestPrunedFilterDefaultChunkSize(t *testing.T) {
	n := 2*DefaultChunkSize + DefaultChunkSize/2
	c := NewChunkedInt64Column(NewColumnDef("v", "V", ""))
	for i := 0; i < n; i++ {
		c.Append(int64(i))
	}
	c.FinalizeColumn()
	if !c.IsKey() {
		t.Fatal("sorted unique ints should be a key column")
	}
	for _, v := range []int64{0, 65535, 65536, 131071, 131072, int64(n - 1)} {
		got := c.FilterSelectionEqual(v).ToIndices()
		if !reflect.DeepEqual(got, []uint32{uint32(v)}) {
			t.Fatalf("Equal(%d): got %v, want [%d]", v, got, v)
		}
	}
	if got := c.FilterSelectionEqual(int64(n)).Count(); got != 0 {
		t.Fatalf("Equal(%d): got %d rows, want 0", n, got)
	}
	if min, max, ok := c.ChunkBounds(1); !ok || min != int64(DefaultChunkSize) || max != int64(2*DefaultChunkSize-1) {
		t.Fatalf("ChunkBounds(1) = (%d, %d, %v)", min, max, ok)
	}
}
