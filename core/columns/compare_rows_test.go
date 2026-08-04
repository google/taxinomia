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
	"testing"
)

// sign normalises a comparison result to -1/0/1.
func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}

// checkCompareParity asserts CompareAtIndex agrees between a plain and a
// chunked column holding the same values, for every ordered pair.
func checkCompareParity(t *testing.T, name string, plain, chunked IDataColumn) {
	t.Helper()
	n := plain.Length()
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			p := sign(CompareAtIndex(plain, uint32(i), uint32(j)))
			c := sign(CompareAtIndex(chunked, uint32(i), uint32(j)))
			if p != c {
				t.Fatalf("%s: CompareAtIndex(%d, %d): plain %d, chunked %d", name, i, j, p, c)
			}
		}
	}
}

// TestChunkedCompareAtIndexParity pins that sorting compares chunked columns
// by value, exactly as the plain concrete cases do — not via the
// formatted-string fallback, which would order 10 before 9.
func TestChunkedCompareAtIndexParity(t *testing.T) {
	t.Run("int64 value order", func(t *testing.T) {
		plain := NewInt64Column(NewColumnDef("i", "I", ""))
		chunked := newChunkedInt64Column(NewColumnDef("i", "I", ""), testChunkSize)
		for _, v := range []int64{9, 10, -3, 100, 2, -20, 0, 9, 55, 7, 1000, -1} {
			plain.Append(v)
			chunked.Append(v)
		}
		checkCompareParity(t, "int64", plain, chunked)
		// The white-box case the string fallback gets wrong: 9 < 10.
		if CompareAtIndex(chunked, 0, 1) >= 0 {
			t.Fatal("chunked int64 column compared 9 >= 10")
		}
	})

	t.Run("uint64 and uint32", func(t *testing.T) {
		p64 := NewUint64Column(NewColumnDef("u", "U", ""))
		c64 := newChunkedUint64Column(NewColumnDef("u", "U", ""), testChunkSize)
		p32 := NewUint32Column(NewColumnDef("v", "V", ""))
		c32 := newChunkedUint32Column(NewColumnDef("v", "V", ""), testChunkSize)
		for _, v := range []uint64{9, 10, 0, math.MaxUint64, 5, 9, 300, 77, 2} {
			p64.Append(v)
			c64.Append(v)
			p32.Append(uint32(v))
			c32.Append(uint32(v))
		}
		checkCompareParity(t, "uint64", p64, c64)
		checkCompareParity(t, "uint32", p32, c32)
	})

	t.Run("float64 with NaN last", func(t *testing.T) {
		plain := NewFloat64Column(NewColumnDef("f", "F", ""))
		chunked := newChunkedFloat64Column(NewColumnDef("f", "F", ""), testChunkSize)
		for _, v := range []float64{1.5, math.NaN(), -2.25, math.Inf(1), math.Inf(-1), 0.0, math.Copysign(0, -1), math.NaN(), 9, 10} {
			plain.Append(v)
			chunked.Append(v)
		}
		checkCompareParity(t, "float64", plain, chunked)
		// NaN sorts after +Inf on both.
		if CompareAtIndex(chunked, 1, 3) <= 0 {
			t.Fatal("chunked float64 column did not sort NaN after +Inf")
		}
	})

	t.Run("string and bool", func(t *testing.T) {
		ps := NewStringColumn(NewColumnDef("s", "S", ""))
		cs := newChunkedStringColumn(NewColumnDef("s", "S", ""), testChunkSize)
		for _, v := range []string{"pear", "apple", "", "Pear", "apple", "zebra", "10", "9"} {
			ps.Append(v)
			cs.Append(v)
		}
		checkCompareParity(t, "string", ps, cs)

		pb := NewBoolColumn(NewColumnDef("b", "B", ""))
		cb := newChunkedBoolColumn(NewColumnDef("b", "B", ""), testChunkSize)
		for _, v := range []bool{true, false, true, true, false} {
			pb.Append(v)
			cb.Append(v)
		}
		checkCompareParity(t, "bool", pb, cb)
	})

	t.Run("datetime", func(t *testing.T) {
		plain := NewDatetimeColumn(NewColumnDef("d", "D", ""))
		chunked := newChunkedDatetimeColumn(NewColumnDef("d", "D", ""), testChunkSize)
		for _, v := range testTimes() {
			plain.Append(v)
			chunked.Append(v)
		}
		checkCompareParity(t, "datetime", plain, chunked)
	})

	t.Run("dict matches value order", func(t *testing.T) {
		// The plain dict column has no CompareAtIndex case and compares via
		// GetString; the chunked dict's CompareRows must give the same order.
		plain := NewStringColumn(NewColumnDef("s", "S", ""))
		chunked := newChunkedDictStringColumn[uint16](NewColumnDef("s", "S", ""), testChunkSize)
		for i := 0; i < 21; i++ {
			v := []string{"north", "south", "east", "west", "north"}[i%5]
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkCompareParity(t, "dict", plain, chunked)
	})
}
