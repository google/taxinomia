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
	"reflect"
	"sync"
	"testing"
)

func TestChunkedDictParityWithPlainDict(t *testing.T) {
	plain := NewDictStringColumn[uint8](NewColumnDef("d", "D", ""))
	chunked := newChunkedDictStringColumn[uint8](NewColumnDef("d", "D", ""), testChunkSize)
	for i := 0; i < 21; i++ {
		v := fmt.Sprintf("v%d", i%5)
		plain.Append(v)
		chunked.Append(v)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()

	if plain.Cardinality() != chunked.Cardinality() {
		t.Fatalf("Cardinality: plain %d, chunked %d", plain.Cardinality(), chunked.Cardinality())
	}
	for code := 0; code < plain.Cardinality(); code++ {
		if plain.DictValue(uint32(code)) != chunked.DictValue(uint32(code)) {
			t.Fatalf("DictValue(%d): plain %q, chunked %q", code, plain.DictValue(uint32(code)), chunked.DictValue(uint32(code)))
		}
	}
	for i := uint32(0); i < 21; i++ {
		if plain.GetCode(i) != chunked.GetCode(i) {
			t.Fatalf("GetCode(%d): plain %d, chunked %d", i, plain.GetCode(i), chunked.GetCode(i))
		}
	}
	if !reflect.DeepEqual(plain.Ranks(), chunked.Ranks()) {
		t.Fatalf("Ranks: plain %v, chunked %v", plain.Ranks(), chunked.Ranks())
	}
	checkColumnParity(t, plain, chunked)
	pred := func(s string) bool { return s == "v2" || s == "v4" }
	checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
}

// TestChunkedDictSubsetFallback pins the dense/hash cutover: a small
// selection over a large dictionary takes the hash path, and its codes still
// agree across the three operations.
func TestChunkedDictSubsetFallback(t *testing.T) {
	plain := NewDictStringColumn[uint16](NewColumnDef("d", "D", ""))
	chunked := newChunkedDictStringColumn[uint16](NewColumnDef("d", "D", ""), testChunkSize)
	for i := 0; i < 400; i++ {
		v := fmt.Sprintf("v%03d", i%200) // 200 distinct
		plain.Append(v)
		chunked.Append(v)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()

	small := RowIndices{5, 6, 7, 8, 9} // 5 rows < 200/8 = 25: hash fallback
	if !chunked.smallGroupSubset(small) {
		t.Fatal("expected the small selection to take the hash fallback")
	}
	pCounts, pFirsts := plain.GroupCounts(small)
	cCounts, cFirsts := chunked.GroupCounts(small)
	if !reflect.DeepEqual(pCounts, cCounts) || !reflect.DeepEqual(pFirsts, cFirsts) {
		t.Fatalf("fallback GroupCounts: plain (%v, %v), chunked (%v, %v)", pCounts, pFirsts, cCounts, cFirsts)
	}
	for code := range pCounts {
		p := plain.GroupMembers(small, uint32(code), 0, -1)
		c := chunked.GroupMembers(small, uint32(code), 0, -1)
		if !reflect.DeepEqual(p, c) {
			t.Fatalf("fallback GroupMembers(%d): plain %v, chunked %v", code, p, c)
		}
	}

	// The dense path on the same column agrees with the plain dense path.
	pCounts, pFirsts = plain.GroupCounts(AllRows(400))
	cCounts, cFirsts = chunked.GroupCounts(AllRows(400))
	if !reflect.DeepEqual(pCounts, cCounts) || !reflect.DeepEqual(pFirsts, cFirsts) {
		t.Fatal("dense GroupCounts disagree")
	}
}

func TestChunkedDictKeyColumn(t *testing.T) {
	c := newChunkedDictStringColumn[uint16](NewColumnDef("k", "K", "thing"), testChunkSize)
	for i := 0; i < 30; i++ {
		c.Append(fmt.Sprintf("key%02d", i))
	}
	c.FinalizeColumn()
	if !c.IsKey() {
		t.Fatal("all-distinct dict column not detected as key")
	}
	idx, err := c.GetIndex("key17")
	if err != nil || idx != 17 {
		t.Fatalf("GetIndex(key17) = %d, %v; want 17, nil", idx, err)
	}
}

func TestChunkedDictAppendOverflowPanics(t *testing.T) {
	c := newChunkedDictStringColumn[uint8](NewColumnDef("d", "D", ""), testChunkSize)
	defer func() {
		if recover() == nil {
			t.Fatal("appending past the uint8 code width did not panic")
		}
	}()
	for i := 0; i < 300; i++ {
		c.Append(fmt.Sprintf("v%03d", i))
	}
}

func TestChunkedDictRanksConcurrent(t *testing.T) {
	c := newChunkedDictStringColumn[uint8](NewColumnDef("d", "D", ""), testChunkSize)
	for i := 0; i < 100; i++ {
		c.Append(fmt.Sprintf("v%02d", i%10))
	}
	c.FinalizeColumn()
	var wg sync.WaitGroup
	results := make([][]uint8, 8)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			results[g] = c.Ranks()
		}(g)
	}
	wg.Wait()
	for g := 1; g < 8; g++ {
		if !reflect.DeepEqual(results[0], results[g]) {
			t.Fatalf("goroutine %d saw different ranks", g)
		}
	}
}

func TestCompactChunkedStringColumn(t *testing.T) {
	def := func() *ColumnDef { return NewColumnDef("s", "S", "") }

	t.Run("below row threshold declines", func(t *testing.T) {
		c := newChunkedStringColumn(def(), 64)
		for i := 0; i < minDictRows-1; i++ {
			c.Append(fmt.Sprintf("v%d", i%10))
		}
		c.FinalizeColumn()
		got, compacted := CompactChunkedStringColumn(c)
		if compacted || got != IDataColumn(c) {
			t.Fatal("column below minDictRows was compacted")
		}
	})

	t.Run("key column declines", func(t *testing.T) {
		c := newChunkedStringColumn(NewColumnDef("k", "K", "thing"), 64)
		for i := 0; i < minDictRows; i++ {
			c.Append(fmt.Sprintf("key%05d", i))
		}
		c.FinalizeColumn()
		if !c.IsKey() {
			t.Fatal("setup: column not a key")
		}
		got, compacted := CompactChunkedStringColumn(c)
		if compacted || got != IDataColumn(c) {
			t.Fatal("key column was compacted")
		}
	})

	t.Run("low cardinality compacts to uint8", func(t *testing.T) {
		c := newChunkedStringColumn(def(), 64)
		for i := 0; i < 5000; i++ {
			c.Append(fmt.Sprintf("v%02d", i%100))
		}
		c.FinalizeColumn()
		got, compacted := CompactChunkedStringColumn(c)
		if !compacted {
			t.Fatal("low-cardinality column not compacted")
		}
		dc, ok := got.(*ChunkedDictStringColumn[uint8])
		if !ok {
			t.Fatalf("compacted to %T, want *ChunkedDictStringColumn[uint8]", got)
		}
		if dc.ChunkSize() != 64 {
			t.Fatalf("chunk size not preserved: got %d, want 64", dc.ChunkSize())
		}
		if dc.Length() != 5000 || dc.Cardinality() != 100 {
			t.Fatalf("Length %d, Cardinality %d; want 5000, 100", dc.Length(), dc.Cardinality())
		}
		for i := 0; i < 5000; i += 501 {
			want, _ := c.GetString(uint32(i))
			gotS, err := dc.GetString(uint32(i))
			if err != nil || gotS != want {
				t.Fatalf("GetString(%d) = %q, %v; want %q", i, gotS, err, want)
			}
		}
	})

	t.Run("medium cardinality compacts to uint16", func(t *testing.T) {
		c := newChunkedStringColumn(def(), 64)
		for i := 0; i < 5000; i++ {
			c.Append(fmt.Sprintf("v%03d", i%300))
		}
		c.FinalizeColumn()
		got, compacted := CompactChunkedStringColumn(c)
		if !compacted {
			t.Fatal("medium-cardinality column not compacted")
		}
		if _, ok := got.(*ChunkedDictStringColumn[uint16]); !ok {
			t.Fatalf("compacted to %T, want *ChunkedDictStringColumn[uint16]", got)
		}
	})

	t.Run("cardinality cap declines", func(t *testing.T) {
		c := newChunkedStringColumn(def(), 1<<12)
		// One past the cap, each value twice so the column is not a key.
		for i := 0; i <= maxDictCardinality; i++ {
			v := fmt.Sprintf("v%06d", i)
			c.Append(v)
			c.Append(v)
		}
		c.FinalizeColumn()
		got, compacted := CompactChunkedStringColumn(c)
		if compacted || got != IDataColumn(c) {
			t.Fatal("column above maxDictCardinality was compacted")
		}
	})
}
