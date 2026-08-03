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
	"testing"
)

func TestDefaultChunkSize(t *testing.T) {
	if DefaultChunkSize&(DefaultChunkSize-1) != 0 {
		t.Fatalf("DefaultChunkSize %d is not a power of two", DefaultChunkSize)
	}
	if DefaultChunkSize < 1<<16 || DefaultChunkSize > 1<<18 {
		t.Fatalf("DefaultChunkSize %d outside the design's 64k-256k range", DefaultChunkSize)
	}
	if DefaultChunkSize%64 != 0 {
		t.Fatalf("DefaultChunkSize %d not word-aligned for Selection bitmaps", DefaultChunkSize)
	}
}

func TestChunkedDataRejectsNonPowerOfTwo(t *testing.T) {
	for _, size := range []int{0, -1, 3, 100, 65535} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("newChunkedData(%d) did not panic", size)
				}
			}()
			newChunkedData[int](size)
		}()
	}
}

func TestChunkedDataGeometry(t *testing.T) {
	cases := []struct {
		n, chunkSize int
		wantChunks   int
		wantLens     []int
	}{
		{0, 8, 0, nil},
		{1, 8, 1, []int{1}},
		{7, 8, 1, []int{7}},
		{8, 8, 1, []int{8}},
		{9, 8, 2, []int{8, 1}},
		{16, 8, 2, []int{8, 8}},
		{21, 8, 3, []int{8, 8, 5}},
		{4, 4, 1, []int{4}},
		{5, 4, 2, []int{4, 1}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("n=%d,chunk=%d", tc.n, tc.chunkSize), func(t *testing.T) {
			d := newChunkedData[uint32](tc.chunkSize)
			for i := 0; i < tc.n; i++ {
				d.append(uint32(i * 10))
			}
			if d.len() != tc.n {
				t.Fatalf("len() = %d, want %d", d.len(), tc.n)
			}
			if d.numChunks() != tc.wantChunks {
				t.Fatalf("numChunks() = %d, want %d", d.numChunks(), tc.wantChunks)
			}
			for c, want := range tc.wantLens {
				if got := d.chunkLen(c); got != want {
					t.Errorf("chunkLen(%d) = %d, want %d", c, got, want)
				}
			}
			// Every row is retrievable and correctly addressed across chunk
			// boundaries.
			for i := 0; i < tc.n; i++ {
				if got := d.at(uint32(i)); got != uint32(i*10) {
					t.Fatalf("at(%d) = %d, want %d", i, got, i*10)
				}
			}
			// Chunk contents match global addressing.
			row := 0
			for c := 0; c < d.numChunks(); c++ {
				for _, v := range d.chunk(c) {
					if v != uint32(row*10) {
						t.Fatalf("chunk(%d) row %d = %d, want %d", c, row, v, row*10)
					}
					row++
				}
			}
			if row != tc.n {
				t.Fatalf("chunks cover %d rows, want %d", row, tc.n)
			}
		})
	}
}

// TestChunkedDataAppendDoesNotMoveData pins the growth guarantee chunking
// exists for: appending never re-copies previously written chunks, unlike a
// flat slice whose doubling growth copies everything.
func TestChunkedDataAppendDoesNotMoveData(t *testing.T) {
	d := newChunkedData[int64](8)
	for i := 0; i < 9; i++ {
		d.append(int64(i))
	}
	firstChunk := d.chunk(0)
	ptr := &firstChunk[0]
	for i := 9; i < 1000; i++ {
		d.append(int64(i))
	}
	if &d.chunk(0)[0] != ptr {
		t.Fatal("chunk 0 backing array moved after later appends")
	}
	for i := 0; i < 1000; i++ {
		if got := d.at(uint32(i)); got != int64(i) {
			t.Fatalf("at(%d) = %d after growth, want %d", i, got, i)
		}
	}
}

// TestChunkedColumnDefaultChunkBoundaries exercises real DefaultChunkSize
// boundaries: the rows around 65536 and 131072 land in the right chunks.
func TestChunkedColumnDefaultChunkBoundaries(t *testing.T) {
	c := NewChunkedUint32Column(NewColumnDef("v", "V", ""))
	n := 2*DefaultChunkSize + 3
	for i := 0; i < n; i++ {
		c.Append(uint32(i))
	}
	if c.NumChunks() != 3 {
		t.Fatalf("NumChunks() = %d, want 3", c.NumChunks())
	}
	if c.ChunkSize() != DefaultChunkSize {
		t.Fatalf("ChunkSize() = %d, want %d", c.ChunkSize(), DefaultChunkSize)
	}
	if got := c.ChunkLen(2); got != 3 {
		t.Fatalf("ChunkLen(2) = %d, want 3", got)
	}
	for _, i := range []int{0, DefaultChunkSize - 1, DefaultChunkSize, DefaultChunkSize + 1, 2*DefaultChunkSize - 1, 2 * DefaultChunkSize, n - 1} {
		v, err := c.GetValue(uint32(i))
		if err != nil || v != uint32(i) {
			t.Fatalf("GetValue(%d) = %d, %v; want %d, nil", i, v, err, i)
		}
	}
	if _, err := c.GetValue(uint32(n)); err == nil {
		t.Fatal("GetValue(n) succeeded, want out-of-bounds error")
	}
}
