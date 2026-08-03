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
	"math/bits"
)

// DefaultChunkSize is the number of rows per chunk in chunked columns
// (docs/scaling-to-1b-rows.md §5): fixed row groups whose size is a power of
// two, so a global row index splits into (chunkID, offset) by shift and mask.
//
// Chunk boundaries must align across the columns of a table — zone-map
// pruning and per-chunk execution work on whole table chunks, not per-column
// ones. The public constructors all use this size, so alignment holds by
// construction.
const DefaultChunkSize = 1 << 16 // 65536 rows

// IChunkedColumn is the optional interface of columns whose storage is split
// into fixed-size row groups. Engines type-assert for it (the io.ReaderAt
// pattern, like IGroupOps): columns that implement it can be scanned, pruned
// and parallelised chunk by chunk; columns that don't are processed as a
// single range.
//
// Chunks are contiguous: chunk c covers global rows
// [c*ChunkSize(), c*ChunkSize()+ChunkLen(c)). Every chunk is full except
// possibly the last.
type IChunkedColumn interface {
	// ChunkSize returns the number of rows in a full chunk (a power of two).
	ChunkSize() int
	// NumChunks returns the number of chunks. Zero for an empty column.
	NumChunks() int
	// ChunkLen returns the number of rows in chunk c: ChunkSize() for every
	// chunk except possibly the last.
	ChunkLen(c int) int
}

// chunkedData is the storage shared by all chunked columns: values held in
// fixed-size heap-allocated chunks instead of one contiguous slice. Appending
// never moves previously written data (a full chunk is simply followed by a
// new one), so building an n-row column allocates exactly ceil(n/chunkSize)
// chunks with no doubling-growth copies, and memory waste is bounded by one
// chunk instead of by the growth factor.
type chunkedData[T any] struct {
	shift  uint32 // log2(chunk size)
	mask   uint32 // chunk size - 1
	chunks [][]T
	n      int
}

// newChunkedData returns empty storage with the given chunk size, which must
// be a power of two.
func newChunkedData[T any](chunkSize int) chunkedData[T] {
	if chunkSize <= 0 || chunkSize&(chunkSize-1) != 0 {
		panic(fmt.Sprintf("columns: chunk size %d is not a power of two", chunkSize))
	}
	return chunkedData[T]{
		shift: uint32(bits.TrailingZeros(uint(chunkSize))),
		mask:  uint32(chunkSize - 1),
	}
}

func (d *chunkedData[T]) chunkSize() int {
	return 1 << d.shift
}

func (d *chunkedData[T]) append(v T) {
	if len(d.chunks) == 0 || len(d.chunks[len(d.chunks)-1]) == d.chunkSize() {
		d.chunks = append(d.chunks, make([]T, 0, d.chunkSize()))
	}
	last := len(d.chunks) - 1
	d.chunks[last] = append(d.chunks[last], v)
	d.n++
}

// at returns the value at global row i. The caller has already bounds-checked
// i against len().
func (d *chunkedData[T]) at(i uint32) T {
	return d.chunks[i>>d.shift][i&d.mask]
}

func (d *chunkedData[T]) len() int {
	return d.n
}

func (d *chunkedData[T]) numChunks() int {
	return len(d.chunks)
}

func (d *chunkedData[T]) chunkLen(c int) int {
	return len(d.chunks[c])
}

// chunk returns chunk c's values. The slice aliases the column's storage and
// must not be modified: columns are immutable after finalize.
func (d *chunkedData[T]) chunk(c int) []T {
	return d.chunks[c]
}
