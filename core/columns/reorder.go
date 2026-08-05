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

import "fmt"

// Reorderable is the optional interface for storage columns that can produce
// a copy of themselves with rows permuted. It is what table-level sorting
// (docs/scaling-to-1b-rows.md §5: sorted storage) is built from: the table
// computes one permutation from its sort key and asks every column for a
// reordered copy, so all columns stay row-aligned.
//
// Like IGroupOps and RowComparator, this is an optional interface rather than
// a widening of IDataColumn: external column implementations without it simply
// leave their tables unsorted (sorting is an optimization, never a
// correctness requirement).
type Reorderable interface {
	// Reorder returns a new column of the same concrete type and column
	// definition whose row i holds the receiver's value at perm[i]. perm must
	// be a permutation of [0, Length()); Reorder panics if the lengths differ
	// and silently produces a corrupt column if perm repeats indices.
	//
	// The copy is finalized (loaded columns are immutable once in sorted
	// storage — the append-only contract), so its zone maps describe the
	// reordered data. The receiver is not modified.
	Reorder(perm []uint32) IDataColumn
}

// checkPermLength is the shared length validation for Reorder implementations.
func checkPermLength(name string, permLen, colLen int) {
	if permLen != colLen {
		panic(fmt.Sprintf("columns: Reorder on %q: permutation length %d != column length %d", name, permLen, colLen))
	}
}

// reorderData appends src's values to dst in permutation order. dst and src
// may not alias.
func reorderData[T any](dst *chunkedData[T], src *chunkedData[T], perm []uint32) {
	for _, p := range perm {
		dst.append(src.at(p))
	}
}

// reorder is the shared Reorder body for the types embedding chunkedColumn:
// copy the values in permutation order into the freshly constructed dst
// (same columnDef, same chunk size) and finalize it.
func (c *chunkedColumn[T, K]) reorderInto(dst *chunkedColumn[T, K], perm []uint32) {
	checkPermLength(c.columnDef.Name(), len(perm), c.data.len())
	reorderData(&dst.data, &c.data, perm)
	dst.FinalizeColumn()
}

func (c *ChunkedStringColumn) Reorder(perm []uint32) IDataColumn {
	n := newChunkedStringColumn(c.columnDef, c.data.chunkSize())
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

func (c *ChunkedBoolColumn) Reorder(perm []uint32) IDataColumn {
	n := newChunkedBoolColumn(c.columnDef, c.data.chunkSize())
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

func (c *ChunkedInt64Column) Reorder(perm []uint32) IDataColumn {
	n := newChunkedInt64Column(c.columnDef, c.data.chunkSize())
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

func (c *ChunkedUint64Column) Reorder(perm []uint32) IDataColumn {
	n := newChunkedUint64Column(c.columnDef, c.data.chunkSize())
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

func (c *ChunkedUint32Column) Reorder(perm []uint32) IDataColumn {
	n := newChunkedUint32Column(c.columnDef, c.data.chunkSize())
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

func (c *ChunkedFloat64Column) Reorder(perm []uint32) IDataColumn {
	n := newChunkedFloat64Column(c.columnDef, c.data.chunkSize())
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

// Reorder on the datetime column preserves the display format and location;
// stored values are already UTC-normalized, so they are copied as-is.
func (c *ChunkedDatetimeColumn) Reorder(perm []uint32) IDataColumn {
	n := newChunkedDatetimeColumn(c.columnDef, c.data.chunkSize())
	n.displayFormat = c.displayFormat
	n.location = c.location
	c.reorderInto(&n.chunkedColumn, perm)
	return n
}

// Reorder on the dict column remaps codes directly instead of re-interning
// strings: new codes are assigned in first-encounter order of the permuted
// sequence, so the copy is exactly what appending the permuted values would
// build, without n map lookups.
func (c *ChunkedDictStringColumn[K]) Reorder(perm []uint32) IDataColumn {
	checkPermLength(c.columnDef.Name(), len(perm), c.codes.len())
	n := newChunkedDictStringColumn[K](c.columnDef, c.codes.chunkSize())
	remap := make([]K, len(c.dict))
	seen := make([]bool, len(c.dict))
	for _, p := range perm {
		old := c.codes.at(p)
		if !seen[old] {
			seen[old] = true
			code := K(len(n.dict))
			remap[old] = code
			n.dict = append(n.dict, c.dict[old])
			n.index[c.dict[old]] = code
		}
		n.codes.append(remap[old])
	}
	n.FinalizeColumn()
	return n
}

// Compile-time checks: every chunked column type is Reorderable, so any table
// a loader builds can be sorted.
var (
	_ Reorderable = (*ChunkedStringColumn)(nil)
	_ Reorderable = (*ChunkedBoolColumn)(nil)
	_ Reorderable = (*ChunkedInt64Column)(nil)
	_ Reorderable = (*ChunkedUint64Column)(nil)
	_ Reorderable = (*ChunkedUint32Column)(nil)
	_ Reorderable = (*ChunkedFloat64Column)(nil)
	_ Reorderable = (*ChunkedDatetimeColumn)(nil)
	_ Reorderable = (*ChunkedDictStringColumn[uint8])(nil)
	_ Reorderable = (*ChunkedDictStringColumn[uint16])(nil)
	_ Reorderable = (*ChunkedDictStringColumn[uint32])(nil)
)
