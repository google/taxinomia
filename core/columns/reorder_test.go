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
	"time"
)

// reversePerm returns the permutation n-1, n-2, ..., 0.
func reversePerm(n int) []uint32 {
	perm := make([]uint32, n)
	for i := range perm {
		perm[i] = uint32(n - 1 - i)
	}
	return perm
}

// shufflePerm returns a fixed pseudo-random permutation of [0, n) so the
// tests are deterministic.
func shufflePerm(n int) []uint32 {
	perm := make([]uint32, n)
	for i := range perm {
		perm[i] = uint32(i)
	}
	// Fisher-Yates with a fixed LCG.
	state := uint64(12345)
	for i := n - 1; i > 0; i-- {
		state = state*6364136223846793005 + 1442695040888963407
		j := int(state % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}

// checkReorderParity reorders col and asserts the copy holds the source's
// values in permutation order, with length, def and chunk layout preserved.
func checkReorderParity(t *testing.T, col IDataColumn, perm []uint32) IDataColumn {
	t.Helper()
	r, ok := col.(Reorderable)
	if !ok {
		t.Fatalf("%T does not implement Reorderable", col)
	}
	out := r.Reorder(perm)
	if out.Length() != col.Length() {
		t.Fatalf("length changed: %d -> %d", col.Length(), out.Length())
	}
	if out.ColumnDef() != col.ColumnDef() {
		t.Errorf("ColumnDef not preserved")
	}
	if src, ok := col.(IChunkedColumn); ok {
		if dst, ok := out.(IChunkedColumn); !ok || dst.ChunkSize() != src.ChunkSize() {
			t.Errorf("chunk size not preserved")
		}
	}
	for i := range perm {
		want, err1 := col.GetString(perm[i])
		got, err2 := out.GetString(uint32(i))
		if err1 != nil || err2 != nil {
			t.Fatalf("GetString error at %d: %v / %v", i, err1, err2)
		}
		if got != want {
			t.Fatalf("row %d: got %q, want %q (source row %d)", i, got, want, perm[i])
		}
	}
	return out
}

func TestReorderAllChunkedTypes(t *testing.T) {
	const n = 100 // chunk size 8 -> 13 chunks, last one partial
	def := NewColumnDef("col", "Col", "")

	strCol := newChunkedStringColumn(def, 8)
	boolCol := newChunkedBoolColumn(def, 8)
	intCol := newChunkedInt64Column(def, 8)
	u64Col := newChunkedUint64Column(def, 8)
	u32Col := newChunkedUint32Column(def, 8)
	fCol := newChunkedFloat64Column(def, 8)
	dtCol := newChunkedDatetimeColumn(def, 8)
	dictCol := newChunkedDictStringColumn[uint8](def, 8)
	for i := 0; i < n; i++ {
		strCol.Append(string(rune('a'+i%26)) + string(rune('0'+i%10)))
		boolCol.Append(i%3 == 0)
		intCol.Append(int64(i%17 - 8))
		u64Col.Append(uint64(i % 13))
		u32Col.Append(uint32(i % 11))
		switch i % 7 {
		case 0:
			fCol.Append(math.NaN())
		case 1:
			fCol.Append(math.Copysign(0, -1))
		default:
			fCol.Append(float64(i%19) - 9.5)
		}
		dtCol.AppendUnix(int64(i%23) * 3600)
		dictCol.Append([]string{"red", "green", "blue"}[i%3])
	}

	cols := map[string]IDataColumn{
		"string": strCol, "bool": boolCol, "int64": intCol,
		"uint64": u64Col, "uint32": u32Col, "float64": fCol,
		"datetime": dtCol, "dict": dictCol,
	}
	for name, col := range cols {
		t.Run(name, func(t *testing.T) {
			checkReorderParity(t, col, shufflePerm(n))
			checkReorderParity(t, col, reversePerm(n))
		})
	}
}

// A reordered copy is finalized: zone maps exist and uniqueness is detected,
// even when the source column was never finalized (the csv loaders don't
// finalize).
func TestReorderFinalizesCopy(t *testing.T) {
	def := NewColumnDef("id", "ID", "test.id")
	col := newChunkedInt64Column(def, 8)
	for i := 0; i < 40; i++ {
		col.Append(int64(40 - i)) // unique, reverse-sorted
	}
	if col.IsKey() {
		t.Fatal("source unexpectedly finalized")
	}
	out := checkReorderParity(t, col, reversePerm(40)).(*ChunkedInt64Column)
	if !out.IsKey() {
		t.Error("reordered copy of a unique column should be a key")
	}
	if _, err := out.GetIndex(17); err != nil {
		t.Errorf("reverse lookup on reordered key column: %v", err)
	}
	if _, _, ok := out.ChunkBounds(0); !ok {
		t.Error("reordered copy has no zone maps")
	}
	// The copy is now ascending, so chunk bounds must be non-overlapping.
	for ci := 1; ci < out.NumChunks(); ci++ {
		_, prevMax, ok1 := out.ChunkBounds(ci - 1)
		curMin, _, ok2 := out.ChunkBounds(ci)
		if !ok1 || !ok2 || prevMax > curMin {
			t.Errorf("chunk bounds overlap at %d: prevMax=%d curMin=%d", ci, prevMax, curMin)
		}
	}
}

func TestReorderDatetimePreservesDisplay(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	def := NewColumnDef("ts", "TS", "")
	col := NewChunkedDatetimeColumnWithFormat(def, "2006-01-02 15:04 MST", loc)
	for i := 0; i < 10; i++ {
		col.AppendUnix(int64(i) * 86400)
	}
	out := checkReorderParity(t, col, reversePerm(10)).(*ChunkedDatetimeColumn)
	s, _ := out.GetString(0)
	want, _ := col.GetString(9)
	if s != want {
		t.Errorf("display format/location not preserved: got %q, want %q", s, want)
	}
}

func TestReorderDictPreservesDictionary(t *testing.T) {
	def := NewColumnDef("color", "Color", "")
	col := newChunkedDictStringColumn[uint8](def, 8)
	values := []string{"red", "green", "blue", "green", "red"}
	for _, v := range values {
		col.Append(v)
	}
	col.FinalizeColumn() // non-key: releases the interning map
	out := checkReorderParity(t, col, reversePerm(5)).(*ChunkedDictStringColumn[uint8])
	if out.Cardinality() != col.Cardinality() {
		t.Errorf("cardinality changed: %d -> %d", col.Cardinality(), out.Cardinality())
	}
}

func TestReorderDictKeyColumnKeepsLookup(t *testing.T) {
	def := NewColumnDef("name", "Name", "test.name")
	col := newChunkedDictStringColumn[uint8](def, 8)
	for _, v := range []string{"delta", "alpha", "charlie", "bravo"} {
		col.Append(v)
	}
	col.FinalizeColumn()
	if !col.IsKey() {
		t.Fatal("source should be a key")
	}
	out := checkReorderParity(t, col, []uint32{1, 3, 2, 0}).(*ChunkedDictStringColumn[uint8])
	if !out.IsKey() {
		t.Fatal("reordered copy should still be a key")
	}
	idx, err := out.GetIndex("delta")
	if err != nil || idx != 3 {
		t.Errorf("GetIndex(delta) = %d, %v; want 3, nil", idx, err)
	}
}

func TestReorderLengthMismatchPanics(t *testing.T) {
	def := NewColumnDef("x", "X", "")
	col := newChunkedInt64Column(def, 8)
	col.Append(1)
	col.Append(2)
	defer func() {
		if recover() == nil {
			t.Error("expected panic on permutation length mismatch")
		}
	}()
	col.Reorder([]uint32{0})
}
