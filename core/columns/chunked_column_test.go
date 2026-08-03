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
	"sort"
	"testing"
)

// The parity suite: a chunked column built from the same values as its plain
// counterpart must agree on every operation. Chunk size 8 keeps multiple
// chunk boundaries inside small test data.
const testChunkSize = 8

type aggPairRecorder struct {
	pairs [][2]uint32
}

func (r *aggPairRecorder) Add(code, row uint32) {
	r.pairs = append(r.pairs, [2]uint32{code, row})
}

// paritySelections returns the row sets the grouping operations are checked
// under: the full universe, a scattered explicit list crossing chunk
// boundaries, and a bitmap.
func paritySelections(n int) map[string]RowSet {
	sels := map[string]RowSet{"all": AllRows(n)}
	var scattered RowIndices
	for _, i := range []int{1, 6, 7, 8, 9, 14, 15, 16, 20} {
		if i < n {
			scattered = append(scattered, uint32(i))
		}
	}
	if len(scattered) > 0 {
		sels["scattered"] = scattered
	}
	if n > 0 {
		bitmap := NewSelection(n)
		for i := 0; i < n; i += 3 {
			bitmap.Add(uint32(i))
		}
		sels["bitmap"] = bitmap
	}
	return sels
}

// sortedPartition normalises a GroupIndices result for comparison across
// implementations whose group keys differ (e.g. Uint32Column keys by value,
// chunked columns by first appearance): the set of membership lists, ordered
// by first member.
func sortedPartition(grouped map[uint32][]uint32) [][]uint32 {
	parts := make([][]uint32, 0, len(grouped))
	for _, members := range grouped {
		parts = append(parts, members)
	}
	sort.Slice(parts, func(a, b int) bool { return parts[a][0] < parts[b][0] })
	return parts
}

func checkColumnParity(t *testing.T, plain, chunked IDataColumn) {
	t.Helper()
	if plain.Length() != chunked.Length() {
		t.Fatalf("Length: plain %d, chunked %d", plain.Length(), chunked.Length())
	}
	n := plain.Length()
	if plain.IsKey() != chunked.IsKey() {
		t.Fatalf("IsKey: plain %v, chunked %v", plain.IsKey(), chunked.IsKey())
	}

	for i := 0; i < n; i++ {
		sp, ep := plain.GetString(uint32(i))
		sc, ec := chunked.GetString(uint32(i))
		if (ep == nil) != (ec == nil) || sp != sc {
			t.Fatalf("GetString(%d): plain (%q, %v), chunked (%q, %v)", i, sp, ep, sc, ec)
		}
	}
	if _, err := chunked.GetString(uint32(n)); err == nil {
		t.Fatalf("GetString(%d) out of bounds succeeded", n)
	}

	plainOps := plain.(IGroupOps)
	chunkedOps := chunked.(IGroupOps)
	for name, sel := range paritySelections(n) {
		pCounts, pFirsts := plainOps.GroupCounts(sel)
		cCounts, cFirsts := chunkedOps.GroupCounts(sel)
		if !reflect.DeepEqual(pCounts, cCounts) || !reflect.DeepEqual(pFirsts, cFirsts) {
			t.Fatalf("GroupCounts(%s): plain (%v, %v), chunked (%v, %v)", name, pCounts, pFirsts, cCounts, cFirsts)
		}

		pAgg, cAgg := &aggPairRecorder{}, &aggPairRecorder{}
		plainOps.GroupAggregates(sel, pAgg)
		chunkedOps.GroupAggregates(sel, cAgg)
		if !reflect.DeepEqual(pAgg.pairs, cAgg.pairs) {
			t.Fatalf("GroupAggregates(%s): plain %v, chunked %v", name, pAgg.pairs, cAgg.pairs)
		}

		for code := 0; code < len(pCounts); code++ {
			full := plainOps.GroupMembers(sel, uint32(code), 0, -1)
			if got := chunkedOps.GroupMembers(sel, uint32(code), 0, -1); !reflect.DeepEqual(full, got) {
				t.Fatalf("GroupMembers(%s, %d): plain %v, chunked %v", name, code, full, got)
			}
			page := plainOps.GroupMembers(sel, uint32(code), 1, 2)
			if got := chunkedOps.GroupMembers(sel, uint32(code), 1, 2); !reflect.DeepEqual(page, got) {
				t.Fatalf("GroupMembers(%s, %d, 1, 2): plain %v, chunked %v", name, code, page, got)
			}
		}
	}

	// The deprecated surface still has to agree on the partition, though not
	// on the (implementation-specific) group keys.
	indices := make([]uint32, n)
	for i := range indices {
		indices[i] = uint32(i)
	}
	pGrouped, pUnmapped := plain.GroupIndices(indices, nil)
	cGrouped, cUnmapped := chunked.GroupIndices(indices, nil)
	if !reflect.DeepEqual(sortedPartition(pGrouped), sortedPartition(cGrouped)) {
		t.Fatalf("GroupIndices partitions differ: plain %v, chunked %v", pGrouped, cGrouped)
	}
	if len(pUnmapped) != len(cUnmapped) {
		t.Fatalf("GroupIndices unmapped: plain %v, chunked %v", pUnmapped, cUnmapped)
	}
}

func checkSelectionsEqual(t *testing.T, name string, plain, chunked *Selection) {
	t.Helper()
	if plain.Len() != chunked.Len() || !reflect.DeepEqual(plain.ToIndices(), chunked.ToIndices()) {
		t.Fatalf("%s: plain selects %v, chunked %v", name, plain.ToIndices(), chunked.ToIndices())
	}
}

func TestChunkedStringColumnParity(t *testing.T) {
	t.Run("repeats", func(t *testing.T) {
		plain := NewStringColumn(NewColumnDef("s", "S", ""))
		chunked := newChunkedStringColumn(NewColumnDef("s", "S", ""), testChunkSize)
		for i := 0; i < 21; i++ {
			v := fmt.Sprintf("v%d", i%5)
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkColumnParity(t, plain, chunked)
		pred := func(s string) bool { return s == "v1" || s == "v3" }
		checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
	})

	t.Run("key", func(t *testing.T) {
		plain := NewStringColumn(NewColumnDef("k", "K", "thing"))
		chunked := newChunkedStringColumn(NewColumnDef("k", "K", "thing"), testChunkSize)
		for i := 0; i < 21; i++ {
			v := fmt.Sprintf("key%02d", i)
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		if !chunked.IsKey() {
			t.Fatal("distinct entity column not detected as key")
		}
		checkColumnParity(t, plain, chunked)
		for i := 0; i < 21; i++ {
			v := fmt.Sprintf("key%02d", i)
			pi, pe := plain.GetIndex(v)
			ci, ce := chunked.GetIndex(v)
			if pe != nil || ce != nil || pi != ci {
				t.Fatalf("GetIndex(%q): plain (%d, %v), chunked (%d, %v)", v, pi, pe, ci, ce)
			}
		}
		if _, err := chunked.GetIndex("missing"); err == nil {
			t.Fatal("GetIndex of missing value succeeded")
		}
	})

	t.Run("empty", func(t *testing.T) {
		plain := NewStringColumn(NewColumnDef("s", "S", ""))
		chunked := newChunkedStringColumn(NewColumnDef("s", "S", ""), testChunkSize)
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		if chunked.Length() != 0 || chunked.NumChunks() != 0 {
			t.Fatalf("empty column: Length %d, NumChunks %d", chunked.Length(), chunked.NumChunks())
		}
		checkColumnParity(t, plain, chunked)
	})
}

func TestChunkedBoolColumnParity(t *testing.T) {
	plain := NewBoolColumn(NewColumnDef("b", "B", ""))
	chunked := newChunkedBoolColumn(NewColumnDef("b", "B", ""), testChunkSize)
	for i := 0; i < 21; i++ {
		v := i%3 == 0
		plain.Append(v)
		chunked.Append(v)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()
	checkColumnParity(t, plain, chunked)
	pred := func(b bool) bool { return b }
	checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
}

func TestChunkedInt64ColumnParity(t *testing.T) {
	t.Run("repeats", func(t *testing.T) {
		plain := NewInt64Column(NewColumnDef("i", "I", ""))
		chunked := newChunkedInt64Column(NewColumnDef("i", "I", ""), testChunkSize)
		for i := 0; i < 21; i++ {
			v := int64(i%4) - 2 // negatives included
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkColumnParity(t, plain, chunked)
		pred := func(v int64) bool { return v < 0 }
		checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
	})

	t.Run("key", func(t *testing.T) {
		plain := NewInt64Column(NewColumnDef("k", "K", "thing"))
		chunked := newChunkedInt64Column(NewColumnDef("k", "K", "thing"), testChunkSize)
		for i := 0; i < 21; i++ {
			plain.Append(int64(i * 100))
			chunked.Append(int64(i * 100))
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkColumnParity(t, plain, chunked)
		pi, pe := plain.GetIndex(1500)
		ci, ce := chunked.GetIndex(1500)
		if pe != nil || ce != nil || pi != ci || ci != 15 {
			t.Fatalf("GetIndex(1500): plain (%d, %v), chunked (%d, %v)", pi, pe, ci, ce)
		}
	})
}

func TestChunkedUint64ColumnParity(t *testing.T) {
	plain := NewUint64Column(NewColumnDef("u", "U", ""))
	chunked := newChunkedUint64Column(NewColumnDef("u", "U", ""), testChunkSize)
	for i := 0; i < 21; i++ {
		v := uint64(i % 6)
		plain.Append(v)
		chunked.Append(v)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()
	checkColumnParity(t, plain, chunked)
	pred := func(v uint64) bool { return v%2 == 0 }
	checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
}

func TestChunkedUint32ColumnParity(t *testing.T) {
	plain := NewUint32Column(NewColumnDef("u", "U", ""))
	chunked := newChunkedUint32Column(NewColumnDef("u", "U", ""), testChunkSize)
	for i := 0; i < 21; i++ {
		v := uint32(i % 7)
		plain.Append(v)
		chunked.Append(v)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()
	checkColumnParity(t, plain, chunked)
	pred := func(v uint32) bool { return v > 3 }
	checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
}

func TestChunkedFloat64ColumnParity(t *testing.T) {
	t.Run("specials", func(t *testing.T) {
		values := []float64{1.5, math.NaN(), 0.0, math.Copysign(0, -1), math.Inf(1), 1.5, math.NaN(), math.Inf(-1), 2.25}
		plain := NewFloat64Column(NewColumnDef("f", "F", ""))
		chunked := newChunkedFloat64Column(NewColumnDef("f", "F", ""), 4)
		for _, v := range values {
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		checkColumnParity(t, plain, chunked)
		pred := math.IsNaN
		checkSelectionsEqual(t, "FilterSelection", plain.FilterSelection(pred), chunked.FilterSelection(pred))
	})

	t.Run("NaN disqualifies key", func(t *testing.T) {
		plain := NewFloat64Column(NewColumnDef("f", "F", "thing"))
		chunked := newChunkedFloat64Column(NewColumnDef("f", "F", "thing"), 4)
		for i := 0; i < 9; i++ {
			v := float64(i)
			if i == 5 {
				v = math.NaN()
			}
			plain.Append(v)
			chunked.Append(v)
		}
		plain.FinalizeColumn()
		chunked.FinalizeColumn()
		if plain.IsKey() || chunked.IsKey() {
			t.Fatalf("NaN column detected as key: plain %v, chunked %v", plain.IsKey(), chunked.IsKey())
		}
	})
}

// TestChunkedColumnAsJoinSource joins through a chunked key column into a
// chunked measure column, exercising the widened joined-column constructors.
func TestChunkedColumnAsJoinSource(t *testing.T) {
	// Target table: chunked PK + chunked measure.
	pk := newChunkedStringColumn(NewColumnDef("id", "ID", "thing"), testChunkSize)
	measure := newChunkedInt64Column(NewColumnDef("m", "M", ""), testChunkSize)
	for i := 0; i < 10; i++ {
		pk.Append(fmt.Sprintf("id%d", i))
		measure.Append(int64(i * 11))
	}
	pk.FinalizeColumn()
	measure.FinalizeColumn()
	if !pk.IsKey() {
		t.Fatal("target PK not detected as key")
	}

	// Source table: FK referencing the target.
	fk := NewStringColumn(NewColumnDef("ref", "Ref", "thing"))
	for _, v := range []string{"id3", "id0", "id9", "missing"} {
		fk.Append(v)
	}
	fk.FinalizeColumn()

	joiner := &JoinerString{Joiner[string]{FromColumn: fk, ToColumn: pk}}
	joined := measure.CreateJoinedColumn(NewColumnDef("ref.m", "Ref M", ""), joiner)
	if joined == nil {
		t.Fatal("CreateJoinedColumn returned nil")
	}
	for i, want := range []string{"33", "0", "99"} {
		got, err := joined.GetString(uint32(i))
		if err != nil || got != want {
			t.Fatalf("joined.GetString(%d) = %q, %v; want %q, nil", i, got, err, want)
		}
	}
	if _, err := joined.GetString(3); err == nil {
		t.Fatal("join of missing key succeeded, want unmatched error")
	}
}
