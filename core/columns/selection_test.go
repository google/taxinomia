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
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func TestSelectionEmptyAndAll(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 70, 128, 1000} {
		empty := NewSelection(n)
		if got := empty.Count(); got != 0 {
			t.Errorf("NewSelection(%d).Count() = %d, want 0", n, got)
		}
		if got := empty.Len(); got != n {
			t.Errorf("NewSelection(%d).Len() = %d, want %d", n, got, n)
		}

		all := NewSelectionAll(n)
		if got := all.Count(); got != n {
			t.Errorf("NewSelectionAll(%d).Count() = %d, want %d", n, got, n)
		}
		if n > 0 && !all.Contains(uint32(n-1)) {
			t.Errorf("NewSelectionAll(%d) misses last row", n)
		}
		if all.Contains(uint32(n)) {
			t.Errorf("NewSelectionAll(%d) claims to contain row %d beyond the universe", n, n)
		}
		if got := len(all.ToIndices()); got != n {
			t.Errorf("NewSelectionAll(%d).ToIndices() has %d entries, want %d", n, got, n)
		}
	}
}

func TestSelectionAddRemoveContains(t *testing.T) {
	s := NewSelection(100)
	for _, i := range []uint32{0, 63, 64, 99} {
		s.Add(i)
	}
	if got := s.Count(); got != 4 {
		t.Fatalf("Count() = %d, want 4", got)
	}
	for _, i := range []uint32{0, 63, 64, 99} {
		if !s.Contains(i) {
			t.Errorf("Contains(%d) = false, want true", i)
		}
	}
	if s.Contains(1) || s.Contains(100) || s.Contains(200) {
		t.Error("Contains reports unselected or out-of-universe rows")
	}

	s.Remove(63)
	s.Remove(200) // out of universe: no-op
	if s.Contains(63) || s.Count() != 3 {
		t.Errorf("after Remove(63): Contains=%v Count=%d, want false/3", s.Contains(63), s.Count())
	}

	defer func() {
		if recover() == nil {
			t.Error("Add beyond the universe did not panic")
		}
	}()
	s.Add(100)
}

func TestSelectionAnd(t *testing.T) {
	a := SelectionFromIndices(200, []uint32{1, 5, 64, 65, 199})
	b := SelectionFromIndices(200, []uint32{5, 64, 100, 199})
	a.And(b)
	want := []uint32{5, 64, 199}
	if got := a.ToIndices(); !reflect.DeepEqual(got, want) {
		t.Errorf("And: got %v, want %v", got, want)
	}

	defer func() {
		if recover() == nil {
			t.Error("And over mismatched universes did not panic")
		}
	}()
	a.And(NewSelection(100))
}

func TestSelectionForEach(t *testing.T) {
	indices := []uint32{3, 7, 63, 64, 128, 500}
	s := SelectionFromIndices(600, indices)

	var visited []uint32
	s.ForEach(func(i uint32) { visited = append(visited, i) })
	if !reflect.DeepEqual(visited, indices) {
		t.Errorf("ForEach visited %v, want ascending %v", visited, indices)
	}

	// Removing rows in later words during iteration is observed; removing the
	// row being visited is safe.
	s = SelectionFromIndices(600, indices)
	visited = nil
	s.ForEach(func(i uint32) {
		visited = append(visited, i)
		if i == 3 {
			s.Remove(500) // later word: must not be visited
		}
		s.Remove(i) // current row: safe
	})
	want := []uint32{3, 7, 63, 64, 128}
	if !reflect.DeepEqual(visited, want) {
		t.Errorf("ForEach with removals visited %v, want %v", visited, want)
	}
	if got := s.Count(); got != 0 {
		t.Errorf("after removing every visited row Count() = %d, want 0", got)
	}
}

func TestSelectionFromIndicesRoundTrip(t *testing.T) {
	const n = 10_000
	rng := rand.New(rand.NewSource(42))
	want := make([]uint32, 0, n/3)
	seen := make(map[uint32]bool)
	for i := 0; i < n/3; i++ {
		v := uint32(rng.Intn(n))
		if !seen[v] {
			seen[v] = true
			want = append(want, v)
		}
	}
	s := SelectionFromIndices(n, want)
	got := s.ToIndices()
	if len(got) != len(want) {
		t.Fatalf("round trip lost indices: got %d, want %d", len(got), len(want))
	}
	for _, v := range want {
		if !s.Contains(v) {
			t.Fatalf("round trip lost index %d", v)
		}
	}
}

// intsToUint32 converts the []int returned by the deprecated Filter methods
// for comparison against Selection.ToIndices.
func intsToUint32(indices []int) []uint32 {
	out := make([]uint32, len(indices))
	for i, v := range indices {
		out[i] = uint32(v)
	}
	return out
}

// TestFilterSelectionParity checks, for every column type with a Filter
// method, that FilterSelection selects exactly the rows Filter returns.
func TestFilterSelectionParity(t *testing.T) {
	const rows = 1000

	t.Run("string", func(t *testing.T) {
		c := NewStringColumn(NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(fmt.Sprintf("value-%03d", i%97))
		}
		pred := func(v string) bool { return strings.HasSuffix(v, "7") }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})

	t.Run("dict", func(t *testing.T) {
		c := NewDictStringColumn[uint16](NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(fmt.Sprintf("value-%03d", i%97))
		}
		c.FinalizeColumn()
		pred := func(v string) bool { return strings.HasSuffix(v, "7") }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})

	t.Run("bool", func(t *testing.T) {
		c := NewBoolColumn(NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(i%3 == 0)
		}
		pred := func(v bool) bool { return v }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})

	t.Run("float64", func(t *testing.T) {
		c := NewFloat64Column(NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(float64(i%251) / 10)
		}
		pred := func(v float64) bool { return v > 12.5 }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})

	t.Run("int64", func(t *testing.T) {
		c := NewInt64Column(NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(int64(i%501) - 250)
		}
		pred := func(v int64) bool { return v < 0 }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})

	t.Run("uint64", func(t *testing.T) {
		c := NewUint64Column(NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(uint64(i % 353))
		}
		pred := func(v uint64) bool { return v%5 == 0 }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})

	t.Run("uint32", func(t *testing.T) {
		c := NewUint32Column(NewColumnDef("c", "C", ""))
		for i := 0; i < rows; i++ {
			c.Append(uint32(i % 353))
		}
		pred := func(v uint32) bool { return v%5 == 0 }
		assertFilterParity(t, intsToUint32(c.Filter(pred)), c.FilterSelection(pred))
	})
}

func assertFilterParity(t *testing.T, want []uint32, sel *Selection) {
	t.Helper()
	if len(want) == 0 {
		t.Fatal("test predicate matched no rows; parity check would be vacuous")
	}
	got := sel.ToIndices()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FilterSelection selected %d rows, Filter returned %d; first divergence around %v vs %v",
			len(got), len(want), truncate(got), truncate(want))
	}
}

func truncate(indices []uint32) []uint32 {
	if len(indices) > 10 {
		return indices[:10]
	}
	return indices
}
