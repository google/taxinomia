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
	"time"
)

// collectAcc is a GroupAccumulator that materialises the partition, for
// comparing GroupAggregates against the other operations.
type collectAcc struct {
	byCode map[uint32][]uint32
}

func newCollectAcc() *collectAcc {
	return &collectAcc{byCode: make(map[uint32][]uint32)}
}

func (a *collectAcc) Add(code uint32, row uint32) {
	a.byCode[code] = append(a.byCode[code], row)
}

// checkGroupOpsParity verifies that ops (the three narrow operations) produce
// exactly the partition GroupIndices produces for the same selection: same
// groups, same members in the same order, consistent codes across the three
// operations, and working pagination.
//
// Groups are matched by their first member, which is unique per group because
// every implementation fills groups in selection order.
//
// The selection is passed to the three operations as a RowIndices RowSet; a
// separate test pins that a Selection bitmap over the same rows behaves
// identically.
func checkGroupOpsParity(t *testing.T, col IDataColumn, ops IGroupOps, selIndices []uint32) {
	t.Helper()
	sel := RowIndices(selIndices)

	grouped, _ := col.GroupIndices(selIndices, nil)
	expected := make(map[uint32][]uint32, len(grouped)) // first member -> members
	for _, members := range grouped {
		if len(members) == 0 {
			t.Fatalf("GroupIndices returned an empty group")
		}
		expected[members[0]] = members
	}

	counts, firsts := ops.GroupCounts(sel)
	if len(counts) != len(firsts) {
		t.Fatalf("GroupCounts: len(counts)=%d != len(firsts)=%d", len(counts), len(firsts))
	}

	acc := newCollectAcc()
	ops.GroupAggregates(sel, acc)

	nonZero := 0
	for code := range counts {
		if counts[code] == 0 {
			if _, ok := acc.byCode[uint32(code)]; ok {
				t.Errorf("code %d: GroupCounts says empty but GroupAggregates visited it", code)
			}
			continue
		}
		nonZero++

		members := ops.GroupMembers(sel, uint32(code), 0, -1)
		if uint32(len(members)) != counts[code] {
			t.Errorf("code %d: GroupMembers returned %d members, GroupCounts says %d", code, len(members), counts[code])
			continue
		}
		if members[0] != firsts[code] {
			t.Errorf("code %d: first member %d != firsts[%d]=%d", code, members[0], code, firsts[code])
		}
		want, ok := expected[members[0]]
		if !ok {
			t.Errorf("code %d: no GroupIndices group starts at row %d", code, members[0])
			continue
		}
		if !reflect.DeepEqual(members, want) {
			t.Errorf("code %d: members mismatch\n got %v\nwant %v", code, members, want)
		}
		if got := acc.byCode[uint32(code)]; !reflect.DeepEqual(got, want) {
			t.Errorf("code %d: GroupAggregates visit mismatch\n got %v\nwant %v", code, got, want)
		}

		// Pagination: middle page, empty page, offset past the end.
		if len(members) > 1 {
			if got := ops.GroupMembers(sel, uint32(code), 1, 1); !reflect.DeepEqual(got, members[1:2]) {
				t.Errorf("code %d: GroupMembers(offset=1,n=1) = %v, want %v", code, got, members[1:2])
			}
		}
		if got := ops.GroupMembers(sel, uint32(code), 0, 0); len(got) != 0 {
			t.Errorf("code %d: GroupMembers(n=0) returned %v, want empty", code, got)
		}
		if got := ops.GroupMembers(sel, uint32(code), len(members), -1); len(got) != 0 {
			t.Errorf("code %d: GroupMembers(offset=len) returned %v, want empty", code, got)
		}
	}
	if nonZero != len(expected) {
		t.Errorf("non-empty group count %d != GroupIndices group count %d", nonZero, len(expected))
	}
	if len(acc.byCode) != len(expected) {
		t.Errorf("GroupAggregates visited %d groups, GroupIndices has %d", len(acc.byCode), len(expected))
	}
}

// nativeParity asserts col implements IGroupOps natively and checks parity.
func nativeParity(t *testing.T, col IDataColumn, sel []uint32) {
	t.Helper()
	ops, ok := col.(IGroupOps)
	if !ok {
		t.Fatalf("%T does not implement IGroupOps", col)
	}
	checkGroupOpsParity(t, col, ops, sel)
}

func selRange(n int) []uint32 {
	sel := make([]uint32, n)
	for i := range sel {
		sel[i] = uint32(i)
	}
	return sel
}

func TestGroupOpsStringColumn(t *testing.T) {
	col := NewStringColumn(NewColumnDef("s", "S", ""))
	values := []string{"b", "a", "b", "c", "a", "a", "d", "b"}
	for _, v := range values {
		col.Append(v)
	}
	col.FinalizeColumn()

	nativeParity(t, col, selRange(len(values)))
	nativeParity(t, col, []uint32{7, 3, 1, 0})
	nativeParity(t, col, nil)
}

func TestGroupOpsUint32Column(t *testing.T) {
	col := NewUint32Column(NewColumnDef("u", "U", ""))
	for _, v := range []uint32{5, 2, 5, 9, 2, 2, 5} {
		col.Append(v)
	}
	col.FinalizeColumn()

	nativeParity(t, col, selRange(7))
	// Out-of-range indices are dropped, matching GroupIndices.
	nativeParity(t, col, []uint32{0, 42, 3, 99, 5})
}

func TestGroupOpsInt64Column(t *testing.T) {
	col := NewInt64Column(NewColumnDef("i", "I", ""))
	for _, v := range []int64{-3, 7, -3, 0, 7, 7} {
		col.Append(v)
	}
	col.FinalizeColumn()
	nativeParity(t, col, selRange(6))
	nativeParity(t, col, []uint32{1, 100, 4})
}

func TestGroupOpsUint64Column(t *testing.T) {
	col := NewUint64Column(NewColumnDef("u", "U", ""))
	for _, v := range []uint64{1, math.MaxUint64, 1, 42, math.MaxUint64} {
		col.Append(v)
	}
	col.FinalizeColumn()
	nativeParity(t, col, selRange(5))
}

func TestGroupOpsFloat64Column(t *testing.T) {
	col := NewFloat64Column(NewColumnDef("f", "F", ""))
	negZero := math.Copysign(0, -1)
	for _, v := range []float64{1.5, math.NaN(), 0, negZero, 1.5, math.NaN(), 2.25} {
		col.Append(v)
	}
	col.FinalizeColumn()

	// All NaNs one group, -0 groups with +0 — the Float64Column semantics.
	nativeParity(t, col, selRange(7))
	nativeParity(t, col, []uint32{6, 5, 1, 3})
}

func TestGroupOpsBoolColumn(t *testing.T) {
	col := NewBoolColumn(NewColumnDef("b", "B", ""))
	for _, v := range []bool{true, false, true, true, false} {
		col.Append(v)
	}
	col.FinalizeColumn()
	nativeParity(t, col, selRange(5))
	nativeParity(t, col, []uint32{4, 0, 9})
}

func TestGroupOpsDatetimeColumn(t *testing.T) {
	col := NewDatetimeColumn(NewColumnDef("d", "D", ""))
	t1 := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	t2 := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
	for _, v := range []time.Time{t1, t2, t1, t1, t2} {
		col.Append(v)
	}
	col.FinalizeColumn()
	nativeParity(t, col, selRange(5))
}

func TestGroupOpsDurationColumn(t *testing.T) {
	col := NewDurationColumn(NewColumnDef("d", "D", ""))
	for _, v := range []time.Duration{time.Second, time.Minute, time.Second, 0, time.Minute} {
		col.Append(v)
	}
	nativeParity(t, col, selRange(5))
}

func buildDictColumn(t *testing.T, n, distinct int) IDataColumn {
	t.Helper()
	src := NewStringColumn(NewColumnDef("dict", "Dict", ""))
	for i := 0; i < n; i++ {
		src.Append(fmt.Sprintf("val-%04d", i%distinct))
	}
	src.FinalizeColumn()
	col, ok := CompactStringColumn(src)
	if !ok {
		t.Fatalf("expected compaction for n=%d d=%d", n, distinct)
	}
	return col
}

func TestGroupOpsDictColumnDense(t *testing.T) {
	col := buildDictColumn(t, 5000, 11)
	nativeParity(t, col, selRange(5000))
	// A slice of the middle keeps the dense path (len(sel) >= d/8 trivially).
	mid := make([]uint32, 0, 300)
	for i := 700; i < 1000; i++ {
		mid = append(mid, uint32(i))
	}
	nativeParity(t, col, mid)
}

func TestGroupOpsDictColumnSmallSubset(t *testing.T) {
	// 1000 distinct values, selection of 50 rows: 50 < 1000/8 takes the hash
	// fallback, whose codes are first-encounter rather than dictionary codes.
	col := buildDictColumn(t, 5000, 1000)
	sel := make([]uint32, 0, 50)
	for i := 0; i < 50; i++ {
		sel = append(sel, uint32(i*97%5000))
	}
	nativeParity(t, col, sel)
}

// testJoiner maps source rows to target rows, failing rows not present.
type testJoiner struct {
	mapping map[uint32]uint32
}

func (j *testJoiner) Lookup(i uint32) (uint32, error) {
	if target, ok := j.mapping[i]; ok {
		return target, nil
	}
	return 0, ErrUnmatched
}

// joinerFor builds a joiner over n rows mapping row i -> i % targets, with
// every 4th row unmatched.
func joinerFor(n, targets int) IJoiner {
	m := make(map[uint32]uint32)
	for i := 0; i < n; i++ {
		if i%4 == 3 {
			continue
		}
		m[uint32(i)] = uint32(i % targets)
	}
	return &testJoiner{mapping: m}
}

func TestGroupOpsJoinedColumns(t *testing.T) {
	const n, targets = 12, 3
	joiner := joinerFor(n, targets)
	sel := selRange(n)

	strSrc := NewStringColumn(NewColumnDef("name", "Name", ""))
	strSrc.Append("x")
	strSrc.Append("y")
	strSrc.Append("x")
	strSrc.FinalizeColumn()
	nativeParity(t, NewJoinedStringColumn(NewColumnDef("j", "J", ""), joiner, strSrc), sel)

	u32Src := NewUint32Column(NewColumnDef("n", "N", ""))
	u32Src.Append(7)
	u32Src.Append(7)
	u32Src.Append(9)
	u32Src.FinalizeColumn()
	nativeParity(t, NewJoinedUint32Column(NewColumnDef("j", "J", ""), joiner, u32Src), sel)

	dtSrc := NewDatetimeColumn(NewColumnDef("d", "D", ""))
	base := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	dtSrc.Append(base)
	dtSrc.Append(base.Add(time.Hour))
	dtSrc.Append(base)
	dtSrc.FinalizeColumn()
	nativeParity(t, NewJoinedDatetimeColumn(NewColumnDef("j", "J", ""), joiner, dtSrc), sel)

	durSrc := NewDurationColumn(NewColumnDef("d", "D", ""))
	durSrc.Append(time.Second)
	durSrc.Append(time.Minute)
	durSrc.Append(time.Second)
	nativeParity(t, NewJoinedDurationColumn(NewColumnDef("j", "J", ""), joiner, durSrc), sel)

	boolSrc := NewBoolColumn(NewColumnDef("b", "B", ""))
	boolSrc.Append(true)
	boolSrc.Append(false)
	boolSrc.Append(true)
	nativeParity(t, NewJoinedBoolColumn(NewColumnDef("j", "J", ""), joiner, boolSrc), sel)

	f64Src := NewFloat64Column(NewColumnDef("f", "F", ""))
	f64Src.Append(1.5)
	f64Src.Append(math.NaN())
	f64Src.Append(1.5)
	f64Src.FinalizeColumn()
	nativeParity(t, NewJoinedFloat64Column(NewColumnDef("j", "J", ""), joiner, f64Src), sel)

	i64Src := NewInt64Column(NewColumnDef("i", "I", ""))
	i64Src.Append(-1)
	i64Src.Append(5)
	i64Src.Append(-1)
	i64Src.FinalizeColumn()
	nativeParity(t, NewJoinedInt64Column(NewColumnDef("j", "J", ""), joiner, i64Src), sel)

	u64Src := NewUint64Column(NewColumnDef("u", "U", ""))
	u64Src.Append(11)
	u64Src.Append(22)
	u64Src.Append(11)
	u64Src.FinalizeColumn()
	nativeParity(t, NewJoinedUint64Column(NewColumnDef("j", "J", ""), joiner, u64Src), sel)
}

func TestGroupOpsComputedColumns(t *testing.T) {
	const n = 16
	sel := selRange(n)
	failing := func(i uint32) bool { return i%5 == 2 } // computed error rows

	nativeParity(t, NewComputedStringColumn(NewColumnDef("c", "C", ""), n, func(i uint32) (string, error) {
		if failing(i) {
			return "", fmt.Errorf("no value")
		}
		return fmt.Sprintf("s%d", i%3), nil
	}), sel)

	nativeParity(t, NewComputedUint32Column(NewColumnDef("c", "C", ""), n, func(i uint32) (uint32, error) {
		if failing(i) {
			return 0, fmt.Errorf("no value")
		}
		return i % 4, nil
	}), sel)

	nativeParity(t, NewComputedInt64Column(NewColumnDef("c", "C", ""), n, func(i uint32) (int64, error) {
		if failing(i) {
			return 0, fmt.Errorf("no value")
		}
		return int64(i%3) - 1, nil
	}), sel)

	// Raw float64 keys: every NaN row is its own group (the historical
	// map[float64] behaviour), which parity with GroupIndices confirms.
	nativeParity(t, NewComputedFloat64Column(NewColumnDef("c", "C", ""), n, func(i uint32) (float64, error) {
		if failing(i) {
			return 0, fmt.Errorf("no value")
		}
		if i%6 == 1 {
			return math.NaN(), nil
		}
		return float64(i % 3), nil
	}), sel)

	nativeParity(t, NewComputedDatetimeColumn(NewColumnDef("c", "C", ""), n, func(i uint32) (int64, error) {
		if failing(i) {
			return 0, fmt.Errorf("no value")
		}
		return int64(i%3) * 1e9, nil
	}), sel)

	nativeParity(t, NewComputedDurationColumn(NewColumnDef("c", "C", ""), n, func(i uint32) (time.Duration, error) {
		if failing(i) {
			return 0, fmt.Errorf("no value")
		}
		return time.Duration(i%3) * time.Second, nil
	}), sel)

	nativeParity(t, NewComputedBoolColumn(NewColumnDef("c", "C", ""), n, func(i uint32) (bool, error) {
		if failing(i) {
			return false, fmt.Errorf("no value")
		}
		return i%2 == 0, nil
	}), sel)
}

// externalColumn wraps a column exposing only IDataColumn, simulating an
// external implementation that predates IGroupOps.
type externalColumn struct {
	inner IDataColumn
}

func (c *externalColumn) ColumnDef() *ColumnDef      { return c.inner.ColumnDef() }
func (c *externalColumn) Length() int                { return c.inner.Length() }
func (c *externalColumn) GetString(i uint32) (string, error) { return c.inner.GetString(i) }
func (c *externalColumn) IsKey() bool                { return c.inner.IsKey() }
func (c *externalColumn) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return c.inner.CreateJoinedColumn(columnDef, joiner)
}
func (c *externalColumn) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	return c.inner.GroupIndices(indices, columnView)
}

func TestGroupOpsFallbackShim(t *testing.T) {
	inner := NewStringColumn(NewColumnDef("s", "S", ""))
	for _, v := range []string{"b", "a", "b", "c", "a", "a"} {
		inner.Append(v)
	}
	inner.FinalizeColumn()
	col := &externalColumn{inner: inner}

	if _, ok := interface{}(col).(IGroupOps); ok {
		t.Fatal("externalColumn must not implement IGroupOps for this test")
	}
	ops := GroupOpsFor(col, nil)
	if _, isShim := ops.(*groupIndicesShim); !isShim {
		t.Fatalf("GroupOpsFor returned %T, want the GroupIndices shim", ops)
	}
	checkGroupOpsParity(t, col, ops, selRange(6))

	// And for a native column GroupOpsFor must return the column itself.
	if ops := GroupOpsFor(inner, nil); ops != IGroupOps(inner) {
		t.Fatalf("GroupOpsFor(native) returned %T, want the column", ops)
	}
}
