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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// countingStringKeyColumn wraps a string key column and counts GetIndex calls,
// so tests can assert the §6 property: a join sweep hits the key index once
// per distinct FK value, not once per row.
type countingStringKeyColumn struct {
	IDataColumnT[string]
	getIndexCalls atomic.Int64
}

func (c *countingStringKeyColumn) GetIndex(v string) (uint32, error) {
	c.getIndexCalls.Add(1)
	return c.IDataColumnT.GetIndex(v)
}

// perCodeFromColumns builds the same FK data in all six dictionary-encoded
// representations (plain and chunked, three code widths each).
func perCodeFromColumns(values []string) map[string]IDataColumn {
	def := func() *ColumnDef { return NewColumnDef("fk", "FK", "thing") }
	cols := map[string]IDataColumn{
		"plain-uint8":    NewDictStringColumn[uint8](def()),
		"plain-uint16":   NewDictStringColumn[uint16](def()),
		"plain-uint32":   NewDictStringColumn[uint32](def()),
		"chunked-uint8":  NewChunkedDictStringColumn[uint8](def()),
		"chunked-uint16": NewChunkedDictStringColumn[uint16](def()),
		"chunked-uint32": NewChunkedDictStringColumn[uint32](def()),
	}
	type appender interface{ Append(string) }
	for _, c := range cols {
		a := c.(appender)
		for _, v := range values {
			a.Append(v)
		}
	}
	return cols
}

// perCodeToColumns builds the same key data ("k00".."k09", sorted and unique)
// in every string to-side representation a join can target.
func perCodeToColumns(t *testing.T) map[string]IDataColumnT[string] {
	t.Helper()
	keys := make([]string, 10)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
	}
	def := func() *ColumnDef { return NewColumnDef("pk", "PK", "thing") }

	plain := NewStringColumn(def())
	chunked := NewChunkedStringColumn(def())
	dict := NewChunkedDictStringColumn[uint16](def())
	arena := NewChunkedArenaStringColumn(def())
	fcSrc := NewChunkedStringColumn(def())
	for _, k := range keys {
		plain.Append(k)
		chunked.Append(k)
		dict.Append(k)
		arena.Append(k)
		fcSrc.Append(k)
	}
	plain.FinalizeColumn()
	chunked.FinalizeColumn()
	dict.FinalizeColumn()
	arena.FinalizeColumn()
	fcSrc.FinalizeColumn()
	fc, ok := FrontCodeChunkedStringColumn(fcSrc)
	if !ok {
		t.Fatal("front coding declined the sorted unique key column")
	}
	return map[string]IDataColumnT[string]{
		"plain":       plain,
		"chunked":     chunked,
		"dict":        dict,
		"arena":       arena,
		"front-coded": fc,
	}
}

// TestPerCodeJoinerFor pins which column pairs get a per-code joiner: every
// dictionary-encoded from side against a string to side, nothing else.
func TestPerCodeJoinerFor(t *testing.T) {
	to := NewStringColumn(NewColumnDef("pk", "PK", "thing"))
	to.Append("a")
	to.FinalizeColumn()

	for name, from := range perCodeFromColumns([]string{"a", "b"}) {
		if PerCodeJoinerFor(from, to) == nil {
			t.Errorf("%s: no per-code joiner for a dict from side", name)
		}
	}

	plainStr := NewStringColumn(NewColumnDef("fk", "FK", "thing"))
	chunkedStr := NewChunkedStringColumn(NewColumnDef("fk", "FK", "thing"))
	arena := NewChunkedArenaStringColumn(NewColumnDef("fk", "FK", "thing"))
	for _, from := range []IDataColumn{plainStr, chunkedStr, arena} {
		if j := PerCodeJoinerFor(from, to); j != nil {
			t.Errorf("%T: per-code joiner for a non-dict from side: %T", from, j)
		}
	}

	dictFrom := NewDictStringColumn[uint8](NewColumnDef("fk", "FK", "thing"))
	intTo := NewInt64Column(NewColumnDef("pk", "PK", "num"))
	if j := PerCodeJoinerFor(dictFrom, intTo); j != nil {
		t.Errorf("per-code joiner for a non-string to side: %T", j)
	}
}

// TestPerCodeJoinerParity checks Lookup row by row against the per-row
// Joiner[string] for every dict from-representation crossed with every
// string to-representation, including unmatched values and out-of-range rows.
func TestPerCodeJoinerParity(t *testing.T) {
	// Matched keys, an unmatched value, and repeats out of interning order.
	values := []string{"k03", "k07", "zzz-missing", "k00", "k07", "k09", "zzz-missing", "k03"}
	froms := perCodeFromColumns(values)
	tos := perCodeToColumns(t)

	for fromName, from := range froms {
		for toName, to := range tos {
			t.Run(fromName+"->"+toName, func(t *testing.T) {
				perCode := PerCodeJoinerFor(from, to)
				if perCode == nil {
					t.Fatal("no per-code joiner")
				}
				perRow := &Joiner[string]{FromColumn: from.(IDataColumnT[string]), ToColumn: to}

				for i := uint32(0); i < uint32(len(values)); i++ {
					gotIdx, gotErr := perCode.Lookup(i)
					wantIdx, wantErr := perRow.Lookup(i)
					if (gotErr != nil) != (wantErr != nil) {
						t.Fatalf("row %d: per-code err %v, per-row err %v", i, gotErr, wantErr)
					}
					if gotErr != nil {
						if !errors.Is(gotErr, ErrUnmatched) {
							t.Fatalf("row %d: unmatched error is %v, want ErrUnmatched", i, gotErr)
						}
						continue
					}
					if gotIdx != wantIdx {
						t.Fatalf("row %d: per-code %d, per-row %d", i, gotIdx, wantIdx)
					}
				}

				// Out of range errors, and not as an unmatched join.
				if _, err := perCode.Lookup(uint32(len(values))); err == nil {
					t.Fatal("out-of-range row: no error")
				} else if errors.Is(err, ErrUnmatched) {
					t.Fatal("out-of-range row reported as unmatched")
				}
			})
		}
	}
}

// TestPerCodeJoinerNonKeyTarget pins the behaviour against a target column
// that rejects reverse lookups (never finalized, so not a key): every lookup
// is unmatched, exactly as the per-row joiner errors on every row.
func TestPerCodeJoinerNonKeyTarget(t *testing.T) {
	to := NewStringColumn(NewColumnDef("pk", "PK", "thing"))
	to.Append("a") // never finalized -> not a key -> GetIndex errors
	from := NewDictStringColumn[uint8](NewColumnDef("fk", "FK", "thing"))
	from.Append("a")

	j := PerCodeJoinerFor(from, to)
	if _, err := j.Lookup(0); !errors.Is(err, ErrUnmatched) {
		t.Fatalf("non-key target: got %v, want ErrUnmatched", err)
	}
}

// TestPerCodeJoinerDLookups asserts the acceptance property from
// docs/scaling-to-1b-rows.md §6: sweeping a join over n rows hits the target
// key index d times (once per distinct FK value), not n times.
func TestPerCodeJoinerDLookups(t *testing.T) {
	const n, d = 10000, 7
	from := NewChunkedDictStringColumn[uint16](NewColumnDef("fk", "FK", "thing"))
	for i := 0; i < n; i++ {
		from.Append(fmt.Sprintf("k%02d", i%d))
	}
	from.FinalizeColumn()

	tos := perCodeToColumns(t)
	counting := &countingStringKeyColumn{IDataColumnT: tos["front-coded"]}

	j := PerCodeJoinerFor(from, counting)
	for i := uint32(0); i < n; i++ {
		idx, err := j.Lookup(i)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if want := uint32(i % d); idx != want {
			t.Fatalf("row %d: target %d, want %d", i, idx, want)
		}
	}
	if calls := counting.getIndexCalls.Load(); calls != d {
		t.Fatalf("GetIndex called %d times for %d rows, want exactly d=%d", calls, n, d)
	}
}

// TestPerCodeJoinerConcurrent runs concurrent sweeps (meant for -race): the
// memo must be built exactly once and every goroutine must see correct
// results.
func TestPerCodeJoinerConcurrent(t *testing.T) {
	const n, d, goroutines = 4096, 5, 8
	from := NewChunkedDictStringColumn[uint8](NewColumnDef("fk", "FK", "thing"))
	for i := 0; i < n; i++ {
		from.Append(fmt.Sprintf("k%02d", i%d))
	}
	from.FinalizeColumn()

	tos := perCodeToColumns(t)
	counting := &countingStringKeyColumn{IDataColumnT: tos["chunked"]}
	j := PerCodeJoinerFor(from, counting)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint32(0); i < n; i++ {
				idx, err := j.Lookup(i)
				if err != nil {
					errs <- fmt.Errorf("row %d: %v", i, err)
					return
				}
				if want := uint32(i % d); idx != want {
					errs <- fmt.Errorf("row %d: target %d, want %d", i, idx, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if calls := counting.getIndexCalls.Load(); calls != d {
		t.Fatalf("GetIndex called %d times under %d concurrent sweeps, want exactly d=%d (one build)", calls, goroutines, d)
	}
}

// TestPerCodeJoinerAppendedCode covers the append-only growth path: codes
// interned after the memo was built resolve through the direct fallback
// instead of reading past the memo.
func TestPerCodeJoinerAppendedCode(t *testing.T) {
	tos := perCodeToColumns(t)
	to := tos["plain"]

	// Not finalized, so Append after the first Lookup stays legal.
	from := NewDictStringColumn[uint16](NewColumnDef("fk", "FK", "thing"))
	from.Append("k02")
	from.Append("k05")

	j := PerCodeJoinerFor(from, to)
	if idx, err := j.Lookup(0); err != nil || idx != 2 {
		t.Fatalf("Lookup(0) = (%d, %v), want (2, nil)", idx, err)
	}

	// New code after the memo build, plus a reused existing code.
	from.Append("k08")
	from.Append("k05")
	if idx, err := j.Lookup(2); err != nil || idx != 8 {
		t.Fatalf("appended new code: Lookup(2) = (%d, %v), want (8, nil)", idx, err)
	}
	if idx, err := j.Lookup(3); err != nil || idx != 5 {
		t.Fatalf("appended existing code: Lookup(3) = (%d, %v), want (5, nil)", idx, err)
	}
	// A post-build value absent from the target errors like any lookup miss.
	from.Append("zzz-missing")
	if _, err := j.Lookup(4); err == nil {
		t.Fatal("appended unmatched code: no error")
	}
}
