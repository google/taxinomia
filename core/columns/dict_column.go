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
	"sort"
)

// DictStringColumn is optimized for low-cardinality string data, where the same
// values repeat many times. Instead of one string header per row, it stores a
// dictionary of the distinct values plus one narrow code per row.
//
// K selects the code width: uint8 for up to 256 distinct values, uint16 for up
// to 65536, uint32 beyond. Per-row cost is sizeof(K) bytes instead of the 16
// bytes of a string header plus the (duplicated) string bytes.
//
// The code doubles as the group key, so GroupIndices needs no hashing at all,
// and Filter evaluates its predicate once per distinct value rather than once
// per row.
//
// Use CompactStringColumn to build one from a loaded StringColumn; it picks the
// code width from the observed cardinality.
type DictStringColumn[K Unsigned] struct {
	columnDef *ColumnDef
	dict      []string    // code -> distinct value
	codes     []K         // row -> code
	index     map[string]K // value -> code (size d, kept for Append/Filter/GetIndex)
	isKey     bool
	ranks     []K // code -> sort rank, built lazily by Ranks()
}

// NewDictStringColumn creates an empty dictionary-encoded string column.
func NewDictStringColumn[K Unsigned](columnDef *ColumnDef) *DictStringColumn[K] {
	return &DictStringColumn[K]{
		columnDef: columnDef,
		dict:      make([]string, 0),
		codes:     make([]K, 0),
		index:     make(map[string]K),
	}
}

// maxCode returns the largest value representable by the code type.
func maxCode[K Unsigned]() int {
	var zero K
	switch any(zero).(type) {
	case uint8:
		return 1<<8 - 1
	case uint16:
		return 1<<16 - 1
	default:
		return 1<<32 - 1
	}
}

// Append adds a value, interning it in the dictionary.
//
// It panics if the number of distinct values would exceed what the code width K
// can represent. Callers that don't know the cardinality up front should build a
// StringColumn and run CompactStringColumn, which picks K from the data.
func (c *DictStringColumn[K]) Append(value string) {
	code, exists := c.index[value]
	if !exists {
		if len(c.dict) > maxCode[K]() {
			panic(fmt.Sprintf("column %q: dictionary exceeded code width (%d distinct values)", c.columnDef.Name(), len(c.dict)))
		}
		code = K(len(c.dict))
		c.dict = append(c.dict, value)
		c.index[value] = code
	}
	c.codes = append(c.codes, code)
}

func (c *DictStringColumn[K]) Length() int {
	return len(c.codes)
}

func (c *DictStringColumn[K]) ColumnDef() *ColumnDef {
	return c.columnDef
}

// Cardinality returns the number of distinct values held in the dictionary.
func (c *DictStringColumn[K]) Cardinality() int {
	return len(c.dict)
}

func (c *DictStringColumn[K]) GetValue(i uint32) (string, error) {
	if i >= uint32(len(c.codes)) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.codes))
	}
	return c.dict[c.codes[i]], nil
}

// GetString returns the string value at index i. No formatting, no allocation.
func (c *DictStringColumn[K]) GetString(i uint32) (string, error) {
	if i >= uint32(len(c.codes)) {
		return "", fmt.Errorf("index %d out of bounds (length: %d)", i, len(c.codes))
	}
	return c.dict[c.codes[i]], nil
}

// GetIndex returns the row index holding the given value.
//
// Only meaningful for key columns. When every value is distinct the n-th
// appended value receives code n, so the code is also the row index.
func (c *DictStringColumn[K]) GetIndex(v string) (uint32, error) {
	if !c.isKey {
		return 0, fmt.Errorf("column %q is not a key column and doesn't support reverse lookups", c.columnDef.Name())
	}
	if code, exists := c.index[v]; exists {
		return uint32(code), nil
	}
	return 0, fmt.Errorf("value %q not found in column %q", v, c.columnDef.Name())
}

// GetCode returns the dictionary code for row i. Joiners can use it to memoize
// a resolution per distinct value rather than per row.
func (c *DictStringColumn[K]) GetCode(i uint32) K {
	return c.codes[i]
}

// DictValue returns the value for a dictionary code, which is also the group key
// returned by GroupIndices.
func (c *DictStringColumn[K]) DictValue(code uint32) string {
	return c.dict[code]
}

func (c *DictStringColumn[K]) IsKey() bool {
	return c.isKey
}

func (c *DictStringColumn[K]) CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn {
	return NewJoinedStringColumn(columnDef, joiner, c)
}

// FinalizeColumn detects whether the column happens to be unique. The
// dictionary and its index are already built by Append, so there is no second
// pass over the rows.
func (c *DictStringColumn[K]) FinalizeColumn() {
	c.isKey = len(c.dict) == len(c.codes)
}

// Filter returns the indices whose value satisfies the predicate. The predicate
// runs once per distinct value, not once per row.
func (c *DictStringColumn[K]) Filter(predicate func(string) bool) []int {
	keep := make([]bool, len(c.dict))
	for code, value := range c.dict {
		keep[code] = predicate(value)
	}
	indices := make([]int, 0)
	for i, code := range c.codes {
		if keep[code] {
			indices = append(indices, i)
		}
	}
	return indices
}

// Ranks returns, per dictionary code, the position of its value in sorted order.
// Built once and cached, it turns row comparison into an integer compare.
func (c *DictStringColumn[K]) Ranks() []K {
	if c.ranks != nil {
		return c.ranks
	}
	order := make([]int, len(c.dict))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return c.dict[order[a]] < c.dict[order[b]] })

	c.ranks = make([]K, len(c.dict))
	for rank, code := range order {
		c.ranks[code] = K(rank)
	}
	return c.ranks
}

// GroupIndices buckets the given indices by dictionary code.
//
// The code is the group key, so this is a counting sort over a dense array: no
// hashing, no string comparison, and a single allocation for all the group
// slices rather than one per group.
func (c *DictStringColumn[K]) GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32) {
	if len(indices) == 0 {
		return map[uint32][]uint32{}, nil
	}

	// Pass 1: count rows per code.
	counts := make([]uint32, len(c.dict))
	for _, i := range indices {
		counts[c.codes[i]]++
	}

	// Lay the groups out contiguously in one backing array.
	offsets := make([]uint32, len(c.dict))
	nonEmpty := 0
	var off uint32
	for code, n := range counts {
		offsets[code] = off
		off += n
		if n > 0 {
			nonEmpty++
		}
	}

	// Pass 2: scatter each index into its group's slot.
	backing := make([]uint32, len(indices))
	cursor := make([]uint32, len(c.dict))
	copy(cursor, offsets)
	for _, i := range indices {
		code := c.codes[i]
		backing[cursor[code]] = i
		cursor[code]++
	}

	grouped := make(map[uint32][]uint32, nonEmpty)
	for code, n := range counts {
		if n == 0 {
			continue
		}
		start := offsets[code]
		grouped[uint32(code)] = backing[start : start+n : start+n]
	}

	// Every row resolves to a code, so nothing is left unmapped.
	return grouped, nil
}

// CompactStringColumn converts a loaded StringColumn into a dictionary-encoded
// column when the data is repetitive enough to benefit, choosing the narrowest
// code width that fits.
//
// It reports whether compaction happened; when it returns false the original
// column is returned unchanged. This is the intended hook for FinalizeColumn:
// the cardinality is known only after the data is loaded.
func CompactStringColumn(c *StringColumn) (IDataColumn, bool) {
	n := len(c.data)
	if n == 0 {
		return c, false
	}

	// Count distinct values, bailing out as soon as the column looks
	// high-cardinality enough that encoding would not pay for itself.
	threshold := n / 2
	seen := make(map[string]struct{}, min(threshold, 1024))
	for _, v := range c.data {
		seen[v] = struct{}{}
		if len(seen) > threshold {
			return c, false
		}
	}

	d := len(seen)
	switch {
	case d <= 1<<8:
		return buildDict[uint8](c), true
	case d <= 1<<16:
		return buildDict[uint16](c), true
	default:
		return buildDict[uint32](c), true
	}
}

func buildDict[K Unsigned](c *StringColumn) *DictStringColumn[K] {
	dc := NewDictStringColumn[K](c.columnDef)
	dc.codes = make([]K, 0, len(c.data))
	for _, v := range c.data {
		dc.Append(v)
	}
	dc.FinalizeColumn()
	return dc
}
