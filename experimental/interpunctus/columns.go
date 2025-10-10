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

package main

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
)

/*
Design
  Populate table: append
  Filter data: filter on map values
  Sort data: sort map values, mask
  Group data: group sorting, count, sum, mask
*/

type Unsigned interface {
	uint8 | uint16 | uint32
}

type ColumnDef struct {
	name        string // must not contain any of the following characters: & = : ,
	displayName string
	valueToKey  map[string]uint32 // maps a string to the uint32 value (key) that will be stored in the column's data
	keyToValue  map[uint32]string // maps the key to the string
	//keyToOrder  map[uint32]uint32 // maps a key to the order

	comparer Compare
	summable bool
}

// This structure contains data that is specific to a Column and a View
// e.g. grouping data.
type ColumnView struct {
	keyToGroupKey    map[uint32]uint32 // maps a key to a group - for group on functionality
	groupKeyToKey    map[uint32]uint32
	groupKeyToFilter map[uint32]string
	filterToGroupKey map[string]uint32

	groupKeyToOrder map[uint32]uint32
	// example
	//  column values: aa ab ac dd
	//  valueToKey : aa:0 ab:1 ac:2 dd:3
	//  keyToValue : 0:aa 1:ab 2:ac 3:dd
	//  groupOn: 'a' "dd"
	//  aa -> a, ab -> a, ac -> a, dd -> dd
	// filter to groupKey: a:1 dd:2
	// keyToGroupKey 0:1 1:1 2:1 3:2
	// what is the value zero for groupKey used for? Used for keys that do not match any filter
	// reads as      aa:a ab:a ac:a dd:dd
	// for default grouping, the keyToGroupKey is the identity map...?

	// groupKeyToOrder - defines the order of groups when sorting
	// e.g. groupKeyToOrder 0:0 1:1 2:2
	// means that groupKey 0 is first, groupKey 1 is second, groupKey 2 is third
}

type Column[T Unsigned] struct {
	columnDef *ColumnDef
	data      []T
}

type IColumn interface {
	ColumnDef() *ColumnDef
	//ColumnView() *ColumnView
	Append(string)
	group([]bool) map[uint32][]uint32
	group2([]uint32, *ColumnView) map[uint32][]uint32
	Length() int
	summable() bool
	get(i int) uint32
}

func NewColumn[T Unsigned](columnDef *ColumnDef) *Column[T] {
	c := Column[T]{columnDef, []T{}}
	return &c
}

func (c *Column[T]) Length() int {
	return len(c.data)
}

func (c *Column[T]) summable() bool {
	return c.columnDef.summable
}

func (c *Column[T]) get(i int) uint32 {
	return uint32(c.data[i])
}

func (c *Column[T]) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *Column[T]) Append(value string) {
	k, x := c.columnDef.valueToKey[value]
	if !x {
		key := uint32(len(c.columnDef.valueToKey))
		c.columnDef.valueToKey[value] = key
		c.columnDef.keyToValue[key] = value
		k = key
	}
	c.data = append(c.data, T(k))
}

func (c *Column[T]) append(v T) {
	c.data = append(c.data, v)
}

func (c *Column[T]) filter(a T, b T, d T) []bool {
	r := make([]bool, len(c.data))
	for i, v := range c.data {
		r[i] = v == a || v == b || v == d
	}
	return r
}

func (c *Column[T]) filter2(a T, b T, d T, mask []bool) {
	for i, v := range c.data {
		if mask[i] {
			mask[i] = v == a || v == b || v == d
		}
	}
}

func (c *Column[T]) groupOn(mask []bool) map[uint32][]uint32 {
	// pre- populate the map
	r := map[uint32][]uint32{}
	for k, _ := range c.columnDef.keyToValue {
		r[uint32(k)] = []uint32{}
	}
	// group
	for i, v := range c.data {
		if mask[i] {
			r[uint32(v)] = append(r[uint32(v)], uint32(i))
		}
	}
	// TODO remove empty entries
	return r
}

func (c *Column[T]) group(mask []bool) map[uint32][]uint32 {
	// pre- populate the map
	r := map[uint32][]uint32{}
	for k, _ := range c.columnDef.keyToValue {
		r[uint32(k)] = []uint32{}
	}
	// group
	for i, v := range c.data {
		if mask[i] {
			r[uint32(v)] = append(r[uint32(v)], uint32(i))
		}
	}
	// TODO remove empty entries
	return r
}

func (c *Column[T]) group2(indices []uint32, columnView *ColumnView) map[uint32][]uint32 {
	// populate the map: groupKey to list of valueKey
	r := map[uint32][]uint32{}
	//for k, _ := range c.columnDef.keyToValue {
	for k, _ := range columnView.groupKeyToFilter {
		r[uint32(k)] = []uint32{}
	}
	for _, i := range indices {
		valueKey := c.data[i]
		groupKey := columnView.keyToGroupKey[uint32(valueKey)]
		r[groupKey] = append(r[groupKey], uint32(i))
	}

	// Remove empty groups (filtered out)
	for k, v := range r {
		if len(v) == 0 {
			delete(r, k)
		}
	}

	// for k, _ := range c.columnDef.keyToValue {
	// 	r[uint32(k)] = []uint32{}
	// }
	// for _, i := range indices {
	// 	r[uint32(c.data[i])] = append(r[uint32(c.data[i])], uint32(i))
	// }
	return r
}

func CompareStrings(a string, b string) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	} else {
		return 1
	}
}

func CompareNumbers(a string, b string) int {
	i, err := strconv.Atoi(a)
	if err != nil {
		return 0
	}
	j, err := strconv.Atoi(b)
	//fmt.Println("compare  ", a, b, i, j)
	if err != nil {
		return 0
	}
	if i == j {
		return 0
	} else if i < j {
		return -1
	} else {
		return 1
	}
}

type sort struct {
	indices     []int
	count       int
	mask        []bool
	columns     []IColumn
	asc         map[string]bool
	columnViews map[string]*ColumnView

	//  key: value stored in data
	//  value: ordered index, comparison must be done on this.
	//keyToOrder map[string]map[uint32]uint32 // per column
}

// returned indices are the indices of the sorted values in the original data
func (s *sort) sort() {
	// fmt.Println("sort")
	// fmt.Println("len(s.mask)", len(s.mask))
	// fmt.Println("len(s.indices)", len(s.indices))

	// colDef := s.columns[0].ColumnDef()
	// columnView := s.columnViews[colDef.name]
	// fmt.Println("columnView", colDef.name)
	// fmt.Println("columnView", colDef.keyToValue)
	// fmt.Println("columnView", colDef.valueToKey)
	// fmt.Println("columnView.keyToGroupKey", columnView.keyToGroupKey)
	// fmt.Println("columnView.groupKeyToOrder", columnView.groupKeyToOrder)

	// Check if there are any columns to sort
	if len(s.columns) == 0 {
		return
	}

	// fmt.Println("s.columns[0].Length()", s.columns[0].Length())
	for i := 0; i < s.columns[0].Length(); i++ {
		if len(s.mask) > 0 && !s.mask[i] {
			continue
		}
		pos, _ := slices.BinarySearchFunc(s.indices, i, s.compare)
		if pos < s.count {
			s.indices = slices.Insert(s.indices, pos, i)
		} else {
			// nop
		}
		if len(s.indices) > s.count {
			s.indices = s.indices[:s.count]
		}
	}
	// for _, v := range s.indices {
	// 	fmt.Println("    indices", v, s.columns[0].get(v), s.columns[0].ColumnDef().keyToValue[s.columns[0].get(v)])
	// }
	// for i, v := range s.indices {
	// 	fmt.Println("    indices", v, s.columns[0].get(i))
	// }
	// fmt.Println("len(s.indices)", len(s.indices))
}

func (s *sort) compare(i int, j int) int {
	//fmt.Println("compare", len(s.order))
	for _, col := range s.columns {
		//mapping := s.keyToOrder[col.ColumnDef().name]
		// i and j are indices into the columns
		// s.columns[col].get(i) is the index into the indexToValue
		// mapping maps the index to the actual sorted index
		columnView := s.columnViews[col.ColumnDef().name]
		a_groupKey := columnView.keyToGroupKey[col.get(i)]
		b_groupKey := columnView.keyToGroupKey[col.get(j)]
		//fmt.Println(col.ColumnDef().name, a_groupKey, col.get(i), columnView.keyToGroupKey)
		// if _, ok := columnView.groupKeyToOrder[a_groupKey]; !ok {
		// 	fmt.Println("a_groupKey not found", a_groupKey, col.get(i), columnView.keyToGroupKey)
		// }
		a := columnView.groupKeyToOrder[a_groupKey]
		b := columnView.groupKeyToOrder[b_groupKey]
		if !s.asc[col.ColumnDef().name] {
			if a == b {
				continue
			} else if a < b {
				//fmt.Println("a < b", a, b, col.get(i), col.get(j), i, j)
				return 1
			} else {
				//fmt.Println("a > b", a, b, col.get(i), col.get(j), i, j)
				return -1
			}
		}
		//fmt.Println(col.ColumnDef().name, i, j, mapping, a, b)
		// comparison is based on sorted entries
		// b, a, c -> 1, 2, 3
		// 1 => 2
		// 2 => 1
		// c => 3
		if a == b {
			continue
		} else if a < b {
			//fmt.Println("a > b", a, b, col.get(i), col.get(j), i, j)
			return -1
		} else {
			//fmt.Println("a < b", a, b, col.get(i), col.get(j), i, j)
			return 1
		}
	}
	return 0
}

type Compare func(string, string) int

func testSortSingleColumn(limit int, input []uint32, output []int) {
	columnDef := ColumnDef{
		name:        "A",
		displayName: "A",
		valueToKey:  map[string]uint32{"a": 0, "b": 1, "c": 2, "d": 3},
		keyToValue:  map[uint32]string{0: "a", 1: "b", 2: "c", 3: "d"},
	}
	column := Column[uint32]{
		columnDef: &columnDef,
		//data:      []uint32{0, 1, 2, 3},
		//data:      []uint32{3, 2, 1, 0},
		//data:      []uint32{3, 1, 0, 2},
		//data: []uint32{3, 1, 0, 2},
		data: input,
	}
	columnView := ColumnView{
		keyToGroupKey: map[uint32]uint32{0: 1, 1: 2, 2: 3, 3: 4},
		groupKeyToKey: map[uint32]uint32{1: 0, 2: 1, 3: 2, 4: 3},

		groupKeyToOrder: map[uint32]uint32{0: 0, 1: 1, 2: 2, 3: 3, 4: 4},
	}
	// v := View{
	// 	grouping:     []string{},
	// 	sorting:      map[string]bool{"A": true},
	// 	groupOn:      map[string][]string{},
	// 	groupSortPos: map[string]int{},
	// 	filtering:    map[string]string{},
	// 	columnViews:  map[string]*ColumnView{"A": &columnView},
	// }
	s := sort{
		columnViews: map[string]*ColumnView{"A": &columnView},
		columns:     []IColumn{&column},
		count:       limit,
		mask:        []bool{},
		asc:         map[string]bool{"A": true},
	}
	s.sort()
	if !reflect.DeepEqual(s.indices[:limit], output[:limit]) {
		fmt.Println("s.indices", s.indices)
		fmt.Println("output", output)
		panic("sort error")
	}
}

func TestSort() {
	// values will be a b c d
	// keys will be 0 1 2 3
	// groupKeys will be 1 2 3 4

	testSortSingleColumn(4, []uint32{0, 1, 2, 3}, []int{0, 1, 2, 3})
	testSortSingleColumn(4, []uint32{3, 2, 1, 0}, []int{3, 2, 1, 0})
	testSortSingleColumn(4, []uint32{3, 1, 0, 2}, []int{2, 1, 3, 0})
	testSortSingleColumn(2, []uint32{3, 1, 0, 2}, []int{2, 1, 3, 0})
	fmt.Println()
	fmt.Println()

}

func TestGroup2() {
	// Create a column with values: "a", "b", "a", "c", "b"
	// At indices: 0, 1, 2, 3, 4
	// Keys stored: 0, 1, 0, 2, 1
	columnDef := ColumnDef{
		name:        "TestCol",
		displayName: "Test Column",
		valueToKey:  map[string]uint32{"a": 0, "b": 1, "c": 2},
		keyToValue:  map[uint32]string{0: "a", 1: "b", 2: "c"},
	}

	column := Column[uint32]{
		columnDef: &columnDef,
		data:      []uint32{0, 1, 0, 2, 1}, // a, b, a, c, b
	}

	// Group "a" and "b" together as group 1, "c" as group 2
	columnView := ColumnView{
		keyToGroupKey:    map[uint32]uint32{0: 1, 1: 1, 2: 2}, // a->1, b->1, c->2
		groupKeyToFilter: map[uint32]string{1: "ab", 2: "c"},
	}

	// Call group2 with all indices
	indices := []uint32{0, 1, 2, 3, 4}
	result := column.group2(indices, &columnView)

	// Expected: group 1 should contain indices [0, 1, 2, 4]
	//           group 2 should contain indices [3]
	// Current buggy behavior: returns valueKeys instead of indices

	fmt.Println("TestGroup2 Results:")
	fmt.Println("Group 1 (should be indices [0,1,2,4]):", result[1])
	fmt.Println("Group 2 (should be indices [3]):", result[2])

	// Check if we got indices or valueKeys
	expectedGroup1 := []uint32{0, 1, 2, 4}
	expectedGroup2 := []uint32{3}

	if !reflect.DeepEqual(result[1], expectedGroup1) {
		fmt.Printf("ERROR: Group 1 expected %v, got %v\n", expectedGroup1, result[1])
		fmt.Println("BUG: group2 is returning valueKeys instead of indices!")
		panic("group2 test failed")
	}

	if !reflect.DeepEqual(result[2], expectedGroup2) {
		fmt.Printf("ERROR: Group 2 expected %v, got %v\n", expectedGroup2, result[2])
		panic("group2 test failed")
	}

	fmt.Println("TestGroup2 PASSED")
}
