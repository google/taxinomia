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

// grouping, might or might not leverage the base implementation of columns
// filtering layer
// base column data classed, optimized for storage and grouping efficiency
//  for high cardinality data, use direct storage, key is the row index
//  for low cardinality data, use key-based storage, key is the index into the value table

// grouping layer
// every group maps to a list of indices into the base column data
//   to filter, we go iterate through all the rows for simple columns, for keyed ones we only need to go through the keys. The values matched the  filter the rows.
// For interned string, it is very similar

// for grouping, it is very similar...

// need to build some abstraction to cover these two cases...
// pass the filter to the colu, get back the indices that match

package columns

import (
	"fmt"
)

type Unsigned interface {
	uint8 | uint16 | uint32
}

type IColumnDef interface {
	Name() string // must not contain any of the following characters: & = : ,
	DisplayName() string
}

type ColumnDef struct {
	name        string // must not contain any of the following characters: & = : ,
	displayName string
}

// NewColumnDef creates a new ColumnDef with the given name and display name
func NewColumnDef(name, displayName string) *ColumnDef {
	return &ColumnDef{
		name:        name,
		displayName: displayName,
	}
}

func (cd *ColumnDef) Name() string {
	return cd.name
}

func (cd *ColumnDef) DisplayName() string {
	return cd.displayName
}

type IDataColumn interface {
	ColumnDef() *ColumnDef
}

type DataColumn[T any] struct {
	IDataColumn
	columnDef *ColumnDef
	data      []T
}

type Filterable[T any] interface {
	Filter(predicate func(T) bool) *[]bool
}

func (c DataColumn[T]) Filter(predicate func(T) bool) []int {
	switch any(c).(type) {
	case DataColumn[string]:
		col := any(c).(DataColumn[string])
		return col.Filter(any(predicate).(func(string) bool))
	case DataColumn[uint32]:
		col := any(c).(DataColumn[uint32])
		return col.Filter(any(predicate).(func(uint32) bool))
	}
	return []int{}
}

//type StringDataColumn DataColumn[string]

//type PlainDataColumn[T any] struct {
// 	data []T
// }

// func (c PlainDataColumn[T]) Filter(predicate func(T) bool) *[]uint32 {
// 	indices := []uint32{}
// 	for i, v := range c.data {
// 		if predicate(v) {
// 			indices = append(indices, uint32(i))
// 		}
// 	}
// 	return &indices
// }

// func (c StringDataColumn) Filter(predicate func(string) bool) []uint32 {
// 	indices := []uint32{}
// 	for i, v := range c.data {
// 		if predicate(v) {
// 			indices = append(indices, uint32(i))
// 		}
// 	}
// 	return indices
// }

//type Uint32DataColumn DataColumn[uint32]

// func (c Uint32DataColumn) Filter(predicate func(uint32) bool) []int {
// 	return []int{}
// }

// IColumn is the general interface for all column types
type IColumn[T any] interface {
	ColumnDef() *ColumnDef
	Length() int
	// summable() bool
	// Filter(predicate func(T) bool) *[]uint32
}

func NewColumn[T Unsigned](columnDef *ColumnDef) *DataColumn[T] {
	c := DataColumn[T]{
		columnDef: columnDef,
		data:      []T{},
	}
	return &c
}

func (c *DataColumn[T]) Length() int {
	return len(c.data)
}

func (c *DataColumn[T]) GetKey(i int) (uint32, error) {
	if i < 0 || i >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range [0:%d)", i, len(c.data))
	}
	// For DataColumn[uint32], T is already uint32
	var zero T
	switch any(zero).(type) {
	case uint32:
		return any(c.data[i]).(uint32), nil
	case uint16:
		return uint32(any(c.data[i]).(uint16)), nil
	case uint8:
		return uint32(any(c.data[i]).(uint8)), nil
	default:
		return 0, fmt.Errorf("unsupported type for GetKey")
	}
}

func (c *DataColumn[T]) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *DataColumn[T]) append(v T) {
	c.data = append(c.data, v)
}

// func (c *DataColumn[T]) filter(a T, b T, d T) []bool {
// 	r := make([]bool, len(c.data))
// 	// Only works for comparable types
// 	var zero T
// 	switch any(zero).(type) {
// 	case uint32, uint16, uint8:
// 		for i, v := range c.data {
// 			r[i] = any(v) == any(a) || any(v) == any(b) || any(v) == any(d)
// 		}
// 	}
// 	return r
// }

// func (c *DataColumn[T]) filter2(a T, b T, d T, mask []bool) {
// 	// Only works for comparable types
// 	var zero T
// 	switch any(zero).(type) {
// 	case uint32, uint16, uint8:
// 		for i, v := range c.data {
// 			if mask[i] {
// 				mask[i] = any(v) == any(a) || any(v) == any(b) || any(v) == any(d)
// 			}
// 		}
// 	}
// }

// func (c *DataColumn[T]) groupOn(mask []bool) map[uint32][]uint32 {
// 	// pre- populate the map
// 	r := map[uint32][]uint32{}
// 	for k, _ := range c.columnDef.keyToValue {
// 		r[uint32(k)] = []uint32{}
// 	}
// 	// group
// 	for i, v := range c.data {
// 		if mask[i] {
// 			// Convert v to uint32 for map key
// 			var key uint32
// 			switch any(v).(type) {
// 			case uint32:
// 				key = any(v).(uint32)
// 			case uint16:
// 				key = uint32(any(v).(uint16))
// 			case uint8:
// 				key = uint32(any(v).(uint8))
// 			}
// 			r[key] = append(r[key], uint32(i))
// 		}
// 	}
// 	// TODO remove empty entries
// 	return r
// }

// func (c *DataColumn[T]) group(mask []bool) map[uint32][]uint32 {
// 	// pre- populate the map
// 	r := map[uint32][]uint32{}
// 	for k, _ := range c.columnDef.keyToValue {
// 		r[uint32(k)] = []uint32{}
// 	}
// 	// group
// 	for i, v := range c.data {
// 		if mask[i] {
// 			// Convert v to uint32 for map key
// 			var key uint32
// 			switch any(v).(type) {
// 			case uint32:
// 				key = any(v).(uint32)
// 			case uint16:
// 				key = uint32(any(v).(uint16))
// 			case uint8:
// 				key = uint32(any(v).(uint8))
// 			}
// 			r[key] = append(r[key], uint32(i))
// 		}
// 	}
// 	// TODO remove empty entries
// 	return r
// }

// func (c *DataColumn[T]) group2(indices []uint32, columnView *ColumnView) map[uint32][]uint32 {
// 	// populate the map: groupKey to list of valueKey
// 	r := map[uint32][]uint32{}
// 	//for k, _ := range c.columnDef.keyToValue {
// 	for k, _ := range columnView.groupKeyToFilter {
// 		r[uint32(k)] = []uint32{}
// 	}
// 	for _, i := range indices {
// 		v := c.data[i]
// 		// Convert v to uint32 for map lookup
// 		var valueKey uint32
// 		switch any(v).(type) {
// 		case uint32:
// 			valueKey = any(v).(uint32)
// 		case uint16:
// 			valueKey = uint32(any(v).(uint16))
// 		case uint8:
// 			valueKey = uint32(any(v).(uint8))
// 		}
// 		groupKey := columnView.keyToGroupKey[valueKey]
// 		r[groupKey] = append(r[groupKey], uint32(i))
// 	}

// 	// Remove empty groups (filtered out)
// 	for k, v := range r {
// 		if len(v) == 0 {
// 			delete(r, k)
// 		}
// 	}

// 	// for k, _ := range c.columnDef.keyToValue {
// 	// 	r[uint32(k)] = []uint32{}
// 	// }
// 	// for _, i := range indices {
// 	// 	r[uint32(c.data[i])] = append(r[uint32(c.data[i])], uint32(i))
// 	// }
// 	return r
// }

// func CompareStrings(a string, b string) int {
// 	if a == b {
// 		return 0
// 	} else if a < b {
// 		return -1
// 	} else {
// 		return 1
// 	}
// }

// func CompareNumbers(a string, b string) int {
// 	i, err := strconv.Atoi(a)
// 	if err != nil {
// 		return 0
// 	}
// 	j, err := strconv.Atoi(b)
// 	//fmt.Println("compare  ", a, b, i, j)
// 	if err != nil {
// 		return 0
// 	}
// 	if i == j {
// 		return 0
// 	} else if i < j {
// 		return -1
// 	} else {
// 		return 1
// 	}
// }
