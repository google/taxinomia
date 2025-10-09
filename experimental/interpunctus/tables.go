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
	"slices"
	"time"
)

/*
Design
  Populate table: append
  Filter data: filter on map values
  Sort data: sort map values, mask
  Group data: group sorting, count, sum, mask
*/

type Table struct {
	columns map[string]IColumn
}

// Entry point where a view is applied to a table
func (t *Table) Apply(v *View) ([]int, *Group, error) {
	// Reorder columns based on current filtering/grouping state (in-place)
	reordered := v.reorderColums()
	v.order = reordered.order

	// mask will be used to filter rows (true = include, false = exclude)
	var mask []bool
	// Get row count from first column
	rowCount := 0
	for _, col := range t.columns {
		rowCount = col.Length()
		break
	}

	for _, col := range v.order {
		v.columnViews[col] = &ColumnView{
			keyToGroupKey:    map[uint32]uint32{},
			groupKeyToKey:    map[uint32]uint32{},
			groupKeyToFilter: map[uint32]string{},
			filterToGroupKey: map[string]uint32{},
			groupKeyToOrder:  map[uint32]uint32{},
		}
		groupKey := 1 // start at 1, 0 is reserved for the default grouping
		for value, key := range t.columns[col].ColumnDef().valueToKey {
			v.columnViews[col].filterToGroupKey[value] = uint32(groupKey)
			v.columnViews[col].groupKeyToFilter[uint32(groupKey)] = value
			v.columnViews[col].keyToGroupKey[key] = uint32(groupKey)
			v.columnViews[col].groupKeyToKey[uint32(groupKey)] = uint32(key)

			groupKey += 1
		}
		//fmt.Println("GROUP:", col, v.columnViews[col].keyToGroupKey)
	}

	if len(v.groupOn) > 0 {
		groupingCols := v.grouping
		for col, filters := range v.groupOn {
			// Check if this column is in the grouping list (grouping) or just filtering
			isGrouping := slices.Contains(groupingCols, col)

			v.columnViews[col] = &ColumnView{
				keyToGroupKey:    map[uint32]uint32{},
				groupKeyToKey:    map[uint32]uint32{},
				groupKeyToFilter: map[uint32]string{},
				filterToGroupKey: map[string]uint32{},
				groupKeyToOrder:  map[uint32]uint32{},
			}

			if len(filters) > 0 {
				if isGrouping {
					// GROUPING: Create separate groups for each filter value
					groupKey := 1 // start at 1, 0 is reserved for filtered-out values
					for _, filter := range filters {
						v.columnViews[col].filterToGroupKey[filter] = uint32(groupKey)
						v.columnViews[col].groupKeyToFilter[uint32(groupKey)] = filter
						groupKey += 1
					}
					for key, value := range t.columns[col].ColumnDef().keyToValue {
						// Test value against filter using Match() function
						match := false
						for i, filter := range filters {
							if Match(filter, value) {
								match = true
								v.columnViews[col].keyToGroupKey[key] = uint32(i + 1)
								v.columnViews[col].groupKeyToKey[uint32(i+1)] = uint32(key)
								break
							}
						}
						if !match {
							v.columnViews[col].keyToGroupKey[key] = 0 // filtered out
						}
					}
				} else {
					// FILTERING: Build a mask to filter out non-matching rows
					// Initialize mask if not already done
					if len(mask) == 0 {
						mask = make([]bool, rowCount)
						// Start with all rows included
						for i := range mask {
							mask[i] = true
						}
					}

					// Mark non-matching rows as false in the mask
					column := t.columns[col]
					for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
						key := column.get(rowIdx)
						value := column.ColumnDef().keyToValue[key]

						match := false
						for _, filter := range filters {
							if Match(filter, value) {
								match = true
								break
							}
						}
						if !match {
							mask[rowIdx] = false // filter out this row
						}
					}
				}
			} else {
				// Empty filters = default grouping (auto-group all distinct values)
				// Only applies if this column is in grouping list
				if isGrouping {
					groupKey := 1 // start at 1, 0 is reserved for filtered-out values
					for value, key := range t.columns[col].ColumnDef().valueToKey {
						v.columnViews[col].filterToGroupKey[value] = uint32(groupKey)
						v.columnViews[col].groupKeyToFilter[uint32(groupKey)] = value
						v.columnViews[col].keyToGroupKey[key] = uint32(groupKey)
						v.columnViews[col].groupKeyToKey[uint32(groupKey)] = uint32(key)
						groupKey += 1
					}
				}
			}
		}
	}
	groupingCols := v.grouping
	if len(groupingCols) == 0 {
		// No grouping - just sort (may have filters in groupOn for single-value filtering)
		sorted := t.Sort(v.columnViews, v.order, 50, v.sorting, mask)
		return sorted, nil, nil
	} else {
		//if len(groupingCols) > 0 {
		g := t.Group(v.columnViews, groupingCols, v.AggregatedColumns(), mask, v.sorting)

		return nil, g, nil
		// } else {
		// 	// group on
		// 	// cache  mappings??? should be cheap anyway...
		// 	grouping := []string{}
		// 	for col, _ := range v.groupOn {
		// 		grouping = append(grouping, col)
		// 	}
		// 	g := t.Group(grouping, v.AggregatedColumns(), indices, v.sorting)
		// 	return nil, g, nil
		// }
	}
}

func (t *Table) Sort(columnViews map[string]*ColumnView, order []string, limit int, asc map[string]bool, mask []bool) []int {
	//populate the key to order maps
	for _, col := range t.columns {
		values := []string{}
		for k, _ := range col.ColumnDef().valueToKey {
			values = append(values, k) // values is not ordered...
		}
		slices.SortFunc(values, col.ColumnDef().comparer) //   -> a,b,c, basically a map from value to index
		for i, v := range values {
			key := col.ColumnDef().valueToKey[v]
			//fmt.Println(col.ColumnDef().name, t.columnViews[col.ColumnDef().name])
			columnView := columnViews[col.ColumnDef().name]
			if columnView != nil {
				columnView.groupKeyToOrder[columnView.keyToGroupKey[key]] = uint32(i)
			}
		}
	}

	sort := sort{
		columns: []IColumn{},
		count:   limit,
		asc:     asc,
		mask:    mask,

		columnViews: columnViews,
	}

	// first sort all the possible values
	for _, name := range order {
		sort.columns = append(sort.columns, t.columns[name])
		//fmt.Println("sort.columns", name)
	}
	// fmt.Println("sort.count", sort.count)
	fmt.Println("sort.asc", sort.asc)

	start := time.Now()
	sort.sort()
	// fmt.Println("sort.indices", sort.indices)
	fmt.Println(time.Now().Sub(start))
	// for i, v := range sort.indices {
	// 	fmt.Println("sort.indices", i, v)
	// }
	return sort.indices
}

// group sorting
//   value  (grouped col)
//   subgroup count (count(next))
//   ...  (count(next.next...))

func (t *Table) Group(columnViews map[string]*ColumnView, order []string, aggregatedColumns []string, mask []bool, asc map[string]bool) *Group {
	//populate the key to order maps
	for _, col := range t.columns {
		values := []string{}
		// for k, _ := range col.ColumnDef().valueToKey {
		// 	values = append(values, k) // values is not ordered...
		// }
		columnView := columnViews[col.ColumnDef().name]
		if columnView == nil {
			continue
		}
		for _, f := range columnView.groupKeyToFilter {
			values = append(values, f) // values is not ordered...
		}
		slices.SortFunc(values, col.ColumnDef().comparer) //   -> a,b,c, basically a map from value to index
		//fmt.Println("values", values)
		for i, v := range values {
			//key := col.ColumnDef().valueToKey[v]
			//col.ColumnDef().keyToOrder[key] = uint32(i)
			groupKey := columnView.filterToGroupKey[v]
			columnView.groupKeyToOrder[groupKey] = uint32(i)
		}
	}

	comparers := map[string]Compare{}
	for _, c := range order {
		comparers[t.columns[c].ColumnDef().name] = t.columns[c].ColumnDef().comparer
	}

	G := MakeGroup(t, columnViews, mask, order, aggregatedColumns, comparers, asc)
	// fmt.Println(G)
	// G.Dump()
	// fmt.Println()
	// fmt.Println()
	return G
}
