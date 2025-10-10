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
	"strconv"
)

/*
Design
  Populate table: append
  Filter data: filter on map values
  Sort data: sort map values, mask
  Group data: group sorting, count, sum, mask
*/

type Group struct {
	columnDef  *ColumnDef
	ColumnView *ColumnView
	level      int
	value      uint32
	counts     []int
	groups     map[uint32]*Group

	// leafs
	indices map[uint32][]uint32
	// for every column we need a list of aggregates, one per grouping level
	aggregates     map[string]int
	leafaggregates map[string]map[string]int
	//keyToOrder map[string]map[uint32]uint32
	asc bool
}

//	func (g *Group) Dump() {
//		fmt.Println()
//		fmt.Println()
//		fmt.Println()
//		g.dump("")
//		fmt.Println()
//		fmt.Println()
//	}
func (g *Group) dump(indent string) {
	if len(g.indices) != 0 {
		for value, indices := range g.indices {
			fmt.Println(indent, "leaf", g.columnDef.name, "value:", value, len(indices), g.counts)
		}
	} else {
		for value, group := range g.groups {
			fmt.Println(indent, "tree", g.columnDef.name, "group.value", group.value, "value:", value, group.counts)
			group.dump(indent + "   ")
		}
	}
}

func MakeGroup(t *Table, columnViews map[string]*ColumnView, mask []bool, columns []string, aggregatedColumns []string, compare map[string]Compare, asc map[string]bool) *Group {
	indices := []uint32{}
	for _, c := range columns {
		for i := 0; i < t.columns[c].Length(); i++ {
			if len(mask) == 0 || mask[i] {
				indices = append(indices, uint32(i))
			}
		}
		break
	}
	g := group(t, columnViews, indices, columns, aggregatedColumns, asc, 0, 0)
	return g
}

func group(t *Table, columnViews map[string]*ColumnView, indices []uint32, columns []string, aggregatedColumns []string, asc map[string]bool, level int, value uint32) *Group {
	column := t.columns[columns[0]]
	if len(columns) == 1 {
		// last grouping
		g := &Group{
			columnDef:  column.ColumnDef(),
			ColumnView: columnViews[column.ColumnDef().name],
			level:      level + 1,
			//value:     int64(value),
			indices: column.group2(indices, columnViews[column.ColumnDef().name]),
			//keyToOrder: column.ColumnDef().keyToOrder,
			asc:            asc[column.ColumnDef().name],
			aggregates:     map[string]int{},
			groups:         map[uint32]*Group{},
			leafaggregates: map[string]map[string]int{},
		}
		// for i, ii := range g.indices {
		// 	fmt.Println("GROUP", i, len(ii))
		// }
		g.counts = []int{len(g.indices), len(indices)}

		for vv, ii := range g.indices {
			aggregates := map[string]int{}
			for _, c := range aggregatedColumns {
				col := t.columns[c]
				if col.summable() {
					sum := 0
					for _, i := range ii {
						val, err := strconv.Atoi(col.ColumnDef().keyToValue[col.get(int(i))])
						if err == nil {
							sum += val
						}
					}
					aggregates[c] = sum
					g.aggregates[c] += sum
				}
			}

			g.groups[vv] = &Group{
				aggregates: aggregates,
				columnDef:  g.columnDef,
				ColumnView: g.ColumnView,
				counts:     append(g.counts, len(ii)), // I think the goal is to have a new slice here...
			}

		}
		// go through non grouped column and calculate aggregates for summable columns
		return g
	} else {
		//fmt.Println(strings.Repeat("  ", level), "Group "+column.ColumnDef().name)
		g := &Group{
			columnDef:  column.ColumnDef(),
			ColumnView: columnViews[column.ColumnDef().name],
			level:      level + 1,
			value:      value,
			groups:     map[uint32]*Group{},
			counts:     make([]int, len(columns)+1),
			aggregates: map[string]int{},
			//keyToOrder: keyToOrder,
		}
		groups := column.group2(indices, columnViews[column.ColumnDef().name])
		g.counts[0] = len(groups)
		for value, indices := range groups {
			gg := group(t, columnViews, indices, columns[1:], aggregatedColumns, asc, level+1, value)
			gg.value = value
			g.groups[value] = gg
			for i, v := range gg.counts {
				g.counts[i+1] += v
			}
		}
		for _, c := range aggregatedColumns {
			if !t.columns[c].summable() {
				continue
			}
			for _, gg := range g.groups {
				g.aggregates[c] += gg.aggregates[c]
			}

		}
		return g
	}
}

func (g *Group) Counts() []uint32 {
	if g.groups == nil {
		return []uint32{uint32(len(g.indices))}
	} else {
		// sum the counts
		counts := []uint32{}
		for _, v := range g.groups {
			if len(counts) == 0 {
				counts = v.Counts()
			} else {
				for i, v := range v.Counts() {
					counts[i] += v
				}
			}
		}
		return append([]uint32{uint32(len(g.groups))}, counts...)
	}
}
