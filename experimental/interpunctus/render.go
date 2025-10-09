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
	_ "embed"
	"fmt"
	"slices"
	"strings"
)

//go:embed resources/tables/style.css
var tableCSS string

//go:embed resources/tables/script.js
var tableJS string

type Renderer struct {
	sb *strings.Builder

	view  *View
	table *Table

	group   *Group
	indices []int

	rows []*row
}

func Render(t *Table, v *View, g *Group, indices []int) *strings.Builder {
	r := &Renderer{sb: &strings.Builder{}, view: v, table: t, group: g, indices: indices}

	r.renderScripts()

	r.sb.WriteString("<table>\n")
	r.renderTableHeaders()

	if len(r.view.grouping) == 0 {
		r.renderNotGrouped()
	} else {
		cb := CellBuilder()
		sums := map[string][]int{}
		for _, c := range v.AggregatedColumns() {
			sums[c] = []int{g.sums[c]}
		}
		cb.cells(g, v.columnViews, v.AggregatedColumns(), sums)
		r.rows = cb.rows
		r.renderGrouped()
	}

	r.sb.WriteString("</table>\n")
	return r.sb
}

//  data + view + processed data(Group or indices)
//  render group or just columns

func (r *Renderer) renderScripts() {
	r.sb.WriteString("<style>")
	r.sb.WriteString(tableCSS)
	r.sb.WriteString("</style>")

	r.sb.WriteString("<script>")
	r.sb.WriteString(tableJS)
	r.sb.WriteString("</script>")
}

func (r *Renderer) renderTableHeaders() {
	r.sb.WriteString("<tr>")
	for i, c := range r.view.order {
		distinctCount := len(r.table.columns[c].ColumnDef().valueToKey)
		// Get visible row count from group or indices
		var visibleRows int
		var groupCount int
		if r.group != nil && len(r.group.counts) > 0 {
			visibleRows = r.group.counts[len(r.group.counts)-1]
			// Group count is at position i in the counts array
			if i < len(r.group.counts) {
				groupCount = r.group.counts[i]
			}
		} else {
			visibleRows = len(r.indices)
			groupCount = 0
		}
		fmt.Fprintf(r.sb, "<th>")
		if groupCount > 0 {
			fmt.Fprintf(r.sb, "%s (%d/%d/%d)", r.table.columns[c].ColumnDef().displayName, groupCount, distinctCount, visibleRows)
		} else {
			fmt.Fprintf(r.sb, "%s (%d/%d)", r.table.columns[c].ColumnDef().displayName, distinctCount, visibleRows)
		}
		fmt.Fprintf(r.sb, "</th>")
	}
	r.sb.WriteString("</tr>")

	// move commands
	r.sb.WriteString("<tr>")
	for _, col := range r.view.order {
		w, _ := v.MoveLeftMost(col)
		query := w.ToQuery()
		fmt.Fprintf(r.sb, `<th class="cmd">`)
		fmt.Fprintf(r.sb, `<a href="sorted?%s"> << </a>`, query)
		fmt.Fprintf(r.sb, `&nbsp;`)
		fmt.Fprintf(r.sb, `&nbsp;`)
		w, _ = v.MoveLeft(col)
		query = w.ToQuery()
		fmt.Fprintf(r.sb, `<a href="sorted?%s"> < </a>`, query)
		fmt.Fprintf(r.sb, `&nbsp;`)
		fmt.Fprintf(r.sb, `&nbsp;`)
		w, _ = v.MoveRight(col)
		query = w.ToQuery()
		fmt.Fprintf(r.sb, `<a href="sorted?%s"> > </a>`, query)
		fmt.Fprintf(r.sb, `</th >`)
	}

	// sorting commands
	r.sb.WriteString("<tr>")
	for _, col := range r.view.order {
		// index := 0
		// for i, c := range v.sortPriority {
		// 	if c == col {
		// 		index = i
		// 		break
		// 	}
		// }
		w, _ := v.ToggleSortDirection(col)
		query := w.ToQuery()
		fmt.Fprintf(r.sb, `<th class="cmd">`)
		fmt.Fprintf(r.sb, `<a href="sorted?%s">&udarr;</a>`, query)
		// &updownarrow; is too narrow...
		// &udarr;
		//&vArr;
		//&#x21f3;
		// https://www.toptal.com/designers/htmlarrows/arrows/

		fmt.Fprintf(r.sb, `&nbsp;`)

		w, _ = v.LeftShiftSortLevel(col)
		query = w.ToQuery()
		fmt.Fprintf(r.sb, `<a href="sorted?%s">&lArr;</a>`, query)
		fmt.Fprintf(r.sb, `&nbsp;`)

		w, _ = v.RightShiftSortLevel(col)
		query = w.ToQuery()
		fmt.Fprintf(r.sb, `<a href="sorted?%s">&rArr;</a>`, query)
		fmt.Fprintf(r.sb, `&nbsp;`)

		// w, _ = v.OrderFirst(col)
		// query = w.ToQuery()
		// fmt.Fprintf(r.sb, `<a href="sorted?%s">%d</a>`, query, index)
		fmt.Fprintf(r.sb, `</th>`)
	}
	r.sb.WriteString("</tr>")

	// grouping commands
	r.sb.WriteString("<tr>")
	for _, col := range r.view.order {
		index := -1
		for i, c := range v.grouping {
			if c == col {
				index = i
				break
			}
		}
		w, _ := v.ToggleGrouping(col)
		query := w.ToQuery()
		fmt.Fprintf(r.sb, `<th class="cmd">`)
		if index == -1 {
			fmt.Fprintf(r.sb, `<a href="grouped ?%s">G</a>`, query)
		} else {
			fmt.Fprintf(r.sb, `<a href="sorted?%s">U</a>`, query)
		}

		fmt.Fprintf(r.sb, `&nbsp;`)

		fmt.Fprintf(r.sb, `</th>`)
	}
	r.sb.WriteString("</tr>")

	// group-on input row
	r.sb.WriteString("<tr>")
	for _, col := range r.view.order {
		currentGroupOn := ""
		if filters, exists := r.view.groupOn[col]; exists && len(filters) > 0 {
			currentGroupOn = strings.Join(filters, "||")
		}
		fmt.Fprintf(r.sb, `<th class="cmd">`)
		// HTML escape the value to ensure quotes and special chars display correctly
		escapedValue := strings.ReplaceAll(currentGroupOn, `"`, `&quot;`)
		fmt.Fprintf(r.sb, `<input type="text" class="groupon-input" data-column="%s" value="%s" placeholder="filter or ||group" size="12" />`, col, escapedValue)
		fmt.Fprintf(r.sb, `</th>`)
	}
	r.sb.WriteString("</tr>")
}

func (r *Renderer) renderNotGrouped() {
	for _, i := range r.indices {
		r.sb.WriteString("<tr>")
		for _, col := range v.order {
			fmt.Fprintf(r.sb, "<td>")
			r.sb.WriteString(r.table.columns[col].ColumnDef().keyToValue[r.table.columns[col].get(int(i))])
			r.sb.WriteString("</td>")
		}
		r.sb.WriteString(" </tr>\n")
	}
}

func (r *Renderer) renderGrouped() {
	for _, row := range r.rows {
		r.sb.WriteString("<tr>")
		// grouped first and aggregated next, build it...
		// for sorting purposes all the numbers must be first calculated, then sorted and finally displayed
		for _, c := range r.view.order {
			cell, x := row.cells[c]
			//fmt.Println("  ", c, "  ", cell)
			if !x {
				continue
			}
			if cell.span == 0 || cell.span == 1 {
				fmt.Fprintf(r.sb, "<td>")
			} else {
				fmt.Fprintf(r.sb, "<td rowspan=\"%d\">", cell.span)
			}

			sums := ""
			sep := ""
			for _, c := range cell.sums {
				sums += sep + fmt.Sprintf("%d", c)
				sep = "/"
			}
			if sums != "" {
				sums = "  [" + sums + "]"
			}
			r.sb.WriteString(cell.value + sums)

			counts := ""
			sep = ""
			for _, c := range cell.counts {
				counts += sep + fmt.Sprintf("%d", c)
				sep = "/"
			}
			if counts != "" {
				counts = "  (" + counts + ")"
			}

			r.sb.WriteString(counts)
			r.sb.WriteString("</td>        ")
		}
		r.sb.WriteString(" </tr>\n")
	}
}

type row struct {
	cells map[string]cell
}

type cell struct {
	colName string
	span    int
	value   string
	counts  []int
	sums    []int
}

func compareGroups(a *sortGroup, b *sortGroup) int {
	c := a.columnView.groupKeyToOrder[a.key]
	d := b.columnView.groupKeyToOrder[b.key]
	//fmt.Println("compare", a.columnDef.name, a.columnView.groupKeyToOrder, a.key, b.key)
	if c == d {
		return 0
	} else if c < d {
		return -1
	}
	return 1
}

func compareIndices(a *indices, b *indices) int {
	//fmt.Println("a.columnView", a.columnView)
	c := a.columnView.groupKeyToOrder[a.columnView.keyToGroupKey[a.value]]
	d := b.columnView.groupKeyToOrder[b.columnView.keyToGroupKey[b.value]]
	if c == d {
		return 0
	} else if c < d {
		return -1
	}
	return 1
}

type indices struct {
	columnDef  *ColumnDef
	columnView *ColumnView
	value      uint32
	indices    []uint32
	asc        bool
}

type sortGroup struct {
	columnDef  *ColumnDef
	columnView *ColumnView
	key        uint32
	asc        bool
}

type cellBuilder struct {
	rows []*row
	r    *row
}

func CellBuilder() *cellBuilder {
	cb := &cellBuilder{rows: []*row{}, r: &row{}}
	cb.r = &row{map[string]cell{}}
	return cb
}

func (cb *cellBuilder) cells(g *Group, columnViews map[string]*ColumnView, aggregatedColumns []string, sums map[string][]int) {
	if len(g.indices) != 0 {
		// leaf
		groups := []*indices{}
		for k, v := range g.indices {
			groups = append(groups, &indices{g.columnDef, columnViews[g.columnDef.name], k, v, g.asc})
		}
		slices.SortFunc(groups, compareIndices)

		for _, group := range groups {
			var leaf *Group
			for _, gg := range g.groups {
				if gg.columnDef.name == group.columnDef.name {
					leaf = gg
				}
			}
			// fmt.Println("@@@@", g.columnDef.name, group.value, len(group.indices))
			// fmt.Println("@@@@", g.columnDef.name, columnViews[g.columnDef.name].groupKeyToKey)
			// fmt.Println("@@@@", g.columnDef.name, g.columnDef.keyToValue)
			displayValue := "..."
			if group.value != 0 {
				key := columnViews[g.columnDef.name].groupKeyToKey[group.value]
				displayValue = g.columnDef.keyToValue[key]
			}
			c := cell{
				colName: g.columnDef.name,
				span:    1,
				value:   displayValue,
				counts:  []int{len(group.indices)},
			}
			cb.r.cells[g.columnDef.name] = c
			for _, a := range aggregatedColumns {
				if _, e := g.sums[a]; e {
					c := cell{
						colName: a,
						span:    1,
						value:   fmt.Sprintf("%d", leaf.sums[a]),
						sums:    append(sums[a], leaf.sums[a]),
					}
					cb.r.cells[a] = c
				} else {
					c := cell{
						colName: a,
						span:    1,
						value:   "",
						counts:  []int{len(group.indices)},
					}
					cb.r.cells[a] = c
				}
			}

			cb.rows = append(cb.rows, cb.r)
			cb.r = &row{map[string]cell{}}
		}

	} else {
		groups := []*sortGroup{}
		for _, group := range g.groups {
			groups = append(groups, &sortGroup{
				columnDef:  g.columnDef,
				columnView: g.ColumnView,
				key:        group.value,
				asc:        group.asc,
			})
		}
		// sort the groups...
		slices.SortFunc(groups, compareGroups)
		for _, sg := range groups {
			group := g.groups[sg.key]
			sp := 0
			if len(g.counts) > 1 {
				sp = group.counts[len(group.counts)-2]
			}
			displayValue := "..."
			if group.value != 0 {
				key := columnViews[g.columnDef.name].groupKeyToKey[group.value]
				displayValue = g.columnDef.keyToValue[key]
			}
			cell := cell{
				colName: g.columnDef.name,
				span:    sp,
				//value: fmt.Sprintf("(%d)", g.counts[0]
				//value: g.columnDef.name + "-" + fmt.Sprintf(`%d %d %s`, group.value, sg.key, g.columnDef.keyToValue[group.value]), //g.columnDef.indexToValue[uint32(group.value)],
				//value:  g.columnDef.name + "-" + fmt.Sprintf(`%d %d %s`, group.value, sg.key, displayValue), //g.columnDef.indexToValue[uint32(group.value)],
				value:  fmt.Sprintf(`%s`, displayValue), //g.columnDef.indexToValue[uint32(group.value)],
				counts: group.counts,
			}
			cb.r.cells[g.columnDef.name] = cell
			s := map[string][]int{}
			for col, sum := range group.sums {
				s[col] = append(sums[col], sum)
			}
			cb.cells(group, v.columnViews, aggregatedColumns, s)
		}
	}
}
