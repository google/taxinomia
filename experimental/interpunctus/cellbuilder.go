package main

import "fmt"

// row represents a row of cells
type row struct {
	cells map[string]cell
}

// cell represents a single cell in a row
type cell struct {
	colName    string
	span       int
	value      string
	counts     []int
	aggregates []int
}

// cellBuilder builds rows of cells for grouped data
type cellBuilder struct {
	rows []*row
	r    *row
}

// CellBuilder creates a new cellBuilder
func CellBuilder() *cellBuilder {
	cb := &cellBuilder{rows: []*row{}, r: &row{}}
	cb.r = &row{map[string]cell{}}
	return cb
}

// cells recursively builds cells for a group hierarchy
func (cb *cellBuilder) cells(g *Group, columnViews map[string]*ColumnView, aggregatedColumns []string, aggregates map[string][]int) {
	sorter := NewGroupSorter(columnViews)

	if len(g.indices) != 0 {
		// Leaf level - sort and process indices
		indicesList := sorter.PrepareIndicesForSorting(g)
		sorter.SortIndices(indicesList)

		for _, group := range indicesList {
			var leaf *Group
			for _, gg := range g.groups {
				if gg.columnDef.name == group.columnDef.name {
					leaf = gg
				}
			}

			// Get display value
			displayValue := "..."
			if group.value != 0 {
				cv, ok := columnViews[g.columnDef.name]
				if ok && cv != nil {
					key := cv.groupKeyToKey[group.value]
					displayValue = g.columnDef.keyToValue[key]
				}
			}

			c := cell{
				colName: g.columnDef.name,
				span:    1,
				value:   displayValue,
				counts:  []int{len(group.indices)},
			}

			// Handle aggregates for aggregated columns
			for _, col := range aggregatedColumns {
				if col == g.columnDef.name {
					continue
				}
				if leaf != nil {
					c.aggregates = append(aggregates[col], leaf.aggregates[col])
				} else {
					c.aggregates = append(aggregates[col], 0)
				}
				cb.r.cells[col] = cell{
					aggregates: c.aggregates,
					colName:    col,
					counts:     []int{len(group.indices)},
				}
			}

			// Emit row and start new one
			cb.r.cells[g.columnDef.name] = c
			cb.rows = append(cb.rows, cb.r)
			cb.r = &row{cells: map[string]cell{}}
		}
	} else {
		// Non-leaf level - sort and process groups
		groups := sorter.PrepareGroupsForSorting(g)
		sorter.SortGroups(groups)

		for _, sg := range groups {
			group := g.groups[sg.key]
			sp := 0
			if len(g.counts) > 1 {
				sp = group.counts[len(group.counts)-2]
			}

			// Get display value
			displayValue := "..."
			if group.value != 0 {
				cv, ok := columnViews[g.columnDef.name]
				if ok && cv != nil {
					key := cv.groupKeyToKey[group.value]
					displayValue = g.columnDef.keyToValue[key]
				}
			}

			cell := cell{
				colName: g.columnDef.name,
				span:    sp,
				value:   fmt.Sprintf(`%s`, displayValue),
				counts:  group.counts,
			}
			cb.r.cells[g.columnDef.name] = cell

			// Update aggregates
			s := map[string][]int{}
			for col, agg := range group.aggregates {
				s[col] = append(aggregates[col], agg)
			}

			// Recursively process child groups
			cb.cells(group, columnViews, aggregatedColumns, s)
		}
	}
}