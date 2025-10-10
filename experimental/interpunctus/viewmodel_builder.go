package main

import (
	"fmt"
)

// ViewModelBuilder builds a TableViewModel from the raw data structures
type ViewModelBuilder struct {
	view    *View
	table   *Table
	group   *Group
	indices []int
	sorter  *GroupSorter
}

// NewViewModelBuilder creates a new ViewModelBuilder
func NewViewModelBuilder(table *Table, view *View, group *Group, indices []int) *ViewModelBuilder {
	return &ViewModelBuilder{
		view:    view,
		table:   table,
		group:   group,
		indices: indices,
		sorter:  NewGroupSorter(view.columnViews),
	}
}

// Build creates a complete TableViewModel
func (b *ViewModelBuilder) Build() *TableViewModel {
	// Build data rows first (this populates cellBuilder rows if grouped)
	var dataRows []DataRow
	if len(b.view.grouping) == 0 || b.group == nil {
		dataRows = b.buildUngroupedRows()
	} else {
		dataRows = b.buildGroupedRows()
	}

	return &TableViewModel{
		Scripts: Scripts{
			CSS: tableCSS,
			JS:  tableJS,
		},
		Headers: b.buildHeaders(),
		Rows:    dataRows,
	}
}

// buildHeaders builds all header rows
func (b *ViewModelBuilder) buildHeaders() []HeaderRow {
	headers := []HeaderRow{}

	// Move commands row
	headers = append(headers, b.buildMoveCommandsRow())

	// Sort commands row
	headers = append(headers, b.buildSortCommandsRow())

	// Group commands row
	headers = append(headers, b.buildGroupCommandsRow())

	// Filter inputs row
	headers = append(headers, b.buildFilterRow())

	// Column names row
	headers = append(headers, b.buildColumnNamesRow())

	return headers
}

// buildMoveCommandsRow builds the row with move left/right commands
func (b *ViewModelBuilder) buildMoveCommandsRow() HeaderRow {
	cells := []HeaderCell{}

	for _, col := range b.view.order {
		commands := []Command{}

		// Move leftmost
		w, _ := b.view.MoveLeftMost(col)
		commands = append(commands, Command{
			Label: "<<",
			URL:   "sorted?" + w.ToQuery(),
		})

		// Move left
		w, _ = b.view.MoveLeft(col)
		commands = append(commands, Command{
			Label: "<",
			URL:   "sorted?" + w.ToQuery(),
		})

		// Move right
		w, _ = b.view.MoveRight(col)
		commands = append(commands, Command{
			Label: ">",
			URL:   "sorted?" + w.ToQuery(),
		})

		cells = append(cells, HeaderCell{
			ColumnName: col,
			Commands:   commands,
			CSSClass:   "cmd",
		})
	}

	return HeaderRow{
		Type:  HeaderTypeMove,
		Cells: cells,
	}
}

// buildSortCommandsRow builds the row with sort direction toggle and shift commands
func (b *ViewModelBuilder) buildSortCommandsRow() HeaderRow {
	cells := []HeaderCell{}

	for _, col := range b.view.order {
		commands := []Command{}

		// Toggle sort direction
		w, _ := b.view.ToggleSortDirection(col)
		commands = append(commands, Command{
			Label: "↕", // &udarr; in HTML
			URL:   "sorted?" + w.ToQuery(),
		})

		// Left shift sort level
		w, _ = b.view.LeftShiftSortLevel(col)
		commands = append(commands, Command{
			Label: "⇐", // &lArr; in HTML
			URL:   "sorted?" + w.ToQuery(),
		})

		// Right shift sort level
		w, _ = b.view.RightShiftSortLevel(col)
		commands = append(commands, Command{
			Label: "⇒", // &rArr; in HTML
			URL:   "sorted?" + w.ToQuery(),
		})

		cells = append(cells, HeaderCell{
			ColumnName: col,
			Commands:   commands,
			CSSClass:   "cmd",
		})
	}

	return HeaderRow{
		Type:  HeaderTypeSort,
		Cells: cells,
	}
}

// buildGroupCommandsRow builds the row with group/ungroup commands
func (b *ViewModelBuilder) buildGroupCommandsRow() HeaderRow {
	cells := []HeaderCell{}

	for _, col := range b.view.order {
		// Check if column is currently grouped
		index := -1
		for i, c := range b.view.grouping {
			if c == col {
				index = i
				break
			}
		}

		w, _ := b.view.ToggleGrouping(col)
		query := w.ToQuery()

		var command Command
		if index == -1 {
			// Not grouped - show "G"
			command = Command{
				Label: "G",
				URL:   "grouped?" + query,
			}
		} else {
			// Grouped - show "U"
			command = Command{
				Label: "U",
				URL:   "sorted?" + query,
			}
		}

		cells = append(cells, HeaderCell{
			ColumnName: col,
			Commands:   []Command{command},
			CSSClass:   "cmd",
		})
	}

	return HeaderRow{
		Type:  HeaderTypeGroup,
		Cells: cells,
	}
}

// buildFilterRow builds the row with filter inputs
func (b *ViewModelBuilder) buildFilterRow() HeaderRow {
	cells := []HeaderCell{}

	for _, col := range b.view.order {
		inputValue := ""
		if filters, exists := b.view.groupOn[col]; exists && len(filters) > 0 {
			// Join filter values with ||
			for i, f := range filters {
				if i > 0 {
					inputValue += "||"
				}
				inputValue += f
			}
		}

		cells = append(cells, HeaderCell{
			ColumnName: col,
			InputValue: inputValue,
			CSSClass:   "filter",
		})
	}

	return HeaderRow{
		Type:  HeaderTypeFilter,
		Cells: cells,
	}
}

// buildColumnNamesRow builds the row with column names
func (b *ViewModelBuilder) buildColumnNamesRow() HeaderRow {
	cells := []HeaderCell{}

	for _, col := range b.view.order {
		sortPos := ""
		if pos, exists := b.view.groupSortPos[col]; exists {
			sortPos = fmt.Sprintf(" (%d)", pos)
		}

		direction := ""
		if b.view.sorting[col] {
			direction = " ↑"
		} else {
			direction = " ↓"
		}

		cells = append(cells, HeaderCell{
			ColumnName: col,
			Content:    col + sortPos + direction,
		})
	}

	return HeaderRow{
		Type:  HeaderTypeColumn,
		Cells: cells,
	}
}

// buildUngroupedRows builds data rows for ungrouped view
func (b *ViewModelBuilder) buildUngroupedRows() []DataRow {
	rows := []DataRow{}

	for _, i := range b.indices {
		cells := []DataCell{}
		for _, col := range b.view.order {
			value := b.table.columns[col].ColumnDef().keyToValue[b.table.columns[col].get(int(i))]
			cells = append(cells, DataCell{
				Value: value,
				Span:  1,
			})
		}
		rows = append(rows, DataRow{Cells: cells})
	}

	return rows
}

// buildGroupedRows builds data rows for grouped view
func (b *ViewModelBuilder) buildGroupedRows() []DataRow {
	// Use CellBuilder to build the grouped rows
	cb := CellBuilder()
	aggregates := map[string][]int{}
	for _, c := range b.view.AggregatedColumns() {
		aggregates[c] = []int{b.group.aggregates[c]}
	}
	cb.cells(b.group, b.view.columnViews, b.view.AggregatedColumns(), aggregates)

	// Convert cellBuilder rows to DataRows
	dataRows := []DataRow{}
	for _, row := range cb.rows {
		cells := []DataCell{}
		for _, c := range b.view.order {
			cell, exists := row.cells[c]
			if !exists {
				// Skip columns that don't have a cell in this row
				continue
			}

			// Format the cell value
			value := cell.value

			// For aggregated columns (not grouped columns), show the sum
			isGroupedColumn := false
			for _, groupCol := range b.view.grouping {
				if groupCol == c {
					isGroupedColumn = true
					break
				}
			}

			if !isGroupedColumn {
				// Check if this column is summable
				isSummable := false
				if col := b.table.columns[c]; col != nil {
					isSummable = col.summable()
				}

				if isSummable && len(cell.aggregates) > 0 {
					// Show all levels of aggregates for summable columns
					aggregates := ""
					sep := ""
					for _, s := range cell.aggregates {
						aggregates += sep + fmt.Sprintf("%d", s)
						sep = "/"
					}
					value = aggregates
					if len(cell.aggregates) > 1 {
						value = "[" + aggregates + "]"
					}
				} else if len(cell.counts) > 0 {
					// Show all levels of counts for non-summable columns
					counts := ""
					sep := ""
					for _, cnt := range cell.counts {
						counts += sep + fmt.Sprintf("%d", cnt)
						sep = "/"
					}
					value = "[" + counts + "]"
				}
			}

			// Add counts if present (for grouped columns)
			if isGroupedColumn && len(cell.counts) > 0 {
				counts := ""
				sep := ""
				for _, cnt := range cell.counts {
					counts += sep + fmt.Sprintf("%d", cnt)
					sep = "/"
				}
				value += " [" + counts + "]"
			}

			cells = append(cells, DataCell{
				Value:    value,
				Span:     cell.span,
				IsSum:    len(cell.aggregates) > 0 && !isGroupedColumn,
				CSSClass: "",
			})
		}
		dataRows = append(dataRows, DataRow{Cells: cells})
	}

	return dataRows
}