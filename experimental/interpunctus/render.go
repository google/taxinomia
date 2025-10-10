package main

import (
	_ "embed"
	"fmt"
	"strings"
)

//go:embed resources/tables/style.css
var tableCSS string

//go:embed resources/tables/script.js
var tableJS string

// Render is the main entry point - builds the view model and renders it
func Render(t *Table, v *View, g *Group, indices []int) *strings.Builder {
	// Build the view model
	builder := NewViewModelBuilder(t, v, g, indices)
	vm := builder.Build()

	// Render the view model to HTML
	renderer := NewHTMLRenderer()
	return renderer.Render(vm)
}

// HTMLRenderer renders a TableViewModel to HTML
type HTMLRenderer struct {
	sb *strings.Builder
}

// NewHTMLRenderer creates a new HTMLRenderer
func NewHTMLRenderer() *HTMLRenderer {
	return &HTMLRenderer{
		sb: &strings.Builder{},
	}
}

// Render renders the complete table to HTML
func (r *HTMLRenderer) Render(vm *TableViewModel) *strings.Builder {
	r.renderScripts(vm.Scripts)
	r.renderTable(vm)
	return r.sb
}

// renderScripts renders the CSS and JS
func (r *HTMLRenderer) renderScripts(scripts Scripts) {
	r.sb.WriteString(`<style>`)
	r.sb.WriteString(scripts.CSS)
	r.sb.WriteString(`</style>`)
	r.sb.WriteString(`<script>`)
	r.sb.WriteString(scripts.JS)
	r.sb.WriteString(`</script>`)
}

// renderTable renders the complete table
func (r *HTMLRenderer) renderTable(vm *TableViewModel) {
	r.sb.WriteString(`<table>`)

	// Render all header rows
	for _, header := range vm.Headers {
		r.renderHeaderRow(header)
	}

	// Render all data rows
	for _, row := range vm.Rows {
		r.renderDataRow(row)
	}

	r.sb.WriteString(`</table>`)
}

// renderHeaderRow renders a single header row
func (r *HTMLRenderer) renderHeaderRow(header HeaderRow) {
	r.sb.WriteString(`<tr>`)

	for _, cell := range header.Cells {
		switch header.Type {
		case HeaderTypeFilter:
			r.renderFilterCell(cell)
		case HeaderTypeColumn:
			r.renderColumnNameCell(cell)
		default:
			r.renderCommandCell(cell)
		}
	}

	r.sb.WriteString(`</tr>`)
}

// renderCommandCell renders a cell with command buttons
func (r *HTMLRenderer) renderCommandCell(cell HeaderCell) {
	fmt.Fprintf(r.sb, `<th class="%s">`, cell.CSSClass)

	for i, cmd := range cell.Commands {
		if i > 0 {
			r.sb.WriteString(`&nbsp;`)
		}
		fmt.Fprintf(r.sb, `<a href="%s">%s</a>`, cmd.URL, cmd.Label)
		r.sb.WriteString(`&nbsp;`)
	}

	r.sb.WriteString(`</th>`)
}

// renderFilterCell renders a filter input cell
func (r *HTMLRenderer) renderFilterCell(cell HeaderCell) {
	r.sb.WriteString(`<th class="filter">`)
	fmt.Fprintf(r.sb, `<input name="%s" type="text" value="%s" size="7" class="groupon-input" data-column="%s"/>`,
		cell.ColumnName, cell.InputValue, cell.ColumnName)
	r.sb.WriteString(`</th>`)
}

// renderColumnNameCell renders a column name cell
func (r *HTMLRenderer) renderColumnNameCell(cell HeaderCell) {
	r.sb.WriteString(`<th>`)
	r.sb.WriteString(cell.Content)
	r.sb.WriteString(`</th>`)
}

// renderDataRow renders a single data row
func (r *HTMLRenderer) renderDataRow(row DataRow) {
	r.sb.WriteString(`<tr>`)

	for _, cell := range row.Cells {
		r.renderDataCell(cell)
	}

	r.sb.WriteString(`</tr>`)
}

// renderDataCell renders a single data cell
func (r *HTMLRenderer) renderDataCell(cell DataCell) {
	if cell.Span > 1 {
		fmt.Fprintf(r.sb, `<td rowspan="%d">`, cell.Span)
	} else {
		r.sb.WriteString(`<td>`)
	}

	r.sb.WriteString(cell.Value)
	r.sb.WriteString(`</td>`)
}