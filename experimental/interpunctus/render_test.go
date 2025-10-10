package main

import (
	"strings"
	"testing"
)

func TestHTMLRenderer_RenderTable(t *testing.T) {
	// Create a simple view model
	vm := &TableViewModel{
		Scripts: Scripts{
			CSS: "/* test css */",
			JS:  "// test js",
		},
		Headers: []HeaderRow{
			{
				Type: HeaderTypeColumn,
				Cells: []HeaderCell{
					{Content: "Column1"},
					{Content: "Column2"},
				},
			},
		},
		Rows: []DataRow{
			{
				Cells: []DataCell{
					{Value: "data1", Span: 1},
					{Value: "data2", Span: 1},
				},
			},
		},
	}

	renderer := NewHTMLRenderer()
	sb := renderer.Render(vm)
	html := sb.String()

	// Test that basic structure is present
	if !strings.Contains(html, "<style>/* test css */</style>") {
		t.Error("Expected CSS to be rendered")
	}
	if !strings.Contains(html, "<script>// test js</script>") {
		t.Error("Expected JS to be rendered")
	}
	if !strings.Contains(html, "<table>") {
		t.Error("Expected table tag")
	}
	if !strings.Contains(html, "</table>") {
		t.Error("Expected closing table tag")
	}
	if !strings.Contains(html, "Column1") {
		t.Error("Expected Column1 in output")
	}
	if !strings.Contains(html, "data1") {
		t.Error("Expected data1 in output")
	}
}

func TestHTMLRenderer_RenderCommands(t *testing.T) {
	vm := &TableViewModel{
		Headers: []HeaderRow{
			{
				Type: HeaderTypeGroup,
				Cells: []HeaderCell{
					{
						ColumnName: "status",
						Commands: []Command{
							{Label: "G", URL: "grouped?col=status"},
						},
						CSSClass: "cmd",
					},
				},
			},
		},
	}

	renderer := NewHTMLRenderer()
	sb := renderer.Render(vm)
	html := sb.String()

	// Test command rendering
	if !strings.Contains(html, `<a href="grouped?col=status">G</a>`) {
		t.Error("Expected group command link")
	}
	if !strings.Contains(html, `class="cmd"`) {
		t.Error("Expected cmd CSS class")
	}
}

func TestHTMLRenderer_RenderFilterInput(t *testing.T) {
	vm := &TableViewModel{
		Headers: []HeaderRow{
			{
				Type: HeaderTypeFilter,
				Cells: []HeaderCell{
					{
						ColumnName: "status",
						InputValue: "active||inactive",
						CSSClass:   "filter",
					},
				},
			},
		},
	}

	renderer := NewHTMLRenderer()
	sb := renderer.Render(vm)
	html := sb.String()

	// Test filter input rendering
	if !strings.Contains(html, `<input name="status"`) {
		t.Error("Expected input with name attribute")
	}
	if !strings.Contains(html, `value="active||inactive"`) {
		t.Error("Expected input value")
	}
	if !strings.Contains(html, `type="text"`) {
		t.Error("Expected text input type")
	}
}

func TestHTMLRenderer_RenderDataCellSpan(t *testing.T) {
	vm := &TableViewModel{
		Rows: []DataRow{
			{
				Cells: []DataCell{
					{Value: "grouped", Span: 3},
					{Value: "normal", Span: 1},
				},
			},
		},
	}

	renderer := NewHTMLRenderer()
	sb := renderer.Render(vm)
	html := sb.String()

	// Test rowspan rendering
	if !strings.Contains(html, `<td rowspan="3">grouped</td>`) {
		t.Error("Expected rowspan=3 for grouped cell")
	}
	if !strings.Contains(html, `<td>normal</td>`) {
		t.Error("Expected normal td for span=1")
	}
}

func TestHTMLRenderer_MultipleCommandsSpacing(t *testing.T) {
	vm := &TableViewModel{
		Headers: []HeaderRow{
			{
				Type: HeaderTypeMove,
				Cells: []HeaderCell{
					{
						Commands: []Command{
							{Label: "<<", URL: "url1"},
							{Label: "<", URL: "url2"},
							{Label: ">", URL: "url3"},
						},
						CSSClass: "cmd",
					},
				},
			},
		},
	}

	renderer := NewHTMLRenderer()
	sb := renderer.Render(vm)
	html := sb.String()

	// Test that commands are properly spaced
	if !strings.Contains(html, `<a href="url1"><<</a>`) {
		t.Error("Expected first command")
	}
	if !strings.Contains(html, `<a href="url2"><</a>`) {
		t.Error("Expected second command")
	}
	if !strings.Contains(html, `<a href="url3">></a>`) {
		t.Error("Expected third command")
	}
	// Should have &nbsp; between commands
	if strings.Count(html, "&nbsp;") < 2 {
		t.Error("Expected spacing between commands")
	}
}