package main

// TableViewModel represents all the data needed to render a table
// This is a pure data structure with no logic
type TableViewModel struct {
	Scripts Scripts
	Headers []HeaderRow
	Rows    []DataRow
}

// Scripts holds embedded CSS and JS assets
type Scripts struct {
	CSS string
	JS  string
}

// HeaderType represents different types of header rows
type HeaderType string

const (
	HeaderTypeMove   HeaderType = "move"
	HeaderTypeSort   HeaderType = "sort"
	HeaderTypeGroup  HeaderType = "group"
	HeaderTypeFilter HeaderType = "filter"
	HeaderTypeColumn HeaderType = "column"
)

// HeaderRow represents a single row in the table header
type HeaderRow struct {
	Type  HeaderType
	Cells []HeaderCell
}

// HeaderCell represents a single cell in a header row
type HeaderCell struct {
	ColumnName string
	Commands   []Command
	InputValue string // For filter inputs
	Content    string // For column names
	CSSClass   string
}

// Command represents a clickable command in a header cell
type Command struct {
	Label    string // "G", "U", "<<", "<", ">", "↕", etc.
	URL      string // Pre-computed href
	CSSClass string // Additional CSS classes
	Title    string // Tooltip text
}

// DataRow represents a row of actual data
type DataRow struct {
	Cells []DataCell
}

// DataCell represents a single data cell
type DataCell struct {
	Value    string
	Span     int    // For colspan in grouped views
	CSSClass string // For styling
	IsSum    bool   // True if this is a sum/aggregate cell
}