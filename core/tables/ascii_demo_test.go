package tables

import (
	"fmt"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// TestToAsciiDemo demonstrates the ASCII table display with the user's example data
func TestToAsciiDemo(t *testing.T) {
	// Create a test table with the exact data from the user
	table := NewDataTable()

	// Column 1: Status
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	// Active rows (9 rows)
	for i := 0; i < 9; i++ {
		statusCol.Append("Active")
	}
	// Inactive rows (9 rows)
	for i := 0; i < 9; i++ {
		statusCol.Append("Inactive")
	}
	// Pending rows (9 rows)
	for i := 0; i < 9; i++ {
		statusCol.Append("Pending")
	}
	statusCol.FinalizeColumn()

	// Column 2: Region
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	// Active: North(3), South(3), East(3)
	regionCol.Append("North")
	regionCol.Append("North")
	regionCol.Append("North")
	regionCol.Append("South")
	regionCol.Append("South")
	regionCol.Append("South")
	regionCol.Append("East")
	regionCol.Append("East")
	regionCol.Append("East")
	// Inactive: North(3), South(3), East(3)
	regionCol.Append("North")
	regionCol.Append("North")
	regionCol.Append("North")
	regionCol.Append("South")
	regionCol.Append("South")
	regionCol.Append("South")
	regionCol.Append("East")
	regionCol.Append("East")
	regionCol.Append("East")
	// Pending: North(3), South(3), East(3)
	regionCol.Append("North")
	regionCol.Append("North")
	regionCol.Append("North")
	regionCol.Append("South")
	regionCol.Append("South")
	regionCol.Append("South")
	regionCol.Append("East")
	regionCol.Append("East")
	regionCol.Append("East")
	regionCol.FinalizeColumn()

	// Column 3: Category
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", ""))
	// Active North: A, A, B
	categoryCol.Append("A")
	categoryCol.Append("A")
	categoryCol.Append("B")
	// Active South: A, B, B
	categoryCol.Append("A")
	categoryCol.Append("B")
	categoryCol.Append("B")
	// Active East: Z, C, C
	categoryCol.Append("Z")
	categoryCol.Append("C")
	categoryCol.Append("C")
	// Inactive North: A, A, B
	categoryCol.Append("A")
	categoryCol.Append("A")
	categoryCol.Append("B")
	// Inactive South: A, B, B
	categoryCol.Append("A")
	categoryCol.Append("B")
	categoryCol.Append("B")
	// Inactive East: Z, A, Z
	categoryCol.Append("Z")
	categoryCol.Append("A")
	categoryCol.Append("Z")
	// Pending North: A, A, A
	categoryCol.Append("A")
	categoryCol.Append("A")
	categoryCol.Append("A")
	// Pending South: A, B, B
	categoryCol.Append("A")
	categoryCol.Append("B")
	categoryCol.Append("B")
	// Pending East: Z, C, C
	categoryCol.Append("Z")
	categoryCol.Append("C")
	categoryCol.Append("C")
	categoryCol.FinalizeColumn()

	// Add columns to table
	table.AddColumn(statusCol)
	table.AddColumn(regionCol)
	table.AddColumn(categoryCol)

	// Create TableView
	tableView := NewTableView(table, "test_table")
	tableView.columnViews = make(map[string]*columns.ColumnView)
	tableView.columnViews["status"] = &columns.ColumnView{}
	tableView.columnViews["region"] = &columns.ColumnView{}
	tableView.columnViews["category"] = &columns.ColumnView{}
	tableView.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView.blocksByColumn = make(map[string][]*grouping.Block)

	// Group by status, region, category
	tableView.GroupTable([]string{"status", "region", "category"}, []string{}, make(map[string]Compare), make(map[string]bool))

	// Display the grouped table
	fmt.Println("\n=== Grouped Table Display ===")
	fmt.Println(tableView.ToAscii())
}
