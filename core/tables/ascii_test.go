package tables

import (
	"fmt"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/grouping"
)

// TestToAscii demonstrates the ASCII table display
func TestToAscii(t *testing.T) {
	// Test 1: Single column grouping
	fmt.Println("\n=== Test 1: Single Column Grouping (Status) ===")
	table1 := NewDataTable()
	statusCol1 := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	statusCol1.Append("Active")
	statusCol1.Append("Active")
	statusCol1.Append("Inactive")
	statusCol1.Append("Active")
	statusCol1.Append("Inactive")
	statusCol1.Append("Pending")
	statusCol1.FinalizeColumn()
	table1.AddColumn(statusCol1)

	tableView1 := NewTableView(table1, "test_table")
	tableView1.columnViews = make(map[string]*columns.ColumnView)
	tableView1.columnViews["status"] = &columns.ColumnView{}
	tableView1.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView1.blocksByColumn = make(map[string][]*grouping.Block)

	tableView1.GroupTable([]string{"status"}, []string{}, make(map[string]Compare), make(map[string]bool))
	fmt.Println(tableView1.ToAscii())

	// Test 2: Two-level hierarchical grouping
	fmt.Println("\n=== Test 2: Two-Level Hierarchical Grouping (Status -> Region) ===")
	table2 := NewDataTable()

	statusCol2 := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	statusCol2.Append("Active")   // 0
	statusCol2.Append("Active")   // 1
	statusCol2.Append("Inactive") // 2
	statusCol2.Append("Active")   // 3
	statusCol2.Append("Inactive") // 4
	statusCol2.Append("Active")   // 5
	statusCol2.FinalizeColumn()

	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))
	regionCol.Append("North") // 0
	regionCol.Append("South") // 1
	regionCol.Append("North") // 2
	regionCol.Append("North") // 3
	regionCol.Append("South") // 4
	regionCol.Append("East")  // 5
	regionCol.FinalizeColumn()

	table2.AddColumn(statusCol2)
	table2.AddColumn(regionCol)

	tableView2 := NewTableView(table2, "test_table")
	tableView2.columnViews = make(map[string]*columns.ColumnView)
	tableView2.columnViews["status"] = &columns.ColumnView{}
	tableView2.columnViews["region"] = &columns.ColumnView{}
	tableView2.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView2.blocksByColumn = make(map[string][]*grouping.Block)

	tableView2.GroupTable([]string{"status", "region"}, []string{}, make(map[string]Compare), make(map[string]bool))
	fmt.Println(tableView2.ToAscii())

	// Test 3: Numeric columns (Category -> Priority)
	fmt.Println("\n=== Test 3: Numeric Columns (Category -> Priority) ===")
	table3 := NewDataTable()

	categoryCol := columns.NewUint32Column(columns.NewColumnDef("category", "Category", ""))
	categoryCol.Append(1) // 0
	categoryCol.Append(1) // 1
	categoryCol.Append(2) // 2
	categoryCol.Append(1) // 3
	categoryCol.Append(2) // 4
	categoryCol.Append(3) // 5
	categoryCol.FinalizeColumn()

	priorityCol := columns.NewUint32Column(columns.NewColumnDef("priority", "Priority", ""))
	priorityCol.Append(10) // 0
	priorityCol.Append(10) // 1
	priorityCol.Append(10) // 2
	priorityCol.Append(20) // 3
	priorityCol.Append(20) // 4
	priorityCol.Append(10) // 5
	priorityCol.FinalizeColumn()

	table3.AddColumn(categoryCol)
	table3.AddColumn(priorityCol)

	tableView3 := NewTableView(table3, "test_table")
	tableView3.columnViews = make(map[string]*columns.ColumnView)
	tableView3.columnViews["category"] = &columns.ColumnView{}
	tableView3.columnViews["priority"] = &columns.ColumnView{}
	tableView3.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView3.blocksByColumn = make(map[string][]*grouping.Block)

	tableView3.GroupTable([]string{"category", "priority"}, []string{}, make(map[string]Compare), make(map[string]bool))
	fmt.Println(tableView3.ToAscii())

	// Test 4: Three-level hierarchical grouping
	fmt.Println("\n=== Test 4: Three-Level Hierarchical Grouping (Col1 -> Col2 -> Col3) ===")
	table4 := NewDataTable()

	col1 := columns.NewStringColumn(columns.NewColumnDef("col1", "Col1", ""))
	col1.Append("A") // 0
	col1.Append("A") // 1
	col1.Append("B") // 2
	col1.Append("B") // 3
	col1.FinalizeColumn()

	col2 := columns.NewStringColumn(columns.NewColumnDef("col2", "Col2", ""))
	col2.Append("X") // 0
	col2.Append("Y") // 1
	col2.Append("X") // 2
	col2.Append("Y") // 3
	col2.FinalizeColumn()

	col3 := columns.NewStringColumn(columns.NewColumnDef("col3", "Col3", ""))
	col3.Append("1") // 0
	col3.Append("2") // 1
	col3.Append("3") // 2
	col3.Append("4") // 3
	col3.FinalizeColumn()

	table4.AddColumn(col1)
	table4.AddColumn(col2)
	table4.AddColumn(col3)

	tableView4 := NewTableView(table4, "test_table")
	tableView4.columnViews = make(map[string]*columns.ColumnView)
	tableView4.columnViews["col1"] = &columns.ColumnView{}
	tableView4.columnViews["col2"] = &columns.ColumnView{}
	tableView4.columnViews["col3"] = &columns.ColumnView{}
	tableView4.groupedColumns = make(map[string]*grouping.GroupedColumn)
	tableView4.blocksByColumn = make(map[string][]*grouping.Block)

	tableView4.GroupTable([]string{"col1", "col2", "col3"}, []string{}, make(map[string]Compare), make(map[string]bool))
	fmt.Println(tableView4.ToAscii())
}
