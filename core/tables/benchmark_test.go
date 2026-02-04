/*
SPDX-License-Identifier: Apache-2.0

Copyright 2024 The Taxinomia Authors
*/

/*
Benchmarks for TableView operations with large datasets (10M rows).

# Running Benchmarks

Run all benchmarks:

	go test -bench=. -benchmem ./core/tables/

Run specific benchmark:

	go test -bench=BenchmarkFullPipeline10M -benchmem ./core/tables/

Run worst-case benchmarks only (80% selectivity):

	go test -bench=WorstCase -benchmem ./core/tables/

Run with multiple iterations for more accurate results:

	go test -bench=. -benchmem -count=3 ./core/tables/

Run with longer timeout (some benchmarks take several minutes):

	go test -bench=. -benchmem -timeout=30m ./core/tables/

# Available Benchmarks

Normal case (filter reduces to 1% of rows):
  - BenchmarkTableCreation10M      - Create 10M row table with 10 columns
  - BenchmarkTableViewCreation10M  - Create TableView from existing table
  - BenchmarkFiltering10M          - Apply single filter (1% selectivity)
  - BenchmarkFilteringMultiple10M  - Apply multiple filters
  - BenchmarkGrouping10M           - Group by single column (100 groups)
  - BenchmarkGroupingMultiLevel10M - Group by two columns
  - BenchmarkFilterThenGroup10M    - Filter then group
  - BenchmarkComputedColumn10M     - Add and evaluate computed column
  - BenchmarkFullPipeline10M       - Full page load simulation

Worst case (filter keeps 80% of rows):
  - BenchmarkFilteringWorstCase10M     - Filter with 80% selectivity
  - BenchmarkGroupingWorstCase10M      - Group after 80% selectivity filter
  - BenchmarkFullPipelineWorstCase10M  - Full pipeline with 80% selectivity

# Timing Test

Run detailed timing test with step-by-step output:

	go test -run=TestFullPipelineTiming -v ./core/tables/

Skip timing test in short mode:

	go test -short ./core/tables/

# Configuring Table Size

To test with different table sizes, modify the parameters in createLargeTable calls:
  - numRows: Number of rows (default 10,000,000)
  - numCols: Number of columns (default 10)

Example: To benchmark with 1M rows instead of 10M, change:

	table := createLargeTable(1_000_000, 10)
*/

package tables

import (
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/expr"
	"github.com/google/taxinomia/core/grouping"
)

// createLargeTable creates a table with the specified number of rows and columns
func createLargeTable(numRows, numCols int) *DataTable {
	table := NewDataTable()

	for c := 0; c < numCols; c++ {
		colName := fmt.Sprintf("col%d", c)
		var colDef *columns.ColumnDef

		switch c % 4 {
		case 0:
			// String column with moderate cardinality (for grouping)
			colDef = columns.NewColumnDef(colName, colName, "")
			col := columns.NewStringColumn(colDef)
			for i := 0; i < numRows; i++ {
				col.Append(fmt.Sprintf("category_%d", i%100))
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case 1:
			// Numeric string column (for filtering and computed)
			colDef = columns.NewColumnDef(colName, colName, "")
			col := columns.NewStringColumn(colDef)
			for i := 0; i < numRows; i++ {
				col.Append(strconv.Itoa(i % 10000))
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case 2:
			// Key column with unique values (for joins)
			colDef = columns.NewColumnDef(colName, colName, "row_id")
			col := columns.NewStringColumn(colDef)
			for i := 0; i < numRows; i++ {
				col.Append(fmt.Sprintf("id_%d", i))
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case 3:
			// Foreign key column (references another table)
			colDef = columns.NewColumnDef(colName, colName, "lookup_id")
			col := columns.NewStringColumn(colDef)
			for i := 0; i < numRows; i++ {
				col.Append(fmt.Sprintf("lookup_%d", i%1000))
			}
			col.FinalizeColumn()
			table.AddColumn(col)
		}
	}

	return table
}

// createLookupTable creates a small lookup table for joins
func createLookupTable(numRows int) *DataTable {
	table := NewDataTable()

	// Key column
	colDef := columns.NewColumnDef("lookup_id", "Lookup ID", "lookup_id")
	col := columns.NewStringColumn(colDef)
	for i := 0; i < numRows; i++ {
		col.Append(fmt.Sprintf("lookup_%d", i))
	}
	col.FinalizeColumn()
	table.AddColumn(col)

	// Value column
	valDef := columns.NewColumnDef("lookup_value", "Lookup Value", "")
	valCol := columns.NewStringColumn(valDef)
	for i := 0; i < numRows; i++ {
		valCol.Append(fmt.Sprintf("value_for_%d", i))
	}
	valCol.FinalizeColumn()
	table.AddColumn(valCol)

	return table
}

func BenchmarkTableCreation10M(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = createLargeTable(10_000_000, 10)
	}
}

func BenchmarkTableViewCreation10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = NewTableView(table, "benchmark")
	}
}

func BenchmarkFiltering10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	filters := map[string]string{
		"col0": "category_50", // Filter to ~1% of rows
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearFilters()
		tableView.ApplyFilters(filters)
	}
}

func BenchmarkFilteringMultiple10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	filters := map[string]string{
		"col0": "category_50",
		"col4": "category_25",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearFilters()
		tableView.ApplyFilters(filters)
	}
}

func BenchmarkGrouping10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	groupColumns := []string{"col0"} // 100 unique values

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearGroupings()
		tableView.GroupTable(groupColumns, []string{}, make(map[string]Compare), make(map[string]bool))
	}
}

func BenchmarkGroupingMultiLevel10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	groupColumns := []string{"col0", "col4"} // Two level grouping

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearGroupings()
		tableView.GroupTable(groupColumns, []string{}, make(map[string]Compare), make(map[string]bool))
	}
}

func BenchmarkFilterThenGroup10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	filters := map[string]string{
		"col0": "category_50",
	}
	groupColumns := []string{"col4"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearFilters()
		tableView.ClearGroupings()
		tableView.ApplyFilters(filters)
		tableView.GroupTable(groupColumns, []string{}, make(map[string]Compare), make(map[string]bool))
	}
}

func BenchmarkComputedColumn10M(b *testing.B) {
	table := createLargeTable(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	// Compile expression once
	compiled, err := expr.Compile("col1 * 2 + 100")
	if err != nil {
		b.Fatalf("Failed to compile expression: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create column getter
		getColumn := func(colName string, rowIndex uint32) (expr.Value, error) {
			col := tableView.GetColumn(colName)
			if col == nil {
				return expr.NilValue(), fmt.Errorf("column not found")
			}
			strVal, _ := col.GetString(rowIndex)
			if numVal, err := strconv.ParseFloat(strVal, 64); err == nil {
				return expr.NewFloat(numVal), nil
			}
			return expr.NewString(strVal), nil
		}

		bound := compiled.Bind(getColumn)

		colDef := columns.NewColumnDef("computed", "Computed", "")
		length := tableView.GetColumn("col0").Length()

		computedCol := columns.NewComputedFloat64Column(colDef, length, func(idx uint32) (float64, error) {
			val, err := bound.Eval(idx)
			if err != nil {
				return 0, err
			}
			return val.AsFloat(), nil
		})

		tableView.AddComputedColumn("computed", computedCol)

		// Evaluate 1000 sample rows
		for j := 0; j < 1000; j++ {
			_, _ = computedCol.GetValue(uint32(j * 10000))
		}

		tableView.RemoveComputedColumn("computed")
	}
}

func BenchmarkFullPipeline10M(b *testing.B) {
	// This benchmark simulates a full page load:
	// 1. Create table view
	// 2. Apply filters
	// 3. Add computed column
	// 4. Apply grouping
	// 5. Read first 100 rows

	table := createLargeTable(10_000_000, 10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Create table view
		tableView := NewTableView(table, "benchmark")

		// 2. Apply filter
		tableView.ApplyFilters(map[string]string{
			"col0": "category_50",
		})

		// 3. Add computed column
		compiled, _ := expr.Compile("col1 * 2")
		getColumn := func(colName string, rowIndex uint32) (expr.Value, error) {
			col := tableView.GetColumn(colName)
			if col == nil {
				return expr.NilValue(), fmt.Errorf("column not found")
			}
			strVal, _ := col.GetString(rowIndex)
			if numVal, err := strconv.ParseFloat(strVal, 64); err == nil {
				return expr.NewFloat(numVal), nil
			}
			return expr.NewString(strVal), nil
		}
		bound := compiled.Bind(getColumn)
		colDef := columns.NewColumnDef("computed", "Computed", "")
		length := table.Length()
		computedCol := columns.NewComputedFloat64Column(colDef, length, func(idx uint32) (float64, error) {
			val, _ := bound.Eval(idx)
			return val.AsFloat(), nil
		})
		tableView.AddComputedColumn("computed", computedCol)

		// 4. Apply grouping
		tableView.GroupTable([]string{"col4"}, []string{}, make(map[string]Compare), make(map[string]bool))

		// 5. Read first 100 filtered rows
		indices := tableView.GetFilteredIndices()
		limit := 100
		if len(indices) < limit {
			limit = len(indices)
		}
		for j := 0; j < limit; j++ {
			idx := indices[j]
			for _, colName := range tableView.GetColumnNames() {
				col := tableView.GetColumn(colName)
				if col != nil {
					_, _ = col.GetString(idx)
				}
			}
		}
	}
}

// createLargeTableWithLowSelectivity creates a table where "filter_col" has 80% "keep" and 20% "drop"
func createLargeTableWithLowSelectivity(numRows, numCols int) *DataTable {
	table := createLargeTable(numRows, numCols)

	// Add a special column for worst-case filtering (80% selectivity)
	colDef := columns.NewColumnDef("filter_col", "Filter Column", "")
	col := columns.NewStringColumn(colDef)
	for i := 0; i < numRows; i++ {
		if i%5 == 0 {
			col.Append("drop") // 20% of rows
		} else {
			col.Append("keep") // 80% of rows
		}
	}
	col.FinalizeColumn()
	table.AddColumn(col)

	return table
}

func BenchmarkFilteringWorstCase10M(b *testing.B) {
	// Worst case: filter keeps 80% of rows
	table := createLargeTableWithLowSelectivity(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	filters := map[string]string{
		"filter_col": "keep", // Keeps 80% of rows
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearFilters()
		tableView.ApplyFilters(filters)
	}
}

func BenchmarkGroupingWorstCase10M(b *testing.B) {
	// Worst case: grouping after filter that keeps 80% of rows
	table := createLargeTableWithLowSelectivity(10_000_000, 10)
	tableView := NewTableView(table, "benchmark")

	// Apply filter that keeps 80% of rows
	tableView.ApplyFilters(map[string]string{"filter_col": "keep"})

	groupColumns := []string{"col0"} // 100 unique values

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableView.ClearGroupings()
		tableView.GroupTable(groupColumns, []string{}, make(map[string]Compare), make(map[string]bool))
	}
}

func BenchmarkFullPipelineWorstCase10M(b *testing.B) {
	// Worst case scenario: filter only reduces by 20% (keeps 80%)
	// 1. Create table view
	// 2. Apply filter (keeps 80% of rows)
	// 3. Add computed column
	// 4. Apply grouping
	// 5. Read first 100 rows

	table := createLargeTableWithLowSelectivity(10_000_000, 10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Create table view
		tableView := NewTableView(table, "benchmark")

		// 2. Apply filter (keeps 80% of rows - worst case)
		tableView.ApplyFilters(map[string]string{
			"filter_col": "keep",
		})

		// 3. Add computed column
		compiled, _ := expr.Compile("col1 * 2")
		getColumn := func(colName string, rowIndex uint32) (expr.Value, error) {
			col := tableView.GetColumn(colName)
			if col == nil {
				return expr.NilValue(), fmt.Errorf("column not found")
			}
			strVal, _ := col.GetString(rowIndex)
			if numVal, err := strconv.ParseFloat(strVal, 64); err == nil {
				return expr.NewFloat(numVal), nil
			}
			return expr.NewString(strVal), nil
		}
		bound := compiled.Bind(getColumn)
		colDef := columns.NewColumnDef("computed", "Computed", "")
		length := table.Length()
		computedCol := columns.NewComputedFloat64Column(colDef, length, func(idx uint32) (float64, error) {
			val, _ := bound.Eval(idx)
			return val.AsFloat(), nil
		})
		tableView.AddComputedColumn("computed", computedCol)

		// 4. Apply grouping
		tableView.GroupTable([]string{"col4"}, []string{}, make(map[string]Compare), make(map[string]bool))

		// 5. Read first 100 filtered rows
		indices := tableView.GetFilteredIndices()
		limit := 100
		if len(indices) < limit {
			limit = len(indices)
		}
		for j := 0; j < limit; j++ {
			idx := indices[j]
			for _, colName := range tableView.GetColumnNames() {
				col := tableView.GetColumn(colName)
				if col != nil {
					_, _ = col.GetString(idx)
				}
			}
		}
	}
}

// TestFullPipelineTiming runs once and prints detailed timing
func TestFullPipelineTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timing test in short mode")
	}

	t.Log("Creating 10M row table with 10 columns...")
	table := createLargeTable(10_000_000, 10)
	t.Logf("Table created: %d rows, %d columns", table.Length(), len(table.GetColumnNames()))

	t.Log("\n=== Creating TableView ===")
	tableView := NewTableView(table, "benchmark")
	t.Log("TableView created")

	t.Log("\n=== Applying Filter (col0 = category_50) ===")
	tableView.ApplyFilters(map[string]string{"col0": "category_50"})
	visibleCount := tableView.GetFilteredRowCount()
	t.Logf("Filter applied: %d visible rows (%.2f%%)", visibleCount, float64(visibleCount)/float64(table.Length())*100)

	t.Log("\n=== Adding Computed Column ===")
	compiled, _ := expr.Compile("col1 * 2 + 100")
	getColumn := func(colName string, rowIndex uint32) (expr.Value, error) {
		col := tableView.GetColumn(colName)
		if col == nil {
			return expr.NilValue(), fmt.Errorf("column not found")
		}
		strVal, _ := col.GetString(rowIndex)
		if numVal, err := strconv.ParseFloat(strVal, 64); err == nil {
			return expr.NewFloat(numVal), nil
		}
		return expr.NewString(strVal), nil
	}
	bound := compiled.Bind(getColumn)
	colDef := columns.NewColumnDef("computed", "Computed", "")
	computedCol := columns.NewComputedFloat64Column(colDef, table.Length(), func(idx uint32) (float64, error) {
		val, _ := bound.Eval(idx)
		return val.AsFloat(), nil
	})
	tableView.AddComputedColumn("computed", computedCol)
	t.Log("Computed column added")

	t.Log("\n=== Applying Grouping (col4) ===")
	tableView.GroupTable([]string{"col4"}, []string{}, make(map[string]Compare), make(map[string]bool))
	t.Log("Grouping applied")

	t.Log("\n=== Reading First 100 Rows ===")
	indices := tableView.GetFilteredIndices()
	limit := 100
	if len(indices) < limit {
		limit = len(indices)
	}
	for j := 0; j < limit; j++ {
		idx := indices[j]
		for _, colName := range tableView.GetColumnNames() {
			col := tableView.GetColumn(colName)
			if col != nil {
				_, _ = col.GetString(idx)
			}
		}
	}
	t.Logf("Read %d rows with %d columns each", limit, len(tableView.GetColumnNames()))
}

// BenchmarkGroupTable_TopK_1M_1MGroups compares full sort vs top-K for 1M groups
func BenchmarkGroupTable_TopK_1M_1MGroups(b *testing.B) {
	// Create table with 1M unique values (worst case for grouping: 1M groups)
	numRows := 1_000_000
	table := NewDataTable()
	colDef := columns.NewColumnDef("value", "Value", "")
	col := columns.NewUint32Column(colDef)
	for i := 0; i < numRows; i++ {
		col.Append(uint32(i)) // Each row has unique value
	}
	col.FinalizeColumn()
	table.AddColumn(col)

	b.Run("FullSort_NoLimit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tableView := NewTableView(table, "bench")
			tableView.VisibleColumns = []string{"value"}
			// Use GroupTable (no limit - sorts all 1M groups)
			tableView.GroupTable([]string{"value"}, []string{}, make(map[string]Compare), make(map[string]bool))
		}
	})

	b.Run("TopK_100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tableView := NewTableView(table, "bench")
			tableView.VisibleColumns = []string{"value"}
			// Use GroupTableWithLimit - only keep top 100 groups
			tableView.GroupTableWithLimit([]string{"value"}, []string{}, make(map[string]Compare), make(map[string]bool), 100)
		}
	})

	b.Run("TopK_1000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tableView := NewTableView(table, "bench")
			tableView.VisibleColumns = []string{"value"}
			// Use GroupTableWithLimit - only keep top 1000 groups
			tableView.GroupTableWithLimit([]string{"value"}, []string{}, make(map[string]Compare), make(map[string]bool), 1000)
		}
	})
}

// BenchmarkGroupTable_Breakdown_TopK isolates each phase to understand time distribution
func BenchmarkGroupTable_Breakdown_TopK(b *testing.B) {
	numRows := 1_000_000
	table := NewDataTable()
	colDef := columns.NewColumnDef("value", "Value", "")
	col := columns.NewUint32Column(colDef)
	for i := 0; i < numRows; i++ {
		col.Append(uint32(i))
	}
	col.FinalizeColumn()
	table.AddColumn(col)

	// Pre-create indices (simulating what GroupTable does)
	indices := make([]uint32, numRows)
	for i := 0; i < numRows; i++ {
		indices[i] = uint32(i)
	}

	b.Run("1_GroupIndices_Only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col.GroupIndices(indices, nil)
		}
	})

	b.Run("2a_MapIteration_Only", func(b *testing.B) {
		groupedIndices, _ := col.GroupIndices(indices, nil)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			count := 0
			for range groupedIndices {
				count++
			}
			_ = count
		}
	})

	b.Run("2b_MapIteration+StructAlloc", func(b *testing.B) {
		groupedIndices, _ := col.GroupIndices(indices, nil)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for groupKey, groupIndices := range groupedIndices {
				g := &grouping.Group{
					GroupKey:   groupKey,
					Indices:    groupIndices,
					IsComplete: true,
				}
				_ = g
			}
		}
	})

	b.Run("2c_MapIteration+StructAlloc+SliceAppend", func(b *testing.B) {
		groupedIndices, _ := col.GroupIndices(indices, nil)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			groups := make([]*grouping.Group, 0, len(groupedIndices))
			for groupKey, groupIndices := range groupedIndices {
				g := &grouping.Group{
					GroupKey:   groupKey,
					Indices:    groupIndices,
					IsComplete: true,
				}
				groups = append(groups, g)
			}
			_ = groups
		}
	})

	b.Run("2_GroupIndices+Structs", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			groupedIndices, _ := col.GroupIndices(indices, nil)
			// Create Group structs (like groupFirstColumnInTable does)
			groups := make([]*grouping.Group, 0, len(groupedIndices))
			for groupKey, groupIndices := range groupedIndices {
				g := &grouping.Group{
					GroupKey:   groupKey,
					Indices:    groupIndices,
					IsComplete: true,
				}
				groups = append(groups, g)
			}
			_ = groups
		}
	})

	b.Run("3_GroupIndices+Structs+FullSort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			groupedIndices, _ := col.GroupIndices(indices, nil)
			groups := make([]*grouping.Group, 0, len(groupedIndices))
			for groupKey, groupIndices := range groupedIndices {
				g := &grouping.Group{
					GroupKey:   groupKey,
					Indices:    groupIndices,
					IsComplete: true,
				}
				groups = append(groups, g)
			}
			// Full sort (like sortGroupsInBlock does)
			sort.Slice(groups, func(i, j int) bool {
				return columns.CompareAtIndex(col, groups[i].Indices[0], groups[j].Indices[0]) < 0
			})
		}
	})

	b.Run("4_GroupIndices+Structs+TopK100", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			groupedIndices, _ := col.GroupIndices(indices, nil)
			groups := make([]*grouping.Group, 0, len(groupedIndices))
			for groupKey, groupIndices := range groupedIndices {
				g := &grouping.Group{
					GroupKey:   groupKey,
					Indices:    groupIndices,
					IsComplete: true,
				}
				groups = append(groups, g)
			}
			// TopK: scan all groups, compare each with threshold
			limit := 100
			for j := limit; j < len(groups); j++ {
				// One comparison per element (checking against heap top)
				_ = columns.CompareAtIndex(col, groups[j].Indices[0], groups[0].Indices[0])
			}
			// Final sort of K elements
			topK := groups[:limit]
			sort.Slice(topK, func(i, j int) bool {
				return columns.CompareAtIndex(col, topK[i].Indices[0], topK[j].Indices[0]) < 0
			})
		}
	})
}
