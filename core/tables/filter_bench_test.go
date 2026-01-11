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

package tables

import (
	"fmt"
	"testing"

	"github.com/google/taxinomia/core/columns"
)

// BenchmarkFilterStringColumn benchmarks filtering on string columns
func BenchmarkFilterStringColumn(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("SubstringMatch_%d_rows", size), func(b *testing.B) {
			_, tableView := createStringBenchTable(size)
			filters := map[string]string{
				"status": "active", // Substring match (case-insensitive)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tableView.ApplyFilters(filters)
			}

			// Report rows per second
			rowsProcessed := int64(size) * int64(b.N)
			b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
		})

		b.Run(fmt.Sprintf("ExactMatch_%d_rows", size), func(b *testing.B) {
			_, tableView := createStringBenchTable(size)
			filters := map[string]string{
				"status": "\"Active\"", // Exact match (case-sensitive)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tableView.ApplyFilters(filters)
			}

			// Report rows per second
			rowsProcessed := int64(size) * int64(b.N)
			b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
		})

		b.Run(fmt.Sprintf("MultipleFilters_%d_rows", size), func(b *testing.B) {
			_, tableView := createMultiColumnBenchTable(size)
			filters := map[string]string{
				"status":   "active",
				"category": "electronics",
				"region":   "north",
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tableView.ApplyFilters(filters)
			}

			// Report rows per second
			rowsProcessed := int64(size) * int64(b.N)
			b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
		})
	}
}

// BenchmarkFilterUint32Column benchmarks filtering on numeric columns
func BenchmarkFilterUint32Column(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("ExactMatch_%d_rows", size), func(b *testing.B) {
			_, tableView := createUint32BenchTable(size)
			filters := map[string]string{
				"amount": "\"100\"", // Exact match
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tableView.ApplyFilters(filters)
			}

			// Report rows per second
			rowsProcessed := int64(size) * int64(b.N)
			b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
		})

		b.Run(fmt.Sprintf("SubstringMatch_%d_rows", size), func(b *testing.B) {
			_, tableView := createUint32BenchTable(size)
			filters := map[string]string{
				"amount": "10", // Substring match (matches 10, 100, 1000, etc.)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tableView.ApplyFilters(filters)
			}

			// Report rows per second
			rowsProcessed := int64(size) * int64(b.N)
			b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
		})
	}
}

// BenchmarkFilterMixedColumns benchmarks filtering on mixed string and numeric columns
func BenchmarkFilterMixedColumns(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("StringAndNumeric_%d_rows", size), func(b *testing.B) {
			_, tableView := createMixedBenchTable(size)
			filters := map[string]string{
				"status": "active",
				"amount": "100",
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tableView.ApplyFilters(filters)
			}

			// Report rows per second
			rowsProcessed := int64(size) * int64(b.N)
			b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
		})
	}
}

// BenchmarkFilterSelectivity benchmarks filters with different selectivity
func BenchmarkFilterSelectivity(b *testing.B) {
	size := 100000

	b.Run("HighlySelective_1percent", func(b *testing.B) {
		_, tableView := createSelectivityBenchTable(size, 100) // 1% match rate
		filters := map[string]string{
			"category": "rare",
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tableView.ApplyFilters(filters)
		}

		rowsProcessed := int64(size) * int64(b.N)
		b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
	})

	b.Run("ModerateSelective_50percent", func(b *testing.B) {
		_, tableView := createSelectivityBenchTable(size, 2) // 50% match rate
		filters := map[string]string{
			"category": "common",
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tableView.ApplyFilters(filters)
		}

		rowsProcessed := int64(size) * int64(b.N)
		b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
	})

	b.Run("LowSelective_90percent", func(b *testing.B) {
		_, tableView := createSelectivityBenchTable(size, 10) // ~90% match rate (9 of 10)
		filters := map[string]string{
			"category": "common",
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tableView.ApplyFilters(filters)
		}

		rowsProcessed := int64(size) * int64(b.N)
		b.ReportMetric(float64(rowsProcessed)/b.Elapsed().Seconds()/1e6, "Mrows/sec")
	})
}

// Helper functions to create benchmark tables

func createStringBenchTable(rows int) (*DataTable, *TableView) {
	table := NewDataTable()

	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	for i := 0; i < rows; i++ {
		if i%3 == 0 {
			statusCol.Append("Active")
		} else if i%3 == 1 {
			statusCol.Append("Inactive")
		} else {
			statusCol.Append("Pending")
		}
	}
	statusCol.FinalizeColumn()
	table.AddColumn(statusCol)

	return table, NewTableView(table, "bench_table")
}

func createUint32BenchTable(rows int) (*DataTable, *TableView) {
	table := NewDataTable()

	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	for i := 0; i < rows; i++ {
		amountCol.Append(uint32(i%1000 + 1))
	}
	amountCol.FinalizeColumn()
	table.AddColumn(amountCol)

	return table, NewTableView(table, "bench_table")
}

func createMultiColumnBenchTable(rows int) (*DataTable, *TableView) {
	table := NewDataTable()

	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", ""))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", ""))

	for i := 0; i < rows; i++ {
		// Status
		if i%3 == 0 {
			statusCol.Append("Active")
		} else if i%3 == 1 {
			statusCol.Append("Inactive")
		} else {
			statusCol.Append("Pending")
		}

		// Category
		if i%4 == 0 {
			categoryCol.Append("Electronics")
		} else if i%4 == 1 {
			categoryCol.Append("Clothing")
		} else if i%4 == 2 {
			categoryCol.Append("Food")
		} else {
			categoryCol.Append("Books")
		}

		// Region
		if i%2 == 0 {
			regionCol.Append("North")
		} else {
			regionCol.Append("South")
		}
	}

	statusCol.FinalizeColumn()
	categoryCol.FinalizeColumn()
	regionCol.FinalizeColumn()

	table.AddColumn(statusCol)
	table.AddColumn(categoryCol)
	table.AddColumn(regionCol)

	return table, NewTableView(table, "bench_table")
}

func createMixedBenchTable(rows int) (*DataTable, *TableView) {
	table := NewDataTable()

	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))

	for i := 0; i < rows; i++ {
		if i%3 == 0 {
			statusCol.Append("Active")
		} else if i%3 == 1 {
			statusCol.Append("Inactive")
		} else {
			statusCol.Append("Pending")
		}

		amountCol.Append(uint32(i%1000 + 1))
	}

	statusCol.FinalizeColumn()
	amountCol.FinalizeColumn()

	table.AddColumn(statusCol)
	table.AddColumn(amountCol)

	return table, NewTableView(table, "bench_table")
}

func createSelectivityBenchTable(rows int, cardinality int) (*DataTable, *TableView) {
	table := NewDataTable()

	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", ""))

	for i := 0; i < rows; i++ {
		if i%cardinality == 0 {
			categoryCol.Append("rare")
		} else {
			categoryCol.Append("common")
		}
	}

	categoryCol.FinalizeColumn()
	table.AddColumn(categoryCol)

	return table, NewTableView(table, "bench_table")
}
