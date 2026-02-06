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

package datasources

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// CsvLoader implements DataSourceLoader for CSV files.
// All columns are loaded as strings.
//
// Required config keys:
//   - file_path: Path to the CSV file
//
// Optional config keys:
//   - has_header: "true" or "false" (default: "true")
//   - delimiter: Field delimiter (default: ",")
type CsvLoader struct{}

// NewCsvLoader creates a new CSV loader.
func NewCsvLoader() *CsvLoader {
	return &CsvLoader{}
}

// SourceType returns "csv".
func (l *CsvLoader) SourceType() string {
	return "csv"
}

// DiscoverSchema discovers the table schema from the CSV header.
// All columns are typed as string.
func (l *CsvLoader) DiscoverSchema(config map[string]string) (*TableSchema, error) {
	filePath := config["file_path"]
	if filePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}

	hasHeader := true
	if h := config["has_header"]; h == "false" {
		hasHeader = false
	}

	delimiter := ','
	if d := config["delimiter"]; d != "" && len(d) > 0 {
		delimiter = rune(d[0])
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = delimiter

	// Read first row to get column names
	firstRow, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	// Determine column names
	var columnNames []string
	if hasHeader {
		columnNames = firstRow
	} else {
		// Generate column names: col_0, col_1, etc.
		for i := range firstRow {
			columnNames = append(columnNames, fmt.Sprintf("col_%d", i))
		}
	}

	// Build schema (all string columns)
	schema := &TableSchema{
		Columns: make([]*ColumnSchema, len(columnNames)),
	}
	for i, name := range columnNames {
		schema.Columns[i] = &ColumnSchema{
			Name: name,
			Type: TypeString,
		}
	}

	return schema, nil
}

// Load loads a CSV file and returns a DataTable.
func (l *CsvLoader) Load(config map[string]string, enrichedColumns []*EnrichedColumn) (*tables.DataTable, error) {
	filePath := config["file_path"]
	if filePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}

	hasHeader := true
	if h := config["has_header"]; h == "false" {
		hasHeader = false
	}

	delimiter := ','
	if d := config["delimiter"]; d != "" && len(d) > 0 {
		delimiter = rune(d[0])
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = delimiter

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Determine data start
	dataStart := 0
	if hasHeader {
		dataStart = 1
	}

	// Create data table
	table := tables.NewDataTable()

	// Create columns (all string columns for CSV)
	stringCols := make([]*columns.StringColumn, len(enrichedColumns))
	for i, enriched := range enrichedColumns {
		colDef := columns.NewColumnDef(enriched.Name, enriched.DisplayName, enriched.EntityType)
		stringCols[i] = columns.NewStringColumn(colDef)
	}

	// Populate data
	for _, record := range records[dataStart:] {
		for i, col := range stringCols {
			if i < len(record) {
				col.Append(record[i])
			} else {
				col.Append("")
			}
		}
	}

	// Add columns to table
	for _, col := range stringCols {
		table.AddColumn(col)
	}

	return table, nil
}

// CsvLoaderTyped is a CSV loader that infers column types from data.
// It samples data to determine if columns are int, float, bool, or string.
//
// Required config keys:
//   - file_path: Path to the CSV file
//
// Optional config keys:
//   - has_header: "true" or "false" (default: "true")
//   - delimiter: Field delimiter (default: ",")
type CsvLoaderTyped struct{}

// NewCsvLoaderTyped creates a new typed CSV loader.
func NewCsvLoaderTyped() *CsvLoaderTyped {
	return &CsvLoaderTyped{}
}

// SourceType returns "csv_typed".
func (l *CsvLoaderTyped) SourceType() string {
	return "csv_typed"
}

// DiscoverSchema discovers the table schema by sampling CSV data.
// Column types are inferred from the data.
func (l *CsvLoaderTyped) DiscoverSchema(config map[string]string) (*TableSchema, error) {
	filePath := config["file_path"]
	if filePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}

	hasHeader := true
	if h := config["has_header"]; h == "false" {
		hasHeader = false
	}

	delimiter := ','
	if d := config["delimiter"]; d != "" && len(d) > 0 {
		delimiter = rune(d[0])
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = delimiter

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Determine column names
	var columnNames []string
	dataStart := 0

	if hasHeader {
		columnNames = records[0]
		dataStart = 1
	} else {
		for i := range records[0] {
			columnNames = append(columnNames, fmt.Sprintf("col_%d", i))
		}
	}

	dataRecords := records[dataStart:]
	if len(dataRecords) == 0 {
		// No data, default all to string
		schema := &TableSchema{
			Columns: make([]*ColumnSchema, len(columnNames)),
		}
		for i, name := range columnNames {
			schema.Columns[i] = &ColumnSchema{
				Name: name,
				Type: TypeString,
			}
		}
		return schema, nil
	}

	// Infer column types by sampling data
	colTypes := l.inferColumnTypes(columnNames, dataRecords)

	// Build schema
	schema := &TableSchema{
		Columns: make([]*ColumnSchema, len(columnNames)),
	}
	for i, name := range columnNames {
		schema.Columns[i] = &ColumnSchema{
			Name: name,
			Type: colTypes[i],
		}
	}

	return schema, nil
}

// Load loads a CSV file with typed columns.
func (l *CsvLoaderTyped) Load(config map[string]string, enrichedColumns []*EnrichedColumn) (*tables.DataTable, error) {
	filePath := config["file_path"]
	if filePath == "" {
		return nil, fmt.Errorf("file_path is required")
	}

	hasHeader := true
	if h := config["has_header"]; h == "false" {
		hasHeader = false
	}

	delimiter := ','
	if d := config["delimiter"]; d != "" && len(d) > 0 {
		delimiter = rune(d[0])
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	reader.Comma = delimiter

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Determine data start
	dataStart := 0
	if hasHeader {
		dataStart = 1
	}

	dataRecords := records[dataStart:]
	if len(dataRecords) == 0 {
		return nil, fmt.Errorf("CSV file has no data rows")
	}

	// Create data table
	table := tables.NewDataTable()

	// Create and populate columns based on enriched types
	for i, enriched := range enrichedColumns {
		colDef := columns.NewColumnDef(enriched.Name, enriched.DisplayName, enriched.EntityType)

		switch enriched.Type {
		case TypeInt64:
			col := columns.NewInt64Column(colDef)
			for _, record := range dataRecords {
				if i < len(record) {
					if v, err := strconv.ParseInt(record[i], 10, 64); err == nil {
						col.Append(v)
					} else {
						col.Append(0)
					}
				} else {
					col.Append(0)
				}
			}
			table.AddColumn(col)

		case TypeFloat64:
			col := columns.NewFloat64Column(colDef)
			for _, record := range dataRecords {
				if i < len(record) {
					if v, err := strconv.ParseFloat(record[i], 64); err == nil {
						col.Append(v)
					} else {
						col.Append(0.0)
					}
				} else {
					col.Append(0.0)
				}
			}
			table.AddColumn(col)

		case TypeBool:
			col := columns.NewBoolColumn(colDef)
			for _, record := range dataRecords {
				if i < len(record) {
					v := record[i] == "true" || record[i] == "1" || record[i] == "yes"
					col.Append(v)
				} else {
					col.Append(false)
				}
			}
			table.AddColumn(col)

		default: // TypeString
			col := columns.NewStringColumn(colDef)
			for _, record := range dataRecords {
				if i < len(record) {
					col.Append(record[i])
				} else {
					col.Append("")
				}
			}
			table.AddColumn(col)
		}
	}

	return table, nil
}

// inferColumnTypes samples data to determine column types.
func (l *CsvLoaderTyped) inferColumnTypes(columnNames []string, records [][]string) []ColumnType {
	types := make([]ColumnType, len(columnNames))

	for i := range columnNames {
		types[i] = l.inferColumnType(i, records)
	}

	return types
}

func (l *CsvLoaderTyped) inferColumnType(colIdx int, records [][]string) ColumnType {
	// Sample up to 100 rows
	sampleSize := len(records)
	if sampleSize > 100 {
		sampleSize = 100
	}

	isInt := true
	isFloat := true
	isBool := true

	for i := 0; i < sampleSize; i++ {
		if colIdx >= len(records[i]) {
			continue
		}
		val := records[i][colIdx]
		if val == "" {
			continue // Skip empty values
		}

		// Check int
		if isInt {
			if _, err := strconv.ParseInt(val, 10, 64); err != nil {
				isInt = false
			}
		}

		// Check float
		if isFloat {
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				isFloat = false
			}
		}

		// Check bool
		if isBool {
			if val != "true" && val != "false" && val != "0" && val != "1" && val != "yes" && val != "no" {
				isBool = false
			}
		}
	}

	if isInt {
		return TypeInt64
	}
	if isFloat {
		return TypeFloat64
	}
	if isBool {
		return TypeBool
	}
	return TypeString
}
