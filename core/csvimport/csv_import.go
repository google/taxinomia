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

package csvimport

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// CsvColumnType specifies the data type for a column
type CsvColumnType int

const (
	// CsvColumnTypeAuto auto-detects type from data (default)
	CsvColumnTypeAuto CsvColumnType = iota
	// CsvColumnTypeString forces string type
	CsvColumnTypeString
	// CsvColumnTypeUint32 forces uint32 type
	CsvColumnTypeUint32
	// CsvColumnTypeBool forces bool type
	CsvColumnTypeBool
	// CsvColumnTypeFloat64 forces float64 type
	CsvColumnTypeFloat64
	// CsvColumnTypeInt64 forces int64 type
	CsvColumnTypeInt64
	// CsvColumnTypeUint64 forces uint64 type
	CsvColumnTypeUint64
)

// CsvColumnSource defines source metadata for how a column is imported
type CsvColumnSource struct {
	// Name is the column name (defaults to header name if not specified)
	Name string
	// DisplayName is the display name for the column
	DisplayName string
	// EntityType is the entity type for join support (e.g., "region", "category")
	EntityType string
	// Type specifies the data type for this column (default: auto-detect)
	Type CsvColumnType
}

// ImportOptions configures CSV import behavior
type ImportOptions struct {
	// HasHeader indicates whether the first row contains column headers
	HasHeader bool
	// Delimiter is the field delimiter (defaults to comma)
	Delimiter rune
	// ColumnSources provides configuration for specific columns by header name
	ColumnSources map[string]CsvColumnSource
	// SampleSize is the number of rows to sample for type detection (default: 100)
	SampleSize int
}

// DefaultOptions returns default import options
func DefaultOptions() ImportOptions {
	return ImportOptions{
		HasHeader:     true,
		Delimiter:     ',',
		ColumnSources: make(map[string]CsvColumnSource),
		SampleSize:    100,
	}
}

// ImportFromFile imports a CSV file and returns a DataTable
func ImportFromFile(filepath string, options ImportOptions) (*tables.DataTable, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	return ImportFromReader(file, options)
}

// ImportFromReader imports CSV data from an io.Reader and returns a DataTable
func ImportFromReader(reader io.Reader, options ImportOptions) (*tables.DataTable, error) {
	csvReader := csv.NewReader(reader)
	if options.Delimiter != 0 {
		csvReader.Comma = options.Delimiter
	}

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Extract headers
	var headers []string
	var dataRows [][]string

	if options.HasHeader {
		headers = records[0]
		dataRows = records[1:]
	} else {
		// Generate column names if no header
		numCols := len(records[0])
		headers = make([]string, numCols)
		for i := 0; i < numCols; i++ {
			headers[i] = fmt.Sprintf("column_%d", i+1)
		}
		dataRows = records
	}

	if len(dataRows) == 0 {
		return nil, fmt.Errorf("CSV file has no data rows")
	}

	// Detect column types
	sampleSize := options.SampleSize
	if sampleSize <= 0 {
		sampleSize = 100
	}
	columnTypes := detectColumnTypes(headers, dataRows, sampleSize, options.ColumnSources)

	// Create the table
	table := tables.NewDataTable()

	// Create columns based on detected types
	stringCols := make(map[int]*columns.StringColumn)
	uint32Cols := make(map[int]*columns.Uint32Column)
	boolCols := make(map[int]*columns.BoolColumn)
	float64Cols := make(map[int]*columns.Float64Column)
	int64Cols := make(map[int]*columns.Int64Column)
	uint64Cols := make(map[int]*columns.Uint64Column)

	for i, header := range headers {
		config := getColumnSource(header, options.ColumnSources)
		name := header
		displayName := header
		entityType := ""

		if config.Name != "" {
			name = config.Name
		}
		if config.DisplayName != "" {
			displayName = config.DisplayName
		}
		if config.EntityType != "" {
			entityType = config.EntityType
		}

		colDef := columns.NewColumnDef(name, displayName, entityType)

		if columnTypes[i] == "bool" || config.Type == CsvColumnTypeBool {
			col := columns.NewBoolColumn(colDef)
			boolCols[i] = col
			table.AddColumn(col)
		} else if columnTypes[i] == "float64" || config.Type == CsvColumnTypeFloat64 {
			col := columns.NewFloat64Column(colDef)
			float64Cols[i] = col
			table.AddColumn(col)
		} else if columnTypes[i] == "int64" || config.Type == CsvColumnTypeInt64 {
			col := columns.NewInt64Column(colDef)
			int64Cols[i] = col
			table.AddColumn(col)
		} else if columnTypes[i] == "uint64" || config.Type == CsvColumnTypeUint64 {
			col := columns.NewUint64Column(colDef)
			uint64Cols[i] = col
			table.AddColumn(col)
		} else if columnTypes[i] == "uint32" && config.Type != CsvColumnTypeString {
			col := columns.NewUint32Column(colDef)
			uint32Cols[i] = col
			table.AddColumn(col)
		} else {
			col := columns.NewStringColumn(colDef)
			stringCols[i] = col
			table.AddColumn(col)
		}
	}

	// Populate the columns
	for _, row := range dataRows {
		for i := range headers {
			value := ""
			if i < len(row) {
				value = strings.TrimSpace(row[i])
			}

			if col, ok := stringCols[i]; ok {
				col.Append(value)
			} else if col, ok := uint32Cols[i]; ok {
				// Parse as uint32
				if value == "" {
					col.Append(0)
				} else {
					n, err := strconv.ParseUint(value, 10, 32)
					if err != nil {
						// Fallback: treat as 0 if parsing fails
						col.Append(0)
					} else {
						col.Append(uint32(n))
					}
				}
			} else if col, ok := float64Cols[i]; ok {
				// Parse as float64
				if value == "" {
					col.Append(0)
				} else {
					f, err := strconv.ParseFloat(value, 64)
					if err != nil {
						// Fallback: treat as NaN if parsing fails
						col.Append(math.NaN())
					} else {
						col.Append(f)
					}
				}
			} else if col, ok := boolCols[i]; ok {
				// Parse as bool
				b, err := columns.ParseBool(value)
				if err != nil {
					// Fallback: treat as false if parsing fails
					col.Append(false)
				} else {
					col.Append(b)
				}
			} else if col, ok := int64Cols[i]; ok {
				// Parse as int64
				if value == "" {
					col.Append(0)
				} else {
					n, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						// Fallback: treat as 0 if parsing fails
						col.Append(0)
					} else {
						col.Append(n)
					}
				}
			} else if col, ok := uint64Cols[i]; ok {
				// Parse as uint64
				if value == "" {
					col.Append(0)
				} else {
					n, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						// Fallback: treat as 0 if parsing fails
						col.Append(0)
					} else {
						col.Append(n)
					}
				}
			}
		}
	}

	// Finalize columns
	for _, col := range stringCols {
		col.FinalizeColumn()
	}
	for _, col := range uint32Cols {
		col.FinalizeColumn()
	}
	for _, col := range boolCols {
		col.FinalizeColumn()
	}
	for _, col := range float64Cols {
		col.FinalizeColumn()
	}
	for _, col := range int64Cols {
		col.FinalizeColumn()
	}
	for _, col := range uint64Cols {
		col.FinalizeColumn()
	}

	return table, nil
}

// detectColumnTypes samples data to determine if columns are numeric or string
func detectColumnTypes(headers []string, dataRows [][]string, sampleSize int, configs map[string]CsvColumnSource) []string {
	types := make([]string, len(headers))

	// Sample rows for type detection
	rowsToSample := sampleSize
	if rowsToSample > len(dataRows) {
		rowsToSample = len(dataRows)
	}

	for i, header := range headers {
		// Check if type is explicitly set
		if config, ok := configs[header]; ok {
			switch config.Type {
			case CsvColumnTypeString:
				types[i] = "string"
				continue
			case CsvColumnTypeUint32:
				types[i] = "uint32"
				continue
			case CsvColumnTypeBool:
				types[i] = "bool"
				continue
			case CsvColumnTypeFloat64:
				types[i] = "float64"
				continue
			case CsvColumnTypeInt64:
				types[i] = "int64"
				continue
			case CsvColumnTypeUint64:
				types[i] = "uint64"
				continue
			}
		}

		// Check if all sampled values are valid uint32
		isUint32 := true
		hasNonEmpty := false

		for j := 0; j < rowsToSample; j++ {
			if i >= len(dataRows[j]) {
				continue
			}

			value := strings.TrimSpace(dataRows[j][i])
			if value == "" {
				continue
			}

			hasNonEmpty = true

			// Try to parse as uint32
			n, err := strconv.ParseUint(value, 10, 32)
			if err != nil || n > 4294967295 {
				isUint32 = false
				break
			}
		}

		if isUint32 && hasNonEmpty {
			types[i] = "uint32"
		} else {
			types[i] = "string"
		}
	}

	return types
}

// getColumnSource returns the config for a column, or an empty config if not specified
func getColumnSource(header string, configs map[string]CsvColumnSource) CsvColumnSource {
	if configs == nil {
		return CsvColumnSource{}
	}
	if config, ok := configs[header]; ok {
		return config
	}
	return CsvColumnSource{}
}
