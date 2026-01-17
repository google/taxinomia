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
	"os"
	"strconv"
	"strings"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// ColumnType specifies the data type for a column
type ColumnType int

const (
	// ColumnTypeAuto auto-detects type from data (default)
	ColumnTypeAuto ColumnType = iota
	// ColumnTypeString forces string type
	ColumnTypeString
	// ColumnTypeUint32 forces uint32 type
	ColumnTypeUint32
)

// ColumnAnnotation defines annotations for how a column is imported
type ColumnAnnotation struct {
	// Name is the column name (defaults to header name if not specified)
	Name string
	// DisplayName is the display name for the column
	DisplayName string
	// EntityType is the entity type for join support (e.g., "region", "category")
	EntityType string
	// Type specifies the data type for this column (default: auto-detect)
	Type ColumnType
}

// ImportOptions configures CSV import behavior
type ImportOptions struct {
	// HasHeader indicates whether the first row contains column headers
	HasHeader bool
	// Delimiter is the field delimiter (defaults to comma)
	Delimiter rune
	// ColumnAnnotations provides configuration for specific columns by header name
	ColumnAnnotations map[string]ColumnAnnotation
	// SampleSize is the number of rows to sample for type detection (default: 100)
	SampleSize int
}

// DefaultOptions returns default import options
func DefaultOptions() ImportOptions {
	return ImportOptions{
		HasHeader:     true,
		Delimiter:     ',',
		ColumnAnnotations: make(map[string]ColumnAnnotation),
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
	columnTypes := detectColumnTypes(headers, dataRows, sampleSize, options.ColumnAnnotations)

	// Create the table
	table := tables.NewDataTable()

	// Create columns based on detected types
	stringCols := make(map[int]*columns.StringColumn)
	uint32Cols := make(map[int]*columns.Uint32Column)

	for i, header := range headers {
		config := getColumnAnnotation(header, options.ColumnAnnotations)
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

		if columnTypes[i] == "uint32" && config.Type != ColumnTypeString {
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

	return table, nil
}

// detectColumnTypes samples data to determine if columns are numeric or string
func detectColumnTypes(headers []string, dataRows [][]string, sampleSize int, configs map[string]ColumnAnnotation) []string {
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
			case ColumnTypeString:
				types[i] = "string"
				continue
			case ColumnTypeUint32:
				types[i] = "uint32"
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

// getColumnAnnotation returns the config for a column, or an empty config if not specified
func getColumnAnnotation(header string, configs map[string]ColumnAnnotation) ColumnAnnotation {
	if configs == nil {
		return ColumnAnnotation{}
	}
	if config, ok := configs[header]; ok {
		return config
	}
	return ColumnAnnotation{}
}
