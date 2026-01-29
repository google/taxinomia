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
	"strings"
	"testing"
)

func TestImportBasicCSV(t *testing.T) {
	csvData := `name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`

	reader := strings.NewReader(csvData)
	table, err := ImportFromReader(reader, DefaultOptions())
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	// Check table length
	if table.Length() != 3 {
		t.Errorf("expected 3 rows, got %d", table.Length())
	}

	// Check columns exist
	names := table.GetColumnNames()
	if len(names) != 3 {
		t.Errorf("expected 3 columns, got %d", len(names))
	}

	// Check name column (string)
	nameCol := table.GetColumn("name")
	if nameCol == nil {
		t.Fatal("name column not found")
	}
	val, err := nameCol.GetString(0)
	if err != nil || val != "Alice" {
		t.Errorf("expected 'Alice', got '%s'", val)
	}

	// Check age column (should be uint32)
	ageCol := table.GetColumn("age")
	if ageCol == nil {
		t.Fatal("age column not found")
	}
	ageVal, err := ageCol.GetString(0)
	if err != nil || ageVal != "30" {
		t.Errorf("expected '30', got '%s'", ageVal)
	}
}

func TestImportWithoutHeader(t *testing.T) {
	csvData := `Alice,30,New York
Bob,25,Los Angeles`

	reader := strings.NewReader(csvData)
	options := DefaultOptions()
	options.HasHeader = false

	table, err := ImportFromReader(reader, options)
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	// Check auto-generated column names
	col1 := table.GetColumn("column_1")
	if col1 == nil {
		t.Fatal("column_1 not found")
	}

	val, err := col1.GetString(0)
	if err != nil || val != "Alice" {
		t.Errorf("expected 'Alice', got '%s'", val)
	}
}

func TestImportWithCsvColumnSource(t *testing.T) {
	csvData := `id,region,amount
1,North,100
2,South,200`

	reader := strings.NewReader(csvData)
	options := DefaultOptions()
	options.ColumnSources = map[string]CsvColumnSource{
		"region": {
			EntityType: "region",
		},
		"id": {
			DisplayName: "Order ID",
			Type:        CsvColumnTypeString, // Force string even though it looks numeric
		},
	}

	table, err := ImportFromReader(reader, options)
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	// Check region column has entity type (by checking it exists)
	regionCol := table.GetColumn("region")
	if regionCol == nil {
		t.Fatal("region column not found")
	}

	// Check ID is string (forced)
	idCol := table.GetColumn("id")
	if idCol == nil {
		t.Fatal("id column not found")
	}
	// String columns return the value directly
	idVal, err := idCol.GetString(0)
	if err != nil || idVal != "1" {
		t.Errorf("expected '1', got '%s'", idVal)
	}
}

func TestImportWithDelimiter(t *testing.T) {
	csvData := `name;age;city
Alice;30;New York
Bob;25;Los Angeles`

	reader := strings.NewReader(csvData)
	options := DefaultOptions()
	options.Delimiter = ';'

	table, err := ImportFromReader(reader, options)
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	if table.Length() != 2 {
		t.Errorf("expected 2 rows, got %d", table.Length())
	}

	nameCol := table.GetColumn("name")
	if nameCol == nil {
		t.Fatal("name column not found")
	}
}

func TestImportEmptyCSV(t *testing.T) {
	csvData := ``

	reader := strings.NewReader(csvData)
	_, err := ImportFromReader(reader, DefaultOptions())
	if err == nil {
		t.Error("expected error for empty CSV")
	}
}

func TestImportHeaderOnly(t *testing.T) {
	csvData := `name,age,city`

	reader := strings.NewReader(csvData)
	_, err := ImportFromReader(reader, DefaultOptions())
	if err == nil {
		t.Error("expected error for header-only CSV")
	}
}

func TestImportMixedNumericString(t *testing.T) {
	csvData := `code,value
ABC,100
DEF,200
123,300`

	reader := strings.NewReader(csvData)
	table, err := ImportFromReader(reader, DefaultOptions())
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	// Code should be string (has non-numeric values)
	codeCol := table.GetColumn("code")
	if codeCol == nil {
		t.Fatal("code column not found")
	}
	codeVal, _ := codeCol.GetString(0)
	if codeVal != "ABC" {
		t.Errorf("expected 'ABC', got '%s'", codeVal)
	}

	// Value should be uint32 (all numeric)
	valueCol := table.GetColumn("value")
	if valueCol == nil {
		t.Fatal("value column not found")
	}
	valueVal, _ := valueCol.GetString(0)
	if valueVal != "100" {
		t.Errorf("expected '100', got '%s'", valueVal)
	}
}

func TestImportWithEmptyValues(t *testing.T) {
	csvData := `name,count
Alice,10
Bob,
Charlie,20`

	reader := strings.NewReader(csvData)
	table, err := ImportFromReader(reader, DefaultOptions())
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	countCol := table.GetColumn("count")
	if countCol == nil {
		t.Fatal("count column not found")
	}

	// Empty value should be 0 for uint32
	countVal, _ := countCol.GetString(1)
	if countVal != "0" {
		t.Errorf("expected '0' for empty numeric, got '%s'", countVal)
	}
}
