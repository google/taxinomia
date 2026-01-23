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

func TestParseTableSource(t *testing.T) {
	textproto := `
columns {
  name: "id"
  display_name: "ID"
  type: COLUMN_TYPE_STRING
}
columns {
  name: "region"
  display_name: "Region"
  entity_type: "region"
}
columns {
  name: "amount"
  display_name: "Amount ($)"
  type: COLUMN_TYPE_UINT32
}
`
	source, err := ParseTableSource(textproto)
	if err != nil {
		t.Fatalf("failed to parse textproto: %v", err)
	}

	if len(source.GetColumns()) != 3 {
		t.Errorf("expected 3 columns, got %d", len(source.GetColumns()))
	}

	// Check first column
	col0 := source.GetColumns()[0]
	if col0.GetName() != "id" {
		t.Errorf("expected name 'id', got '%s'", col0.GetName())
	}
	if col0.GetDisplayName() != "ID" {
		t.Errorf("expected display_name 'ID', got '%s'", col0.GetDisplayName())
	}

	// Check second column with entity type
	col1 := source.GetColumns()[1]
	if col1.GetEntityType() != "region" {
		t.Errorf("expected entity_type 'region', got '%s'", col1.GetEntityType())
	}
}

func TestColumnTypeConversion(t *testing.T) {
	textproto := `
columns {
  name: "id"
  type: COLUMN_TYPE_STRING
}
columns {
  name: "count"
  type: COLUMN_TYPE_UINT32
}
columns {
  name: "name"
}
`
	options, err := OptionsFromTextproto(textproto)
	if err != nil {
		t.Fatalf("failed to create options: %v", err)
	}

	idSource := options.ColumnSources["id"]
	if idSource.Type != ColumnTypeString {
		t.Errorf("expected ColumnTypeString, got %v", idSource.Type)
	}

	countSource := options.ColumnSources["count"]
	if countSource.Type != ColumnTypeUint32 {
		t.Errorf("expected ColumnTypeUint32, got %v", countSource.Type)
	}

	nameSource := options.ColumnSources["name"]
	if nameSource.Type != ColumnTypeAuto {
		t.Errorf("expected ColumnTypeAuto, got %v", nameSource.Type)
	}
}

func TestOptionsFromTextproto(t *testing.T) {
	textproto := `
columns {
  name: "name"
  display_name: "Full Name"
}
columns {
  name: "category"
  display_name: "Category"
  entity_type: "category"
}
`
	options, err := OptionsFromTextproto(textproto)
	if err != nil {
		t.Fatalf("failed to create options: %v", err)
	}

	if len(options.ColumnSources) != 2 {
		t.Errorf("expected 2 column sources, got %d", len(options.ColumnSources))
	}

	nameSource, ok := options.ColumnSources["name"]
	if !ok {
		t.Fatal("name column source not found")
	}
	if nameSource.DisplayName != "Full Name" {
		t.Errorf("expected display name 'Full Name', got '%s'", nameSource.DisplayName)
	}

	catSource, ok := options.ColumnSources["category"]
	if !ok {
		t.Fatal("category column source not found")
	}
	if catSource.EntityType != "category" {
		t.Errorf("expected entity type 'category', got '%s'", catSource.EntityType)
	}
}

func TestImportWithTextprotoSource(t *testing.T) {
	csvData := `name,region,amount
Alice,North,100
Bob,South,200`

	textproto := `
columns {
  name: "name"
  display_name: "Full Name"
}
columns {
  name: "region"
  display_name: "Region"
  entity_type: "region"
}
columns {
  name: "amount"
  display_name: "Amount ($)"
}
`
	options, err := OptionsFromTextproto(textproto)
	if err != nil {
		t.Fatalf("failed to create options: %v", err)
	}

	table, err := ImportFromReader(strings.NewReader(csvData), options)
	if err != nil {
		t.Fatalf("failed to import CSV: %v", err)
	}

	if table.Length() != 2 {
		t.Errorf("expected 2 rows, got %d", table.Length())
	}

	// Verify region column exists and has correct entity type via the column def
	regionCol := table.GetColumn("region")
	if regionCol == nil {
		t.Fatal("region column not found")
	}

	val, _ := regionCol.GetString(0)
	if val != "North" {
		t.Errorf("expected 'North', got '%s'", val)
	}
}
