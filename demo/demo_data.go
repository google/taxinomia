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

package demo

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/google/taxinomia/core/csvimport"
	"github.com/google/taxinomia/core/tables"
)

//go:embed data/orders.csv
var ordersCSV string

//go:embed data/regions.csv
var regionsCSV string

//go:embed data/capitals.csv
var capitalsCSV string

//go:embed data/items.csv
var itemsCSV string

//go:embed data/annotations.textproto
var annotations string

var tableOptions map[string]csvimport.ImportOptions

func init() {
	var err error
	tableOptions, err = csvimport.OptionsMapFromTextproto(annotations)
	if err != nil {
		panic(fmt.Sprintf("failed to parse annotations: %v", err))
	}
}

// importTable is a helper function to import a CSV table using pre-parsed annotations
func importTable(name, csv string) *tables.DataTable {
	options, ok := tableOptions[name]
	if !ok {
		panic(fmt.Sprintf("no annotations found for table %s", name))
	}

	table, err := csvimport.ImportFromReader(strings.NewReader(csv), options)
	if err != nil {
		panic(fmt.Sprintf("failed to import %s CSV: %v", name, err))
	}

	fmt.Printf("\n%s Data: %d rows imported from CSV\n", name, table.Length())
	return table
}

// CreateDemoTable creates and populates a demo table with sample order data from embedded CSV
func CreateDemoTable() *tables.DataTable {
	return importTable("orders", ordersCSV)
}

// CreateRegionsTable creates and populates a table with region information from embedded CSV
func CreateRegionsTable() *tables.DataTable {
	return importTable("regions", regionsCSV)
}

// CreateCapitalsTable creates and populates a table with capital city information from embedded CSV
func CreateCapitalsTable() *tables.DataTable {
	return importTable("capitals", capitalsCSV)
}

// CreateItemsTable creates and populates a table with item/category information from embedded CSV
func CreateItemsTable() *tables.DataTable {
	return importTable("items", itemsCSV)
}
