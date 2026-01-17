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

//go:embed data/orders.textproto
var ordersTextproto string

//go:embed data/regions.csv
var regionsCSV string

//go:embed data/regions.textproto
var regionsTextproto string

//go:embed data/capitals.csv
var capitalsCSV string

//go:embed data/capitals.textproto
var capitalsTextproto string

//go:embed data/items.csv
var itemsCSV string

//go:embed data/items.textproto
var itemsTextproto string

// importTable is a helper function to import a CSV table with its textproto annotation
func importTable(name, csv, textproto string) *tables.DataTable {
	options, err := csvimport.OptionsFromTextproto(textproto)
	if err != nil {
		panic(fmt.Sprintf("failed to parse %s textproto: %v", name, err))
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
	return importTable("Orders", ordersCSV, ordersTextproto)
}

// CreateRegionsTable creates and populates a table with region information from embedded CSV
func CreateRegionsTable() *tables.DataTable {
	return importTable("Regions", regionsCSV, regionsTextproto)
}

// CreateCapitalsTable creates and populates a table with capital city information from embedded CSV
func CreateCapitalsTable() *tables.DataTable {
	return importTable("Capitals", capitalsCSV, capitalsTextproto)
}

// CreateItemsTable creates and populates a table with item/category information from embedded CSV
func CreateItemsTable() *tables.DataTable {
	return importTable("Items", itemsCSV, itemsTextproto)
}
