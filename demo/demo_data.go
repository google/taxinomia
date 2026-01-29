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
	"time"

	"github.com/google/taxinomia/core/columns"
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

//go:embed data/sales.csv
var salesCSV string

//go:embed data/sources.textproto
var sources string

var tableOptions map[string]csvimport.ImportOptions

func init() {
	var err error
	tableOptions, err = csvimport.OptionsMapFromTextproto(sources)
	if err != nil {
		panic(fmt.Sprintf("failed to parse sources: %v", err))
	}
}

// importTable is a helper function to import a CSV table using pre-parsed sources
func importTable(name, csv string) *tables.DataTable {
	options, ok := tableOptions[name]
	if !ok {
		panic(fmt.Sprintf("no sources found for table %s", name))
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

// CreateSalesTable creates a table with sales data using default auto-detection (no sources)
func CreateSalesTable() *tables.DataTable {
	table, err := csvimport.ImportFromReader(strings.NewReader(salesCSV), csvimport.DefaultOptions())
	if err != nil {
		panic(fmt.Sprintf("failed to import sales CSV: %v", err))
	}

	fmt.Printf("\nSales Data: %d rows imported from CSV (auto-detected types)\n", table.Length())
	return table
}

// CreateEventsTable creates a table with datetime columns for demonstrating datetime functionality
func CreateEventsTable() *tables.DataTable {
	fmt.Println("Creating events table with datetime columns...")

	t := tables.NewDataTable()

	// Create columns
	eventIDCol := columns.NewUint32Column(columns.NewColumnDef("event_id", "Event ID", "event_id"))
	eventNameCol := columns.NewStringColumn(columns.NewColumnDef("event_name", "Event Name", ""))
	eventTypeCol := columns.NewStringColumn(columns.NewColumnDef("event_type", "Event Type", ""))
	createdAtCol := columns.NewDatetimeColumn(columns.NewColumnDef("created_at", "Created At", ""))
	scheduledAtCol := columns.NewDatetimeColumn(columns.NewColumnDef("scheduled_at", "Scheduled At", ""))
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))

	t.AddColumn(eventIDCol)
	t.AddColumn(eventNameCol)
	t.AddColumn(eventTypeCol)
	t.AddColumn(createdAtCol)
	t.AddColumn(scheduledAtCol)
	t.AddColumn(statusCol)

	// Event types and statuses
	eventTypes := []string{"meeting", "webinar", "workshop", "conference", "training"}
	statuses := []string{"scheduled", "completed", "cancelled", "in_progress"}
	eventNames := []string{
		"Q1 Planning Session", "Product Demo", "Team Standup", "Customer Workshop",
		"Tech Talk", "Sales Review", "Design Sprint", "Code Review", "Strategy Meeting",
		"Launch Event", "Training Session", "Onboarding", "Retrospective", "Roadmap Review",
		"Budget Meeting", "Hackathon", "Town Hall", "1:1 Meeting", "Board Meeting", "AMA Session",
	}

	// Base time for generating realistic data
	baseTime := time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC)

	// Generate 50 events spread across 2 years
	for i := uint32(0); i < 50; i++ {
		eventIDCol.Append(i + 1)
		eventNameCol.Append(eventNames[i%uint32(len(eventNames))])
		eventTypeCol.Append(eventTypes[i%uint32(len(eventTypes))])

		// Created at: spread across 2 years, with some clustering
		createdOffset := time.Duration(i*7*24+i*3) * time.Hour // ~weekly spread with variation
		createdAt := baseTime.Add(createdOffset)
		createdAtCol.Append(createdAt)

		// Scheduled at: 1-14 days after creation
		scheduleOffset := time.Duration((i%14)+1) * 24 * time.Hour
		scheduledAt := createdAt.Add(scheduleOffset)
		scheduledAtCol.Append(scheduledAt)

		statusCol.Append(statuses[i%uint32(len(statuses))])
	}

	// Finalize columns
	eventIDCol.FinalizeColumn()
	eventNameCol.FinalizeColumn()
	eventTypeCol.FinalizeColumn()
	createdAtCol.FinalizeColumn()
	scheduledAtCol.FinalizeColumn()
	statusCol.FinalizeColumn()

	fmt.Printf("  Created %d events with datetime columns\n", 50)
	return t
}
