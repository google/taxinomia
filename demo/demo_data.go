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
	"fmt"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// Row represents a single row of demo data
type Row struct {
	Status   string
	Region   string
	Category string
	Amount   int
}

// RegionInfo represents information about a region
type RegionInfo struct {
	Region     string
	Population int
	Area       float64 // in square km
	Capital    string
	TimeZone   string
	Currency   string
	Language   string
	GDP        float64 // in billions
}

// CapitalInfo represents information about a capital city
type CapitalInfo struct {
	Capital      string
	Region       string
	Population   float64 // in millions
	Founded      int     // year founded
	Elevation    int     // meters above sea level
	Latitude     float64
	Longitude    float64
	MayorName    string
	Universities int
	Museums      int
	Airports     int
}

// ItemInfo represents information about an item
type ItemInfo struct {
	ItemID      string
	ItemName    string
	Category    string
	Subcategory string
	Brand       string
	Price       float64
	Stock       int
	Weight      float64 // in kg
	Rating      float64 // 0-5 scale
	Reviews     int
	Supplier    string
}

// CreateDemoTable creates and populates a demo table with sample data
func CreateDemoTable() *tables.DataTable {
	t := tables.NewDataTable()

	// Create StringColumns for text fields
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", "region"))
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", "category"))
	// Use Uint32Column for numeric amount field
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))

	t.AddColumn(statusCol)
	t.AddColumn(regionCol)
	t.AddColumn(categoryCol)
	t.AddColumn(amountCol)

	// Create demo data and populate columns
	data := createDemoData()

	// Populate the columns
	for _, row := range data {
		statusCol.Append(row.Status)
		regionCol.Append(row.Region)
		categoryCol.Append(row.Category)
		amountCol.Append(uint32(row.Amount))
	}

	// Finalize columns to detect uniqueness and build indexes
	statusCol.FinalizeColumn()
	regionCol.FinalizeColumn()
	categoryCol.FinalizeColumn()
	amountCol.FinalizeColumn()

	// Print demo output
	fmt.Println("\nDemo Data:")
	printDemoData(data)

	return t
}

func createDemoData() []Row {
	return []Row{
		// Cancelled - East (2)
		{Status: "Cancelled", Region: "East", Category: "Electronics", Amount: 1300},
		{Status: "Cancelled", Region: "East", Category: "Electronics", Amount: 1250},

		// Delivered - East (2)
		{Status: "Delivered", Region: "East", Category: "Office Supplies", Amount: 15},
		{Status: "Delivered", Region: "East", Category: "Electronics", Amount: 350},

		// Delivered - North (8)
		{Status: "Delivered", Region: "North", Category: "Electronics", Amount: 1200},
		{Status: "Delivered", Region: "North", Category: "Electronics", Amount: 25},
		{Status: "Delivered", Region: "North", Category: "Electronics", Amount: 75},
		{Status: "Delivered", Region: "North", Category: "Furniture", Amount: 250},
		{Status: "Delivered", Region: "North", Category: "Electronics", Amount: 80},
		{Status: "Delivered", Region: "North", Category: "Electronics", Amount: 30},
		{Status: "Delivered", Region: "North", Category: "Office Supplies", Amount: 20},
		{Status: "Delivered", Region: "North", Category: "Electronics", Amount: 550},

		// Delivered - South (3)
		{Status: "Delivered", Region: "South", Category: "Electronics", Amount: 1150},
		{Status: "Delivered", Region: "South", Category: "Furniture", Amount: 160},
		{Status: "Delivered", Region: "South", Category: "Electronics", Amount: 1400},

		// Delivered - West (5)
		{Status: "Delivered", Region: "West", Category: "Electronics", Amount: 300},
		{Status: "Delivered", Region: "West", Category: "Office Supplies", Amount: 10},
		{Status: "Delivered", Region: "West", Category: "Office Supplies", Amount: 5},
		{Status: "Delivered", Region: "West", Category: "Electronics", Amount: 120},
		{Status: "Delivered", Region: "West", Category: "Office Supplies", Amount: 8},

		// Processing - East (1)
		{Status: "Processing", Region: "East", Category: "Furniture", Amount: 180},

		// Processing - North (1)
		{Status: "Processing", Region: "North", Category: "Furniture", Amount: 280},

		// Processing - South (3)
		{Status: "Processing", Region: "South", Category: "Furniture", Amount: 450},
		{Status: "Processing", Region: "South", Category: "Furniture", Amount: 500},
		{Status: "Processing", Region: "South", Category: "Electronics", Amount: 90},

		// Shipped - East (1)
		{Status: "Shipped", Region: "East", Category: "Furniture", Amount: 480},

		// Shipped - South (2)
		{Status: "Shipped", Region: "South", Category: "Furniture", Amount: 150},
		{Status: "Shipped", Region: "South", Category: "Electronics", Amount: 600},

		// Shipped - West (2)
		{Status: "Shipped", Region: "West", Category: "Furniture", Amount: 200},
		{Status: "Shipped", Region: "West", Category: "Electronics", Amount: 70},
	}
}

func printDemoData(data []Row) {
	for i, row := range data {
		fmt.Printf("%2d: %s - %s - %s - %d\n", i, row.Status, row.Region, row.Category, row.Amount)
	}
}

// CreateRegionsTable creates and populates a table with region information
func CreateRegionsTable() *tables.DataTable {
	t := tables.NewDataTable()

	// Create columns for region properties
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", "region"))
	populationCol := columns.NewUint32Column(columns.NewColumnDef("population", "Population (millions)", ""))
	areaCol := columns.NewUint32Column(columns.NewColumnDef("area", "Area (km²)", ""))
	capitalCol := columns.NewStringColumn(columns.NewColumnDef("capital", "Capital", "capital"))
	timezoneCol := columns.NewStringColumn(columns.NewColumnDef("timezone", "Time Zone", ""))
	currencyCol := columns.NewStringColumn(columns.NewColumnDef("currency", "Currency", ""))
	languageCol := columns.NewStringColumn(columns.NewColumnDef("language", "Primary Language", ""))
	gdpCol := columns.NewUint32Column(columns.NewColumnDef("gdp", "GDP (billions $)", ""))

	t.AddColumn(regionCol)
	t.AddColumn(populationCol)
	t.AddColumn(areaCol)
	t.AddColumn(capitalCol)
	t.AddColumn(timezoneCol)
	t.AddColumn(currencyCol)
	t.AddColumn(languageCol)
	t.AddColumn(gdpCol)

	// Create region data
	regions := createRegionData()

	// Populate the columns
	for _, region := range regions {
		regionCol.Append(region.Region)
		populationCol.Append(uint32(region.Population))
		areaCol.Append(uint32(region.Area))
		capitalCol.Append(region.Capital)
		timezoneCol.Append(region.TimeZone)
		currencyCol.Append(region.Currency)
		languageCol.Append(region.Language)
		gdpCol.Append(uint32(region.GDP))
	}

	// Finalize columns to detect uniqueness and build indexes
	regionCol.FinalizeColumn()
	populationCol.FinalizeColumn()
	areaCol.FinalizeColumn()
	capitalCol.FinalizeColumn()
	timezoneCol.FinalizeColumn()
	currencyCol.FinalizeColumn()
	languageCol.FinalizeColumn()
	gdpCol.FinalizeColumn()

	// Print region info
	fmt.Println("\nRegion Data:")
	printRegionData(regions)

	return t
}

func createRegionData() []RegionInfo {
	return []RegionInfo{
		{
			Region:     "North",
			Population: 45,
			Area:       985000,
			Capital:    "Northville",
			TimeZone:   "UTC-5",
			Currency:   "USD",
			Language:   "English",
			GDP:        2150,
		},
		{
			Region:     "South",
			Population: 38,
			Area:       765000,
			Capital:    "Southport",
			TimeZone:   "UTC-6",
			Currency:   "USD",
			Language:   "English",
			GDP:        1820,
		},
		{
			Region:     "East",
			Population: 52,
			Area:       820000,
			Capital:    "Eastborough",
			TimeZone:   "UTC-4",
			Currency:   "USD",
			Language:   "English",
			GDP:        2480,
		},
		{
			Region:     "West",
			Population: 41,
			Area:       1100000,
			Capital:    "Westfield",
			TimeZone:   "UTC-8",
			Currency:   "USD",
			Language:   "English",
			GDP:        2350,
		},
	}
}

func printRegionData(regions []RegionInfo) {
	for i, region := range regions {
		fmt.Printf("%2d: %s - Pop: %dM, Area: %.0f km², Capital: %s, GDP: $%.0fB\n",
			i, region.Region, region.Population, region.Area, region.Capital, region.GDP)
	}
}

// CreateCapitalsTable creates and populates a table with capital city information
func CreateCapitalsTable() *tables.DataTable {
	t := tables.NewDataTable()

	// Create columns for capital properties
	capitalCol := columns.NewStringColumn(columns.NewColumnDef("capital", "Capital City", "capital"))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region", "region"))
	populationCol := columns.NewUint32Column(columns.NewColumnDef("population", "Population (millions)", ""))
	foundedCol := columns.NewUint32Column(columns.NewColumnDef("founded", "Year Founded", ""))
	elevationCol := columns.NewUint32Column(columns.NewColumnDef("elevation", "Elevation (m)", ""))
	latitudeCol := columns.NewStringColumn(columns.NewColumnDef("latitude", "Latitude", ""))
	longitudeCol := columns.NewStringColumn(columns.NewColumnDef("longitude", "Longitude", ""))
	mayorCol := columns.NewStringColumn(columns.NewColumnDef("mayor", "Mayor", ""))
	universitiesCol := columns.NewUint32Column(columns.NewColumnDef("universities", "Universities", ""))
	museumsCol := columns.NewUint32Column(columns.NewColumnDef("museums", "Museums", ""))
	airportsCol := columns.NewUint32Column(columns.NewColumnDef("airports", "Airports", ""))

	t.AddColumn(capitalCol)
	t.AddColumn(regionCol)
	t.AddColumn(populationCol)
	t.AddColumn(foundedCol)
	t.AddColumn(elevationCol)
	t.AddColumn(latitudeCol)
	t.AddColumn(longitudeCol)
	t.AddColumn(mayorCol)
	t.AddColumn(universitiesCol)
	t.AddColumn(museumsCol)
	t.AddColumn(airportsCol)

	// Create capital data
	capitals := createCapitalData()

	// Populate the columns
	for _, capital := range capitals {
		capitalCol.Append(capital.Capital)
		regionCol.Append(capital.Region)
		populationCol.Append(uint32(capital.Population * 10)) // Convert to show decimals
		foundedCol.Append(uint32(capital.Founded))
		elevationCol.Append(uint32(capital.Elevation))
		latitudeCol.Append(fmt.Sprintf("%.4f", capital.Latitude))
		longitudeCol.Append(fmt.Sprintf("%.4f", capital.Longitude))
		mayorCol.Append(capital.MayorName)
		universitiesCol.Append(uint32(capital.Universities))
		museumsCol.Append(uint32(capital.Museums))
		airportsCol.Append(uint32(capital.Airports))
	}

	// Finalize columns to detect uniqueness and build indexes
	capitalCol.FinalizeColumn()
	regionCol.FinalizeColumn()
	populationCol.FinalizeColumn()
	foundedCol.FinalizeColumn()
	elevationCol.FinalizeColumn()
	latitudeCol.FinalizeColumn()
	longitudeCol.FinalizeColumn()
	mayorCol.FinalizeColumn()
	universitiesCol.FinalizeColumn()
	museumsCol.FinalizeColumn()
	airportsCol.FinalizeColumn()

	// Print capital info
	fmt.Println("\nCapital City Data:")
	printCapitalData(capitals)

	return t
}

func createCapitalData() []CapitalInfo {
	return []CapitalInfo{
		{
			Capital:      "Northville",
			Region:       "North",
			Population:   12.5,
			Founded:      1823,
			Elevation:    245,
			Latitude:     45.5231,
			Longitude:    -94.1682,
			MayorName:    "Sarah Johnson",
			Universities: 8,
			Museums:      15,
			Airports:     2,
		},
		{
			Capital:      "Southport",
			Region:       "South",
			Population:   9.8,
			Founded:      1845,
			Elevation:    85,
			Latitude:     33.7490,
			Longitude:    -84.3880,
			MayorName:    "Michael Chen",
			Universities: 6,
			Museums:      12,
			Airports:     1,
		},
		{
			Capital:      "Eastborough",
			Region:       "East",
			Population:   15.2,
			Founded:      1795,
			Elevation:    320,
			Latitude:     40.7128,
			Longitude:    -74.0060,
			MayorName:    "Emily Rodriguez",
			Universities: 12,
			Museums:      25,
			Airports:     3,
		},
		{
			Capital:      "Westfield",
			Region:       "West",
			Population:   11.3,
			Founded:      1852,
			Elevation:    610,
			Latitude:     37.7749,
			Longitude:    -122.4194,
			MayorName:    "David Kim",
			Universities: 10,
			Museums:      18,
			Airports:     2,
		},
	}
}

func printCapitalData(capitals []CapitalInfo) {
	for i, capital := range capitals {
		fmt.Printf("%2d: %s (%s) - Pop: %.1fM, Founded: %d, Elevation: %dm\n",
			i, capital.Capital, capital.Region, capital.Population, capital.Founded, capital.Elevation)
	}
}

// CreateItemsTable creates and populates a table with item/category information
func CreateItemsTable() *tables.DataTable {
	t := tables.NewDataTable()

	// Create columns for item properties
	itemIDCol := columns.NewStringColumn(columns.NewColumnDef("item_id", "Item ID", "item_id"))
	itemNameCol := columns.NewStringColumn(columns.NewColumnDef("item_name", "Item Name", ""))
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category", "category"))
	subcategoryCol := columns.NewStringColumn(columns.NewColumnDef("subcategory", "Subcategory", ""))
	brandCol := columns.NewStringColumn(columns.NewColumnDef("brand", "Brand", ""))
	priceCol := columns.NewUint32Column(columns.NewColumnDef("price", "Price ($)", ""))
	stockCol := columns.NewUint32Column(columns.NewColumnDef("stock", "Stock", ""))
	weightCol := columns.NewStringColumn(columns.NewColumnDef("weight", "Weight (kg)", ""))
	ratingCol := columns.NewStringColumn(columns.NewColumnDef("rating", "Rating", ""))
	reviewsCol := columns.NewUint32Column(columns.NewColumnDef("reviews", "Reviews", ""))
	supplierCol := columns.NewStringColumn(columns.NewColumnDef("supplier", "Supplier", ""))

	t.AddColumn(itemIDCol)
	t.AddColumn(itemNameCol)
	t.AddColumn(categoryCol)
	t.AddColumn(subcategoryCol)
	t.AddColumn(brandCol)
	t.AddColumn(priceCol)
	t.AddColumn(stockCol)
	t.AddColumn(weightCol)
	t.AddColumn(ratingCol)
	t.AddColumn(reviewsCol)
	t.AddColumn(supplierCol)

	// Create item data
	items := createItemData()

	// Populate the columns
	for _, item := range items {
		itemIDCol.Append(item.ItemID)
		itemNameCol.Append(item.ItemName)
		categoryCol.Append(item.Category)
		subcategoryCol.Append(item.Subcategory)
		brandCol.Append(item.Brand)
		priceCol.Append(uint32(item.Price))
		stockCol.Append(uint32(item.Stock))
		weightCol.Append(fmt.Sprintf("%.2f", item.Weight))
		ratingCol.Append(fmt.Sprintf("%.1f", item.Rating))
		reviewsCol.Append(uint32(item.Reviews))
		supplierCol.Append(item.Supplier)
	}

	// Finalize columns to detect uniqueness and build indexes
	itemIDCol.FinalizeColumn()
	itemNameCol.FinalizeColumn()
	categoryCol.FinalizeColumn()
	subcategoryCol.FinalizeColumn()
	brandCol.FinalizeColumn()
	priceCol.FinalizeColumn()
	stockCol.FinalizeColumn()
	weightCol.FinalizeColumn()
	ratingCol.FinalizeColumn()
	reviewsCol.FinalizeColumn()
	supplierCol.FinalizeColumn()

	// Print item info
	fmt.Println("\nItem Data:")
	printItemData(items)

	return t
}

func createItemData() []ItemInfo {
	return []ItemInfo{
		// Electronics items
		{ItemID: "E001", ItemName: "Laptop Pro 15", Category: "Electronics", Subcategory: "Computers", Brand: "TechBrand", Price: 1299.99, Stock: 45, Weight: 1.8, Rating: 4.5, Reviews: 234, Supplier: "Tech Distributors"},
		{ItemID: "E002", ItemName: "Smartphone X", Category: "Electronics", Subcategory: "Phones", Brand: "MobileTech", Price: 899.99, Stock: 120, Weight: 0.2, Rating: 4.7, Reviews: 567, Supplier: "Mobile Suppliers"},
		{ItemID: "E003", ItemName: "Wireless Earbuds", Category: "Electronics", Subcategory: "Audio", Brand: "SoundPro", Price: 149.99, Stock: 200, Weight: 0.05, Rating: 4.3, Reviews: 892, Supplier: "Audio World"},
		{ItemID: "E004", ItemName: "4K Monitor", Category: "Electronics", Subcategory: "Displays", Brand: "ViewTech", Price: 599.99, Stock: 35, Weight: 5.2, Rating: 4.6, Reviews: 145, Supplier: "Display Direct"},
		{ItemID: "E005", ItemName: "Smart Watch", Category: "Electronics", Subcategory: "Wearables", Brand: "FitTech", Price: 299.99, Stock: 80, Weight: 0.06, Rating: 4.4, Reviews: 423, Supplier: "Wearable Tech Co"},

		// Furniture items
		{ItemID: "F001", ItemName: "Executive Desk", Category: "Furniture", Subcategory: "Office", Brand: "WorkSpace", Price: 450.00, Stock: 15, Weight: 45.0, Rating: 4.2, Reviews: 67, Supplier: "Office Furniture Inc"},
		{ItemID: "F002", ItemName: "Ergonomic Chair", Category: "Furniture", Subcategory: "Seating", Brand: "ComfortPro", Price: 280.00, Stock: 25, Weight: 18.5, Rating: 4.8, Reviews: 289, Supplier: "Seating Solutions"},
		{ItemID: "F003", ItemName: "Bookshelf Unit", Category: "Furniture", Subcategory: "Storage", Brand: "SpaceSaver", Price: 150.00, Stock: 40, Weight: 28.0, Rating: 4.0, Reviews: 123, Supplier: "Storage Masters"},
		{ItemID: "F004", ItemName: "Coffee Table", Category: "Furniture", Subcategory: "Living Room", Brand: "HomeStyle", Price: 180.00, Stock: 30, Weight: 22.0, Rating: 4.1, Reviews: 89, Supplier: "Home Furnishings"},
		{ItemID: "F005", ItemName: "Filing Cabinet", Category: "Furniture", Subcategory: "Office", Brand: "OfficePro", Price: 200.00, Stock: 20, Weight: 35.0, Rating: 3.9, Reviews: 45, Supplier: "Office Furniture Inc"},

		// Office Supplies items
		{ItemID: "O001", ItemName: "Printer Paper", Category: "Office Supplies", Subcategory: "Paper", Brand: "PaperMate", Price: 25.00, Stock: 500, Weight: 2.5, Rating: 4.3, Reviews: 1023, Supplier: "Paper Products Co"},
		{ItemID: "O002", ItemName: "Gel Pens Pack", Category: "Office Supplies", Subcategory: "Writing", Brand: "WriteWell", Price: 15.00, Stock: 300, Weight: 0.15, Rating: 4.5, Reviews: 456, Supplier: "Stationery Supply"},
		{ItemID: "O003", ItemName: "Desk Organizer", Category: "Office Supplies", Subcategory: "Organization", Brand: "TidyDesk", Price: 20.00, Stock: 150, Weight: 0.8, Rating: 4.2, Reviews: 234, Supplier: "Office Essentials"},
		{ItemID: "O004", ItemName: "Stapler Pro", Category: "Office Supplies", Subcategory: "Tools", Brand: "OfficeTools", Price: 10.00, Stock: 200, Weight: 0.3, Rating: 4.0, Reviews: 178, Supplier: "Tool Distributors"},
		{ItemID: "O005", ItemName: "Label Maker", Category: "Office Supplies", Subcategory: "Organization", Brand: "LabelPro", Price: 5.00, Stock: 100, Weight: 0.4, Rating: 4.4, Reviews: 312, Supplier: "Label Solutions"},
	}
}

func printItemData(items []ItemInfo) {
	for i, item := range items {
		fmt.Printf("%2d: %s - %s (%s/%s) - $%.2f, Stock: %d\n",
			i, item.ItemID, item.ItemName, item.Category, item.Subcategory, item.Price, item.Stock)
	}
}
