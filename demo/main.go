package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/rendering"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/core/views"
)

func main() {
	fmt.Println("Starting Interpunctus V2...")

	t := tables.NewDataTable()

	// Create StringColumns for each field
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status"))
	regionCol := columns.NewStringColumn(columns.NewColumnDef("region", "Region"))
	categoryCol := columns.NewStringColumn(columns.NewColumnDef("category", "Category"))
	// For amount, we'll still use StringColumn but store string representations
	amountCol := columns.NewStringColumn(columns.NewColumnDef("amount", "Amount"))

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
		amountCol.Append(fmt.Sprintf("%d", row.Amount))
	}

	// Create renderer
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		log.Fatalf("Failed to create renderer: %v", err)
	}

	// Print demo output
	fmt.Println("\nDemo Data:")
	printDemoData(data)

	// Start web server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Define the view - which columns to display and in what order
		view := views.TableView{
			Columns: []string{"status", "region", "category", "amount"},
		}

		// Build the view model from the table
		viewModel := views.BuildViewModel(t, view, "Interpunctus V2 - Data Table Example")

		// Render using the renderer
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.Render(w, viewModel); err != nil {
			http.Error(w, fmt.Sprintf("Template rendering failed: %v", err), http.StatusInternalServerError)
			return
		}
	})

	fmt.Println("\nServer starting on http://127.0.0.1:8097")
	log.Fatal(http.ListenAndServe("127.0.0.1:8097", nil))
}

// Demo data structure
type Row struct {
	Status   string
	Region   string
	Category string
	Amount   int
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
