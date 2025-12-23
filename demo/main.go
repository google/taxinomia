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

package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/google/taxinomia/core/query"
	"github.com/google/taxinomia/core/rendering"
	"github.com/google/taxinomia/core/views"
)

func main() {
	fmt.Println("Starting Interpunctus V2...")

	// Create demo tables with sample data
	ordersTable := CreateDemoTable()
	regionsTable := CreateRegionsTable()
	capitalsTable := CreateCapitalsTable()
	itemsTable := CreateItemsTable()

	// Create renderer
	renderer, err := rendering.NewTableRenderer()
	if err != nil {
		log.Fatalf("Failed to create renderer: %v", err)
	}

	// Landing page with links to tables
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		landingVM := views.LandingViewModel{
			Title:    "Taxinomia Demo Tables",
			Subtitle: "Explore the power of dynamic table rendering with column visibility and drag-and-drop ordering",
			Tables: []views.TableInfo{
				{
					Name:           "Orders Table",
					Description:    "Track orders with status, region, category, and amount data. Perfect for analyzing sales patterns and order fulfillment.",
					URL:            "/orders",
					RecordCount:    30,
					ColumnCount:    4,
					DefaultColumns: "4 columns",
					Categories:     "Sales, Logistics",
				},
				{
					Name:           "Regions Table",
					Description:    "Geographic and economic information about different regions including population, area, capital cities, and GDP.",
					URL:            "/regions",
					RecordCount:    4,
					ColumnCount:    8,
					DefaultColumns: "5 columns",
					Categories:     "Geographic, Economic",
				},
				{
					Name:           "Capitals Table",
					Description:    "Detailed information about capital cities including population, founding year, elevation, and civic infrastructure.",
					URL:            "/capitals",
					RecordCount:    4,
					ColumnCount:    11,
					DefaultColumns: "6 columns",
					Categories:     "Cities, Demographics",
				},
				{
					Name:           "Items Table",
					Description:    "Product catalog with category hierarchy, pricing, inventory levels, and supplier information.",
					URL:            "/items",
					RecordCount:    15,
					ColumnCount:    11,
					DefaultColumns: "6 columns",
					Categories:     "Inventory, Products",
				},
			},
		}

		// Render using the landing template
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.RenderLanding(w, landingVM); err != nil {
			log.Printf("Landing page rendering error: %v", err)
			return
		}
	})

	// Orders table handler
	http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		// Parse columns from query parameter
		defaultColumns := []string{"status", "region", "category", "amount"}
		columns := query.ParseColumns(r.URL.Query(), defaultColumns)

		// Define the view - which columns to display and in what order
		view := views.TableView{
			Columns: columns,
		}

		// Build the view model from the table
		viewModel := views.BuildViewModel(ordersTable, view, "Orders Table - Taxinomia Demo")

		// Render using the renderer
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.Render(w, viewModel); err != nil {
			// Log the error instead of trying to write an error response
			// since the renderer may have already written to the response
			log.Printf("Template rendering error: %v", err)
			return
		}
	})

	// Regions table handler
	http.HandleFunc("/regions", func(w http.ResponseWriter, r *http.Request) {
		// Parse columns from query parameter
		defaultColumns := []string{"region", "population", "capital", "timezone", "gdp"}
		columns := query.ParseColumns(r.URL.Query(), defaultColumns)

		// Define the view - which columns to display and in what order
		view := views.TableView{
			Columns: columns,
		}

		// Build the view model from the table
		viewModel := views.BuildViewModel(regionsTable, view, "Regions Table - Taxinomia Demo")

		// Render using the renderer
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.Render(w, viewModel); err != nil {
			// Log the error instead of trying to write an error response
			// since the renderer may have already written to the response
			log.Printf("Template rendering error: %v", err)
			return
		}
	})

	// Capitals table handler
	http.HandleFunc("/capitals", func(w http.ResponseWriter, r *http.Request) {
		// Parse columns from query parameter
		defaultColumns := []string{"capital", "region", "population", "founded", "mayor", "universities"}
		columns := query.ParseColumns(r.URL.Query(), defaultColumns)

		// Define the view - which columns to display and in what order
		view := views.TableView{
			Columns: columns,
		}

		// Build the view model from the table
		viewModel := views.BuildViewModel(capitalsTable, view, "Capitals Table - Taxinomia Demo")

		// Render using the renderer
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.Render(w, viewModel); err != nil {
			// Log the error instead of trying to write an error response
			// since the renderer may have already written to the response
			log.Printf("Template rendering error: %v", err)
			return
		}
	})

	// Items table handler
	http.HandleFunc("/items", func(w http.ResponseWriter, r *http.Request) {
		// Parse columns from query parameter
		defaultColumns := []string{"item_id", "item_name", "category", "subcategory", "price", "stock"}
		columns := query.ParseColumns(r.URL.Query(), defaultColumns)

		// Define the view - which columns to display and in what order
		view := views.TableView{
			Columns: columns,
		}

		// Build the view model from the table
		viewModel := views.BuildViewModel(itemsTable, view, "Items Table - Taxinomia Demo")

		// Render using the renderer
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := renderer.Render(w, viewModel); err != nil {
			// Log the error instead of trying to write an error response
			// since the renderer may have already written to the response
			log.Printf("Template rendering error: %v", err)
			return
		}
	})

	fmt.Println("\nServer starting on http://127.0.0.1:8097")
	log.Fatal(http.ListenAndServe("127.0.0.1:8097", nil))
}
