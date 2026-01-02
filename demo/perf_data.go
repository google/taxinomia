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

// Performance test configuration - easily modifiable cardinality
const (
	PERF_NUM_TRANSACTIONS = 1_000_000
	PERF_NUM_USERS        = 800_000 // High cardinality: 80% unique (1.25 txns per user avg)
	PERF_NUM_PRODUCTS     = 50_000  // Medium cardinality: (20 txns per product avg)
	PERF_NUM_CATEGORIES   = 200     // Low cardinality: (5000 txns per category avg)
)

// CreatePerfTransactionsTable creates a large transaction table for performance testing
func CreatePerfTransactionsTable() *tables.DataTable {
	fmt.Printf("Creating performance transactions table with %d rows...\n", PERF_NUM_TRANSACTIONS)

	t := tables.NewDataTable()

	// Create columns
	txnIDCol := columns.NewUint32Column(columns.NewColumnDef("txn_id", "Transaction ID", "txn_id"))
	userIDCol := columns.NewUint32Column(columns.NewColumnDef("user_id", "User ID", "user_id"))
	productIDCol := columns.NewUint32Column(columns.NewColumnDef("product_id", "Product ID", "product_id"))
	categoryIDCol := columns.NewUint32Column(columns.NewColumnDef("category_id", "Category ID", "category_id"))
	amountCol := columns.NewUint32Column(columns.NewColumnDef("amount", "Amount", ""))
	statusCol := columns.NewStringColumn(columns.NewColumnDef("status", "Status", ""))

	t.AddColumn(txnIDCol)
	t.AddColumn(userIDCol)
	t.AddColumn(productIDCol)
	t.AddColumn(categoryIDCol)
	t.AddColumn(amountCol)
	t.AddColumn(statusCol)

	// Status values for cycling
	statuses := []string{"pending", "completed", "cancelled", "processing"}

	// Generate data directly into columns (memory efficient)
	for i := uint32(0); i < PERF_NUM_TRANSACTIONS; i++ {
		txnIDCol.Append(i)

		// User ID: cycle through users to create realistic distribution
		// First 80% of transactions get unique users, rest reuse
		userID := i % uint32(PERF_NUM_USERS)
		userIDCol.Append(userID)

		// Product ID: moderate reuse
		productID := i % uint32(PERF_NUM_PRODUCTS)
		productIDCol.Append(productID)

		// Category ID: heavy reuse (low cardinality)
		// Use weighted distribution - some categories more popular
		categoryID := (i % uint32(PERF_NUM_CATEGORIES))
		if i%7 == 0 { // Make category 0 more common
			categoryID = 0
		}
		categoryIDCol.Append(categoryID)

		// Amount: deterministic but varied
		amount := uint32(10 + (i % 1000))
		amountCol.Append(amount)

		// Status: cycle through
		status := statuses[i%uint32(len(statuses))]
		statusCol.Append(status)
	}

	// Finalize columns
	txnIDCol.FinalizeColumn()
	userIDCol.FinalizeColumn()
	productIDCol.FinalizeColumn()
	categoryIDCol.FinalizeColumn()
	amountCol.FinalizeColumn()
	statusCol.FinalizeColumn()

	fmt.Printf("  Created %d transactions\n", PERF_NUM_TRANSACTIONS)
	return t
}

// CreatePerfUsersTable creates a user table for high-cardinality joins
func CreatePerfUsersTable() *tables.DataTable {
	fmt.Printf("Creating performance users table with %d rows...\n", PERF_NUM_USERS)

	t := tables.NewDataTable()

	// Create columns
	userIDCol := columns.NewUint32Column(columns.NewColumnDef("user_id", "User ID", "user_id"))
	usernameCol := columns.NewStringColumn(columns.NewColumnDef("username", "Username", ""))
	countryCol := columns.NewStringColumn(columns.NewColumnDef("country", "Country", ""))
	signupYearCol := columns.NewUint32Column(columns.NewColumnDef("signup_year", "Signup Year", ""))

	t.AddColumn(userIDCol)
	t.AddColumn(usernameCol)
	t.AddColumn(countryCol)
	t.AddColumn(signupYearCol)

	// Country values for distribution
	countries := []string{"US", "UK", "CA", "AU", "DE", "FR", "JP", "CN", "IN", "BR"}

	// Generate data
	for i := uint32(0); i < uint32(PERF_NUM_USERS); i++ {
		userIDCol.Append(i)
		usernameCol.Append(fmt.Sprintf("user_%d", i))
		countryCol.Append(countries[i%uint32(len(countries))])
		signupYearCol.Append(2020 + (i % 5)) // Years 2020-2024
	}

	// Finalize columns
	userIDCol.FinalizeColumn()
	usernameCol.FinalizeColumn()
	countryCol.FinalizeColumn()
	signupYearCol.FinalizeColumn()

	fmt.Printf("  Created %d users (high cardinality join target)\n", PERF_NUM_USERS)
	return t
}

// CreatePerfProductsTable creates a product table for medium-cardinality joins
func CreatePerfProductsTable() *tables.DataTable {
	fmt.Printf("Creating performance products table with %d rows...\n", PERF_NUM_PRODUCTS)

	t := tables.NewDataTable()

	// Create columns
	productIDCol := columns.NewUint32Column(columns.NewColumnDef("product_id", "Product ID", "product_id"))
	productNameCol := columns.NewStringColumn(columns.NewColumnDef("product_name", "Product Name", ""))
	categoryIDCol := columns.NewUint32Column(columns.NewColumnDef("category_id", "Category ID", "category_id"))
	priceCol := columns.NewUint32Column(columns.NewColumnDef("price", "Price", ""))

	t.AddColumn(productIDCol)
	t.AddColumn(productNameCol)
	t.AddColumn(categoryIDCol)
	t.AddColumn(priceCol)

	// Generate data
	for i := uint32(0); i < uint32(PERF_NUM_PRODUCTS); i++ {
		productIDCol.Append(i)
		productNameCol.Append(fmt.Sprintf("Product_%d", i))

		// Map products to categories
		categoryID := i % uint32(PERF_NUM_CATEGORIES)
		categoryIDCol.Append(categoryID)

		// Price based on product ID
		price := uint32(10 + (i % 500))
		priceCol.Append(price)
	}

	// Finalize columns
	productIDCol.FinalizeColumn()
	productNameCol.FinalizeColumn()
	categoryIDCol.FinalizeColumn()
	priceCol.FinalizeColumn()

	fmt.Printf("  Created %d products (medium cardinality join target)\n", PERF_NUM_PRODUCTS)
	return t
}

// CreatePerfCategoriesTable creates a category table for low-cardinality joins
func CreatePerfCategoriesTable() *tables.DataTable {
	fmt.Printf("Creating performance categories table with %d rows...\n", PERF_NUM_CATEGORIES)

	t := tables.NewDataTable()

	// Create columns
	categoryIDCol := columns.NewUint32Column(columns.NewColumnDef("category_id", "Category ID", "category_id"))
	categoryNameCol := columns.NewStringColumn(columns.NewColumnDef("category_name", "Category Name", ""))
	departmentCol := columns.NewStringColumn(columns.NewColumnDef("department", "Department", ""))

	t.AddColumn(categoryIDCol)
	t.AddColumn(categoryNameCol)
	t.AddColumn(departmentCol)

	// Departments for categories
	departments := []string{"Electronics", "Home & Garden", "Sports", "Books", "Clothing"}

	// Generate data
	for i := uint32(0); i < uint32(PERF_NUM_CATEGORIES); i++ {
		categoryIDCol.Append(i)
		categoryNameCol.Append(fmt.Sprintf("Category_%d", i))
		departmentCol.Append(departments[i%uint32(len(departments))])
	}

	// Finalize columns
	categoryIDCol.FinalizeColumn()
	categoryNameCol.FinalizeColumn()
	departmentCol.FinalizeColumn()

	fmt.Printf("  Created %d categories (low cardinality join target)\n", PERF_NUM_CATEGORIES)
	return t
}
