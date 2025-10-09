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
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Order struct {
	OrderID         string `json:"order_id"`
	Customer        string `json:"customer"`
	Region          string `json:"region"`
	Country         string `json:"country"`
	ProductCategory string `json:"product_category"`
	Product         string `json:"product"`
	Status          string `json:"status"`
	Priority        string `json:"priority"`
	Amount          int    `json:"amount"`
	Quantity        int    `json:"quantity"`
	Year            string `json:"year"`
	Quarter         string `json:"quarter"`
	Month           string `json:"month"`
}

func LoadSampleOrders(filename string) (*Table, error) {
	// Read JSON file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse JSON
	var orders []Order
	if err := json.Unmarshal(data, &orders); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Create columns
	orderIDCol := NewColumn[uint32](&ColumnDef{
		name:        "order_id",
		displayName: "Order ID",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	customerCol := NewColumn[uint32](&ColumnDef{
		name:        "customer",
		displayName: "Customer",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	regionCol := NewColumn[uint8](&ColumnDef{
		name:        "region",
		displayName: "Region",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	countryCol := NewColumn[uint8](&ColumnDef{
		name:        "country",
		displayName: "Country",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	productCategoryCol := NewColumn[uint8](&ColumnDef{
		name:        "product_category",
		displayName: "Product Category",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	productCol := NewColumn[uint32](&ColumnDef{
		name:        "product",
		displayName: "Product",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	statusCol := NewColumn[uint8](&ColumnDef{
		name:        "status",
		displayName: "Status",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	priorityCol := NewColumn[uint8](&ColumnDef{
		name:        "priority",
		displayName: "Priority",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	amountCol := NewColumn[uint32](&ColumnDef{
		name:        "amount",
		displayName: "Amount",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    true,
	})

	quantityCol := NewColumn[uint32](&ColumnDef{
		name:        "quantity",
		displayName: "Quantity",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    true,
	})

	yearCol := NewColumn[uint16](&ColumnDef{
		name:        "year",
		displayName: "Year",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    CompareNumbers,
		summable:    false,
	})

	quarterCol := NewColumn[uint8](&ColumnDef{
		name:        "quarter",
		displayName: "Quarter",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	monthCol := NewColumn[uint8](&ColumnDef{
		name:        "month",
		displayName: "Month",
		valueToKey:  map[string]uint32{},
		keyToValue:  map[uint32]string{},
		comparer:    strings.Compare,
		summable:    false,
	})

	// Add rows
	for _, order := range orders {
		orderIDCol.Append(order.OrderID)
		customerCol.Append(order.Customer)
		regionCol.Append(order.Region)
		countryCol.Append(order.Country)
		productCategoryCol.Append(order.ProductCategory)
		productCol.Append(order.Product)
		statusCol.Append(order.Status)
		priorityCol.Append(order.Priority)
		amountCol.Append(fmt.Sprintf("%d", order.Amount))
		quantityCol.Append(fmt.Sprintf("%d", order.Quantity))
		yearCol.Append(order.Year)
		quarterCol.Append(order.Quarter)
		monthCol.Append(order.Month)
	}

	// Create table
	table := &Table{
		columns: map[string]IColumn{
			"order_id":         orderIDCol,
			"customer":         customerCol,
			"region":           regionCol,
			"country":          countryCol,
			"product_category": productCategoryCol,
			"product":          productCol,
			"status":           statusCol,
			"priority":         priorityCol,
			"amount":           amountCol,
			"quantity":         quantityCol,
			"year":             yearCol,
			"quarter":          quarterCol,
			"month":            monthCol,
		},
	}

	fmt.Printf("Loaded %d orders\n", len(orders))
	return table, nil
}
