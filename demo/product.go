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
	"os"
	"path/filepath"

	"github.com/google/taxinomia/core/views"
	"google.golang.org/protobuf/encoding/prototext"
)

// Product defines a product configuration.
// Products filter tables by domain - only tables matching the product's domains are shown.
type Product struct {
	// Name is the URL path identifier for this product.
	Name string

	// Title is displayed on the landing page.
	Title string

	// Subtitle is displayed below the title.
	Subtitle string

	// Domains this product includes. Tables matching any of these domains are shown.
	Domains []string

	// DefaultColumns maps table names to their default visible columns.
	DefaultColumns map[string][]string

	// registry is a reference to the parent registry for accessing all tables.
	registry *ProductRegistry
}

// GetName returns the product name.
func (p *Product) GetName() string {
	return p.Name
}

// GetTitle returns the product title.
func (p *Product) GetTitle() string {
	return p.Title
}

// GetSubtitle returns the product subtitle.
func (p *Product) GetSubtitle() string {
	return p.Subtitle
}

// GetTables returns tables filtered by the product's domains.
func (p *Product) GetTables() []views.TableInfo {
	if p.registry == nil {
		return nil
	}
	return p.registry.GetTablesForDomains(p.Domains)
}

// GetDefaultColumns returns the default columns for a table.
func (p *Product) GetDefaultColumns(tableName string) []string {
	if p.DefaultColumns == nil {
		return nil
	}
	return p.DefaultColumns[tableName]
}

// ProductRegistry manages multiple products and table metadata.
type ProductRegistry struct {
	products  map[string]*Product
	fallback  string // Name of the default product
	allTables []views.TableInfo
}

// NewProductRegistry creates a new product registry.
func NewProductRegistry() *ProductRegistry {
	return &ProductRegistry{
		products: make(map[string]*Product),
		fallback: "default",
	}
}

// SetTables sets the global table metadata for all products.
func (r *ProductRegistry) SetTables(tables []views.TableInfo) {
	r.allTables = tables
}

// GetTablesForDomains returns tables that match any of the given domains.
func (r *ProductRegistry) GetTablesForDomains(domains []string) []views.TableInfo {
	if len(domains) == 0 {
		return r.allTables
	}

	domainSet := make(map[string]bool)
	for _, d := range domains {
		domainSet[d] = true
	}

	var result []views.TableInfo
	for _, table := range r.allTables {
		for _, tableDomain := range table.Domains {
			if domainSet[tableDomain] {
				result = append(result, table)
				break
			}
		}
	}
	return result
}

// Register adds a product to the registry.
func (r *ProductRegistry) Register(product *Product) {
	product.registry = r
	r.products[product.Name] = product
}

// SetFallback sets the fallback product name for unmatched paths.
func (r *ProductRegistry) SetFallback(name string) {
	r.fallback = name
}

// Get returns a product by name, or the fallback product if not found.
func (r *ProductRegistry) Get(name string) *Product {
	if name == "" {
		name = r.fallback
	}
	if product, ok := r.products[name]; ok {
		return product
	}
	return r.products[r.fallback]
}

// GetAll returns all registered products.
func (r *ProductRegistry) GetAll() []*Product {
	result := make([]*Product, 0, len(r.products))
	for _, p := range r.products {
		result = append(result, p)
	}
	return result
}

// ProductFileName is the name of the product file in each product directory.
const ProductFileName = "product.textproto"

// LoadFromDirectory loads products from subdirectories.
// Each subdirectory should contain a product.textproto file.
func (r *ProductRegistry) LoadFromDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read products directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		productPath := filepath.Join(dir, entry.Name(), ProductFileName)
		if _, err := os.Stat(productPath); os.IsNotExist(err) {
			// Skip directories without a product file
			continue
		}

		product, err := LoadProduct(productPath)
		if err != nil {
			return fmt.Errorf("failed to load product %s: %w", entry.Name(), err)
		}

		r.Register(product)
	}

	return nil
}

// LoadProduct loads a single product from a textproto file.
func LoadProduct(filePath string) (*Product, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	pbProduct := &ProductConfig{}
	if err := prototext.Unmarshal(data, pbProduct); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}

	return convertProtoProduct(pbProduct), nil
}

// convertProtoProduct converts a proto ProductConfig to the demo Product struct.
func convertProtoProduct(pbProduct *ProductConfig) *Product {
	product := &Product{
		Name:           pbProduct.GetName(),
		Title:          pbProduct.GetTitle(),
		Subtitle:       pbProduct.GetSubtitle(),
		Domains:        pbProduct.GetDomains(),
		DefaultColumns: make(map[string][]string),
	}

	// Convert default columns
	for _, dc := range pbProduct.GetDefaultColumns() {
		product.DefaultColumns[dc.GetTableName()] = dc.GetColumns()
	}

	return product
}
