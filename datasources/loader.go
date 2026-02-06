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

// Package datasources provides a unified interface for loading data from
// various sources (protobuf, CSV, databases, etc.) with support for
// reusable column annotations.
package datasources

import (
	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// ColumnType represents the data type of a column.
type ColumnType int

const (
	TypeString ColumnType = iota
	TypeInt64
	TypeUint32
	TypeUint64
	TypeFloat64
	TypeBool
	TypeDatetime
	TypeDuration
)

// String returns the string representation of the column type.
func (t ColumnType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeInt64:
		return "int64"
	case TypeUint32:
		return "uint32"
	case TypeUint64:
		return "uint64"
	case TypeFloat64:
		return "float64"
	case TypeBool:
		return "bool"
	case TypeDatetime:
		return "datetime"
	case TypeDuration:
		return "duration"
	default:
		return "unknown"
	}
}

// ColumnSchema represents a single column's schema discovered from a data source.
type ColumnSchema struct {
	Name string
	Type ColumnType
}

// TableSchema represents the full table schema discovered from a data source.
type TableSchema struct {
	Columns []*ColumnSchema
}

// EnrichedColumn combines discovered schema with annotations.
type EnrichedColumn struct {
	Name        string
	Type        ColumnType
	DisplayName string
	EntityType  string
}

// DataSourceLoader is the interface that all data source loaders must implement.
// Taxinomia provides built-in loaders for "proto" and "csv".
// Users can register additional loaders for databases, APIs, or custom formats.
type DataSourceLoader interface {
	// SourceType returns the type identifier used in config (e.g., "proto", "csv", "postgres").
	SourceType() string

	// DiscoverSchema returns the schema discovered from the data source.
	// This is called first to determine column names and types.
	DiscoverSchema(config map[string]string) (*TableSchema, error)

	// Load retrieves data and returns a DataTable.
	// The enriched columns contain the discovered schema plus annotations.
	Load(config map[string]string, columns []*EnrichedColumn) (*tables.DataTable, error)
}

// EnrichSchema combines a discovered TableSchema with ColumnAnnotations.
// For each column in the schema, it applies display_name and entity_type from annotations.
func EnrichSchema(schema *TableSchema, annotations *ColumnAnnotations) []*EnrichedColumn {
	annotationMap := AnnotationsToColumnMap(annotations)

	result := make([]*EnrichedColumn, len(schema.Columns))
	for i, col := range schema.Columns {
		enriched := &EnrichedColumn{
			Name:        col.Name,
			Type:        col.Type,
			DisplayName: col.Name, // default to column name
			EntityType:  "",
		}

		if ann, ok := annotationMap[col.Name]; ok {
			if ann.GetDisplayName() != "" {
				enriched.DisplayName = ann.GetDisplayName()
			}
			enriched.EntityType = ann.GetEntityType()
		}

		result[i] = enriched
	}
	return result
}

// AnnotationsToColumnMap converts ColumnAnnotations to a map for easy lookup by column name.
func AnnotationsToColumnMap(annotations *ColumnAnnotations) map[string]*ColumnAnnotation {
	if annotations == nil {
		return nil
	}
	result := make(map[string]*ColumnAnnotation)
	for _, col := range annotations.GetColumns() {
		result[col.GetName()] = col
	}
	return result
}

// CreateColumnDef creates a columns.ColumnDef from an EnrichedColumn.
func CreateColumnDef(col *EnrichedColumn) *columns.ColumnDef {
	return columns.NewColumnDef(col.Name, col.DisplayName, col.EntityType)
}

// CreateTable creates an empty DataTable with columns based on enriched schema.
func CreateTable(enrichedColumns []*EnrichedColumn) *tables.DataTable {
	table := tables.NewDataTable()
	// Note: columns are added later when data is loaded
	return table
}
