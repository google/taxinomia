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

// Package queryspec holds the transport-neutral query vocabulary: the types
// that describe what to compute (sort columns, aggregate kinds, column type
// classes), independent of how a request arrived. It is the data-side half of
// the former core/query package; the URL/HTTP half lives in web/urlquery.
// Core packages (tables, aggregates) depend on this package only — never on
// the web tree.
package queryspec

// ComputedColumnDef represents a computed column definition from URL
type ComputedColumnDef struct {
	Name       string // Column name
	Expression string // Expression as entered by user (e.g., "add(price,qty)")
}

// SortColumn represents a column in the sort order with its direction
type SortColumn struct {
	Name       string // Column name
	Descending bool   // true = descending (-), false = ascending (+)
}

// GroupAggSort specifies how a grouped column should be sorted by an aggregate value
type GroupAggSort struct {
	GroupedColumn string        // The grouped column to sort
	LeafColumn    string        // The leaf column whose aggregate to sort by
	AggType       AggregateType // The aggregate type to sort by
	Descending    bool          // Sort direction
}

// AggregateType represents a type of aggregate function
type AggregateType string

// Aggregate types for numeric columns
const (
	AggSum    AggregateType = "sum"    // Sum of values
	AggAvg    AggregateType = "avg"    // Average/mean
	AggStdDev AggregateType = "stddev" // Standard deviation
	AggMin    AggregateType = "min"    // Minimum value
	AggMax    AggregateType = "max"    // Maximum value
	AggCount  AggregateType = "count"  // Count of non-null values
)

// Aggregate types for string columns
const (
	AggUnique AggregateType = "unique" // Count of unique values
)

// Aggregate types for boolean columns
const (
	AggTrue  AggregateType = "true"  // Count of true values
	AggFalse AggregateType = "false" // Count of false values
	AggRatio AggregateType = "ratio" // Ratio of true to total
)

// Aggregate types for datetime columns (also uses min, max, avg, stddev)
const (
	AggSpan AggregateType = "span" // Time span (max - min)
)

// Special aggregate types for group-level sorting (not leaf column aggregates)
const (
	AggRowCount      AggregateType = "rows"      // Total row count in group
	AggSubgroupCount AggregateType = "subgroups" // Number of subgroups
)

// ColumnType represents the data type of a column for aggregate purposes
type ColumnType string

const (
	ColumnTypeNumeric  ColumnType = "numeric"
	ColumnTypeString   ColumnType = "string"
	ColumnTypeBool     ColumnType = "bool"
	ColumnTypeDatetime ColumnType = "datetime"
)

// GetAvailableAggregates returns the list of available aggregate types for a column type
func GetAvailableAggregates(colType ColumnType) []AggregateType {
	switch colType {
	case ColumnTypeNumeric:
		return []AggregateType{AggCount, AggSum, AggAvg, AggStdDev, AggMin, AggMax}
	case ColumnTypeString:
		return []AggregateType{AggCount, AggUnique, AggMin, AggMax}
	case ColumnTypeBool:
		return []AggregateType{AggCount, AggTrue, AggFalse, AggRatio}
	case ColumnTypeDatetime:
		return []AggregateType{AggCount, AggMin, AggMax, AggAvg, AggStdDev, AggSpan}
	default:
		return []AggregateType{AggCount}
	}
}

// AggregateSymbol returns the display symbol for an aggregate type
func AggregateSymbol(agg AggregateType) string {
	switch agg {
	case AggSum:
		return "Σ"
	case AggAvg:
		return "μ"
	case AggStdDev:
		return "σ"
	case AggMin:
		return "↓"
	case AggMax:
		return "↑"
	case AggCount:
		return "#"
	case AggUnique:
		return "◇"
	case AggTrue:
		return "✓"
	case AggFalse:
		return "✗"
	case AggRatio:
		return "%"
	case AggSpan:
		return "Δ"
	case AggRowCount:
		return "≡"
	case AggSubgroupCount:
		return "⊞"
	default:
		return string(agg)
	}
}

// AggregateTitle returns the tooltip title for an aggregate type
func AggregateTitle(agg AggregateType) string {
	switch agg {
	case AggSum:
		return "Sum"
	case AggAvg:
		return "Average"
	case AggStdDev:
		return "Standard Deviation"
	case AggMin:
		return "Minimum"
	case AggMax:
		return "Maximum"
	case AggCount:
		return "Count"
	case AggUnique:
		return "Unique Values"
	case AggTrue:
		return "True Count"
	case AggFalse:
		return "False Count"
	case AggRatio:
		return "True Ratio"
	case AggSpan:
		return "Time Span"
	case AggRowCount:
		return "Row Count"
	case AggSubgroupCount:
		return "Subgroup Count"
	default:
		return string(agg)
	}
}
