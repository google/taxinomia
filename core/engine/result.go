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

package engine

import "time"

// Result is the computed window for one Request: the visible group nodes with
// their aggregates, the visible leaf page, and metadata. It is plain copy-out
// data — the window is a few hundred rows plus O(distinct) group summaries,
// so no zero-copy access is needed. No display formatting, no markup, no URLs.
type Result struct {
	// Columns describes the visible columns, in Request.Columns order.
	// Cell rows in every Page are indexed to match.
	Columns []ColumnMeta

	// Groups is the visible window of level-0 group nodes when the request
	// groups, nil otherwise. Child levels are populated only beneath nodes
	// that are expanded, and then only their visible window.
	Groups []GroupNode

	// GroupOffset is the offset of Groups within all level-0 groups.
	GroupOffset int

	// TotalGroups is the number of level-0 groups after filtering, of which
	// Groups is a window.
	TotalGroups int

	// Rows is the visible leaf page for an ungrouped request, nil when the
	// request groups (leaf pages then live inside expanded GroupNodes).
	Rows *Page

	// TotalRows is the number of rows matching the filters, of which any
	// returned page is a window.
	TotalRows int64

	// Errors reports per-field problems (a filter that failed to parse, a
	// computed column that failed to compile). A Result can be partially
	// valid: fields with errors are simply not applied.
	Errors []FieldError

	// Timings reports where the query spent its time.
	Timings []Timing
}

// ColumnMeta describes one visible column: identity and typing, no
// presentation state.
type ColumnMeta struct {
	Name        string
	DisplayName string
	EntityType  string     // primary or foreign key entity type, "" if none
	Type        ColumnType // value type for aggregate and sort purposes
	IsKey       bool       // true if this is the table's primary key column
	IsComputed  bool       // true if defined by Request.Computed
}

// ColumnType is a column's value type as the engine classifies it.
type ColumnType string

const (
	TypeString   ColumnType = "string"
	TypeNumeric  ColumnType = "numeric"
	TypeBool     ColumnType = "bool"
	TypeDatetime ColumnType = "datetime"
)

// GroupNode is one visible group. Its position in the tree gives its path:
// the engine returns nodes nested, and only where visible and expanded.
type GroupNode struct {
	// Value is the group's value in the grouped column.
	Value Value

	// RowCount is the number of rows in this group (after filtering).
	RowCount int64

	// Aggregates holds the requested aggregates over this group's rows.
	Aggregates []Aggregate

	// TotalChildren is the number of child groups at the next grouping
	// level, 0 at the deepest level.
	TotalChildren int

	// Children is the visible window of child group nodes. It is nil unless
	// this node is expanded and a deeper grouping level exists.
	Children []GroupNode

	// ChildOffset is the offset of Children within all child groups.
	ChildOffset int

	// Rows is the visible page of this group's leaf rows. It is nil unless
	// this node is expanded at the deepest grouping level.
	Rows *Page
}

// Aggregate is one computed aggregate value.
type Aggregate struct {
	Column string        // leaf column aggregated; "" for group-level kinds
	Kind   AggregateKind //
	Value  Value         //
}

// Page is a window of leaf rows, materialised only for what is visible.
type Page struct {
	// Offset of the first row within its scope (the table for an ungrouped
	// request, the enclosing group otherwise).
	Offset int

	// Keys holds each row's primary key value, for row identity across
	// requests (selection, detail views). Present whether or not the key
	// column is visible.
	Keys []string

	// Cells is row-major: Cells[r][c] is the value of column
	// Result.Columns[c] in row r.
	Cells [][]Value
}

// ValueKind discriminates Value.
type ValueKind uint8

const (
	ValueNull ValueKind = iota
	ValueString
	ValueInt
	ValueFloat
	ValueBool
	ValueTime
)

// Value is one cell, group value or aggregate value: a plain tagged union,
// no interfaces, safe to copy and serialize. Only the field selected by Kind
// is meaningful.
type Value struct {
	Kind  ValueKind
	Str   string
	Int   int64
	Float float64
	Bool  bool
	Time  time.Time
}

func StringValue(v string) Value { return Value{Kind: ValueString, Str: v} }
func IntValue(v int64) Value     { return Value{Kind: ValueInt, Int: v} }
func FloatValue(v float64) Value { return Value{Kind: ValueFloat, Float: v} }
func BoolValue(v bool) Value     { return Value{Kind: ValueBool, Bool: v} }
func TimeValue(v time.Time) Value {
	return Value{Kind: ValueTime, Time: v}
}

// FieldError reports a problem with one requested field; the rest of the
// Result remains valid.
type FieldError struct {
	Column  string // the column (or computed column) the error concerns
	Message string
}

// Timing is one named phase of query execution and its duration.
type Timing struct {
	Label    string
	Duration time.Duration
}
