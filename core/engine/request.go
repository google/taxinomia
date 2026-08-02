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

// Request is a transport-agnostic description of what to compute: which
// table, which columns, how to filter, group and sort, and — because the
// engine produces a window, not a result set — which part of the result is
// visible (Viewport) and which group subtrees are open (Expanded).
//
// Expansion state is a query concept, not a presentation leak: it determines
// which subtrees the engine computes at all.
type Request struct {
	// Table is the name of the table to query.
	Table string

	// Columns are the visible columns, in display order. Cell pages and
	// column metadata in the Result follow this order.
	Columns []string

	// Filters maps a column name to the filter expression to apply, in the
	// engine's filter syntax (the same strings the current UI passes through).
	Filters map[string]string

	// Computed defines additional columns evaluated from expressions over
	// existing columns. Computed columns may appear in Columns, Filters,
	// GroupBy, Sort and Aggregates by their Name.
	Computed []ComputedColumn

	// GroupBy lists the grouping columns from level 0 downward. Empty means
	// a flat (ungrouped) view.
	GroupBy []string

	// Aggregates maps a column name to the aggregates to compute for it in
	// every visible group. Columns absent from the map get no aggregates.
	Aggregates map[string][]AggregateKind

	// Sort orders leaf rows, highest priority first.
	Sort []SortKey

	// GroupSorts orders groups by an aggregate value instead of by the
	// group's own value. At most one entry per grouped column.
	GroupSorts []GroupSort

	// Viewport is the visible slice of the top level: level-0 groups when
	// grouped, leaf rows when not.
	Viewport Viewport

	// Expanded lists the group paths that are open. The engine computes
	// child levels and leaf pages only beneath paths listed here (and only
	// where visible). An empty list means nothing is expanded.
	Expanded []GroupPath
}

// ComputedColumn is a named expression evaluated per row.
type ComputedColumn struct {
	Name       string // column name the result is exposed under
	Expression string // expression source, e.g. "add(price,qty)"
}

// SortKey is one column in a sort order.
type SortKey struct {
	Column     string
	Descending bool
}

// GroupSort orders the groups of one grouped column by an aggregate.
// AggColumn is the leaf column whose aggregate is compared; it is empty for
// group-level kinds (AggRowCount, AggSubgroupCount), which do not refer to a
// leaf column.
type GroupSort struct {
	GroupColumn string        // the grouped column whose groups are ordered
	AggColumn   string        // leaf column aggregated; "" for group-level kinds
	Agg         AggregateKind // which aggregate to compare
	Descending  bool
}

// Viewport is the visible slice of a result level: skip Offset entries, then
// return up to Limit. Limit 0 means no limit (today's "show all").
type Viewport struct {
	Offset int
	Limit  int
}

// GroupPath identifies a group node by the group values along the path from
// level 0 to the node, e.g. {"europe-west1", "rack-07"}.
type GroupPath []string

// AggregateKind identifies an aggregate function.
type AggregateKind string

// Aggregate kinds for numeric columns.
const (
	AggSum    AggregateKind = "sum"
	AggAvg    AggregateKind = "avg"
	AggStdDev AggregateKind = "stddev"
	AggMin    AggregateKind = "min"
	AggMax    AggregateKind = "max"
	AggCount  AggregateKind = "count"
)

// Aggregate kinds for string columns.
const (
	AggUnique AggregateKind = "unique"
)

// Aggregate kinds for boolean columns.
const (
	AggTrue  AggregateKind = "true"
	AggFalse AggregateKind = "false"
	AggRatio AggregateKind = "ratio"
)

// Aggregate kinds for datetime columns (which also use min, max, avg, stddev).
const (
	AggSpan AggregateKind = "span"
)

// Group-level aggregate kinds, valid only in GroupSort (they describe the
// group itself, not a leaf column).
const (
	AggRowCount      AggregateKind = "rows"
	AggSubgroupCount AggregateKind = "subgroups"
)
