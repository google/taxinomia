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

package urlquery

import "github.com/google/taxinomia/core/queryspec"

// The neutral query vocabulary lives in core/queryspec (the data side of the
// boundary). These aliases re-export it so URL-side code — and the core/query
// forwarding package — can keep addressing the full former core/query surface
// through one package.

type (
	// ComputedColumnDef is an alias for queryspec.ComputedColumnDef.
	ComputedColumnDef = queryspec.ComputedColumnDef
	// SortColumn is an alias for queryspec.SortColumn.
	SortColumn = queryspec.SortColumn
	// GroupAggSort is an alias for queryspec.GroupAggSort.
	GroupAggSort = queryspec.GroupAggSort
	// AggregateType is an alias for queryspec.AggregateType.
	AggregateType = queryspec.AggregateType
	// ColumnType is an alias for queryspec.ColumnType.
	ColumnType = queryspec.ColumnType
)

const (
	AggSum    = queryspec.AggSum
	AggAvg    = queryspec.AggAvg
	AggStdDev = queryspec.AggStdDev
	AggMin    = queryspec.AggMin
	AggMax    = queryspec.AggMax
	AggCount  = queryspec.AggCount

	AggUnique = queryspec.AggUnique

	AggTrue  = queryspec.AggTrue
	AggFalse = queryspec.AggFalse
	AggRatio = queryspec.AggRatio

	AggSpan = queryspec.AggSpan

	AggRowCount      = queryspec.AggRowCount
	AggSubgroupCount = queryspec.AggSubgroupCount

	ColumnTypeNumeric  = queryspec.ColumnTypeNumeric
	ColumnTypeString   = queryspec.ColumnTypeString
	ColumnTypeBool     = queryspec.ColumnTypeBool
	ColumnTypeDatetime = queryspec.ColumnTypeDatetime
)

// GetAvailableAggregates forwards to queryspec.GetAvailableAggregates.
var GetAvailableAggregates = queryspec.GetAvailableAggregates

// AggregateSymbol forwards to queryspec.AggregateSymbol.
var AggregateSymbol = queryspec.AggregateSymbol

// AggregateTitle forwards to queryspec.AggregateTitle.
var AggregateTitle = queryspec.AggregateTitle
