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

// Package query is a forwarding package kept for import-path compatibility.
// The package moved: URL parsing and URL building live in web/urlquery, the
// neutral query vocabulary lives in core/queryspec. Every name here is an
// alias for the same type, so existing code keeps compiling and interoperates
// with code using the new paths.
//
// Deprecated: import github.com/google/taxinomia/web/urlquery (URL/Query) or
// github.com/google/taxinomia/core/queryspec (spec types) instead. This
// forwarding package will be removed in a future cleanup release.
package query

import (
	"github.com/google/taxinomia/web/urlquery"
)

type (
	// ComputedColumnDef is an alias for queryspec.ComputedColumnDef.
	ComputedColumnDef = urlquery.ComputedColumnDef
	// SortColumn is an alias for queryspec.SortColumn.
	SortColumn = urlquery.SortColumn
	// GroupAggSort is an alias for queryspec.GroupAggSort.
	GroupAggSort = urlquery.GroupAggSort
	// AggregateType is an alias for queryspec.AggregateType.
	AggregateType = urlquery.AggregateType
	// ColumnType is an alias for queryspec.ColumnType.
	ColumnType = urlquery.ColumnType
	// Query is an alias for urlquery.Query.
	Query = urlquery.Query
)

const (
	AggSum    = urlquery.AggSum
	AggAvg    = urlquery.AggAvg
	AggStdDev = urlquery.AggStdDev
	AggMin    = urlquery.AggMin
	AggMax    = urlquery.AggMax
	AggCount  = urlquery.AggCount

	AggUnique = urlquery.AggUnique

	AggTrue  = urlquery.AggTrue
	AggFalse = urlquery.AggFalse
	AggRatio = urlquery.AggRatio

	AggSpan = urlquery.AggSpan

	AggRowCount      = urlquery.AggRowCount
	AggSubgroupCount = urlquery.AggSubgroupCount

	ColumnTypeNumeric  = urlquery.ColumnTypeNumeric
	ColumnTypeString   = urlquery.ColumnTypeString
	ColumnTypeBool     = urlquery.ColumnTypeBool
	ColumnTypeDatetime = urlquery.ColumnTypeDatetime
)

// GetAvailableAggregates forwards to queryspec.GetAvailableAggregates.
var GetAvailableAggregates = urlquery.GetAvailableAggregates

// AggregateSymbol forwards to queryspec.AggregateSymbol.
var AggregateSymbol = urlquery.AggregateSymbol

// AggregateTitle forwards to queryspec.AggregateTitle.
var AggregateTitle = urlquery.AggregateTitle

// NewQuery forwards to urlquery.NewQuery.
var NewQuery = urlquery.NewQuery
