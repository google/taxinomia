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

// Package views is a forwarding package kept for import-path compatibility.
// The package moved to web/viewmodel. Every name here is an alias for the
// same type, so existing code keeps compiling and interoperates with code
// using the new path.
//
// Deprecated: import github.com/google/taxinomia/web/viewmodel instead. This
// forwarding package will be removed in a future cleanup release.
package views

import (
	"github.com/google/taxinomia/web/viewmodel"
)

type (
	// AggregateToggle is an alias for viewmodel.AggregateToggle.
	AggregateToggle = viewmodel.AggregateToggle
	// AllURLsResolver is an alias for viewmodel.AllURLsResolver.
	AllURLsResolver = viewmodel.AllURLsResolver
	// ColumnInfo is an alias for viewmodel.ColumnInfo.
	ColumnInfo = viewmodel.ColumnInfo
	// ColumnSummary is an alias for viewmodel.ColumnSummary.
	ColumnSummary = viewmodel.ColumnSummary
	// ComputedColumnInfo is an alias for viewmodel.ComputedColumnInfo.
	ComputedColumnInfo = viewmodel.ComputedColumnInfo
	// EntityTypeDescriptionResolver is an alias for viewmodel.EntityTypeDescriptionResolver.
	EntityTypeDescriptionResolver = viewmodel.EntityTypeDescriptionResolver
	// EntityURL is an alias for viewmodel.EntityURL.
	EntityURL = viewmodel.EntityURL
	// GroupBuildResult is an alias for viewmodel.GroupBuildResult.
	GroupBuildResult = viewmodel.GroupBuildResult
	// GroupedCell is an alias for viewmodel.GroupedCell.
	GroupedCell = viewmodel.GroupedCell
	// GroupedRow is an alias for viewmodel.GroupedRow.
	GroupedRow = viewmodel.GroupedRow
	// HierarchyContext is an alias for viewmodel.HierarchyContext.
	HierarchyContext = viewmodel.HierarchyContext
	// HierarchyContextBuilder is an alias for viewmodel.HierarchyContextBuilder.
	HierarchyContextBuilder = viewmodel.HierarchyContextBuilder
	// HierarchyLevel is an alias for viewmodel.HierarchyLevel.
	HierarchyLevel = viewmodel.HierarchyLevel
	// JoinTarget is an alias for viewmodel.JoinTarget.
	JoinTarget = viewmodel.JoinTarget
	// LandingViewModel is an alias for viewmodel.LandingViewModel.
	LandingViewModel = viewmodel.LandingViewModel
	// RelatedTable is an alias for viewmodel.RelatedTable.
	RelatedTable = viewmodel.RelatedTable
	// RelatedTablesResolver is an alias for viewmodel.RelatedTablesResolver.
	RelatedTablesResolver = viewmodel.RelatedTablesResolver
	// SelectedRowField is an alias for viewmodel.SelectedRowField.
	SelectedRowField = viewmodel.SelectedRowField
	// TableInfo is an alias for viewmodel.TableInfo.
	TableInfo = viewmodel.TableInfo
	// TableViewModel is an alias for viewmodel.TableViewModel.
	TableViewModel = viewmodel.TableViewModel
	// TimingEntry is an alias for viewmodel.TimingEntry.
	TimingEntry = viewmodel.TimingEntry
	// URLResolver is an alias for viewmodel.URLResolver.
	URLResolver = viewmodel.URLResolver
	// ValidationError is an alias for viewmodel.ValidationError.
	ValidationError = viewmodel.ValidationError
	// View is an alias for viewmodel.View.
	View = viewmodel.View
)

// BuildAddColumnAndJoinURL forwards to viewmodel.BuildAddColumnAndJoinURL.
var BuildAddColumnAndJoinURL = viewmodel.BuildAddColumnAndJoinURL

// BuildAddColumnURL forwards to viewmodel.BuildAddColumnURL.
var BuildAddColumnURL = viewmodel.BuildAddColumnURL

// BuildToggleColumnURL forwards to viewmodel.BuildToggleColumnURL.
var BuildToggleColumnURL = viewmodel.BuildToggleColumnURL

// BuildToggleExpansionURL forwards to viewmodel.BuildToggleExpansionURL.
var BuildToggleExpansionURL = viewmodel.BuildToggleExpansionURL

// BuildToggleGroupingURL forwards to viewmodel.BuildToggleGroupingURL.
var BuildToggleGroupingURL = viewmodel.BuildToggleGroupingURL

// BuildToggleJoinedURL forwards to viewmodel.BuildToggleJoinedURL.
var BuildToggleJoinedURL = viewmodel.BuildToggleJoinedURL

// BuildViewModel forwards to viewmodel.BuildViewModel.
var BuildViewModel = viewmodel.BuildViewModel

// GetOrCreateTableView forwards to viewmodel.GetOrCreateTableView.
var GetOrCreateTableView = viewmodel.GetOrCreateTableView

// ParseExpandedPaths forwards to viewmodel.ParseExpandedPaths.
var ParseExpandedPaths = viewmodel.ParseExpandedPaths

// ParseJoinedPaths forwards to viewmodel.ParseJoinedPaths.
var ParseJoinedPaths = viewmodel.ParseJoinedPaths

// ProcessJoinsAndUpdateColumns forwards to viewmodel.ProcessJoinsAndUpdateColumns.
var ProcessJoinsAndUpdateColumns = viewmodel.ProcessJoinsAndUpdateColumns
