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

// Package server is a forwarding package kept for import-path compatibility.
// The package moved to web/handlers. Every name here is an alias for the same
// type, so existing code keeps compiling and interoperates with code using
// the new path.
//
// Deprecated: import github.com/google/taxinomia/web/handlers instead. This
// forwarding package will be removed in a future cleanup release.
package server

import (
	"github.com/google/taxinomia/web/handlers"
)

// Resource limits to prevent abuse. See web/handlers.
const (
	MaxComputedColumns = handlers.MaxComputedColumns
	MaxFilters         = handlers.MaxFilters
	MaxGroupingLevels  = handlers.MaxGroupingLevels
)

type (
	// EntityTypeDescriptionResolver is an alias for handlers.EntityTypeDescriptionResolver.
	EntityTypeDescriptionResolver = handlers.EntityTypeDescriptionResolver
	// PrimaryKeyResolver is an alias for handlers.PrimaryKeyResolver.
	PrimaryKeyResolver = handlers.PrimaryKeyResolver
	// ProductConfig is an alias for handlers.ProductConfig.
	ProductConfig = handlers.ProductConfig
	// Server is an alias for handlers.Server.
	Server = handlers.Server
	// TableHandlerResult is an alias for handlers.TableHandlerResult.
	TableHandlerResult = handlers.TableHandlerResult
	// TimingCollector is an alias for handlers.TimingCollector.
	TimingCollector = handlers.TimingCollector
	// ValidationResult is an alias for handlers.ValidationResult.
	ValidationResult = handlers.ValidationResult
)

// NewServer forwards to handlers.NewServer.
var NewServer = handlers.NewServer

// NewTimingCollector forwards to handlers.NewTimingCollector.
var NewTimingCollector = handlers.NewTimingCollector

// NewValidationResult forwards to handlers.NewValidationResult.
var NewValidationResult = handlers.NewValidationResult
