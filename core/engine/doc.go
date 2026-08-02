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

// Package engine defines the contract between the data engine and any
// presentation layer: a declarative Request in, a neutral Result out, and a
// Catalog describing what exists.
//
// The types here are transport-agnostic plain data. They know nothing about
// HTTP, HTML, URLs or display formatting. A web layer builds a Request from a
// URL; a CLI or RPC layer could build one too. The dependency rule is
// one-directional: presentation imports this package, never the reverse. This
// package must not import any presentation package or web library.
//
// A Result is a window, not a result set: the visible group nodes with their
// aggregates and the visible leaf page. State returned to the caller scales
// with the number of distinct values and the size of the viewport, never with
// the number of rows.
package engine

import "context"

// Engine is the seam between storage/compute and presentation. It answers
// declarative queries and describes the available data as plain metadata.
type Engine interface {
	// Query computes the window described by req. The context carries
	// cancellation: a superseded query must stop consuming resources as soon
	// as its viewport is obsolete.
	Query(ctx context.Context, req Request) (*Result, error)

	// Catalog describes the tables, hierarchies, entity types and joins that
	// exist, as data — the caller builds navigation from it.
	Catalog() *Catalog
}
