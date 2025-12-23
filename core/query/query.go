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

package query

import (
	"net/url"
	"strings"

	"github.com/google/safehtml"
)

// ParseColumns parses the columns parameter from a URL query string
func ParseColumns(query url.Values, defaultColumns []string) []string {
	columnsParam := query.Get("columns")
	if columnsParam != "" {
		return strings.Split(columnsParam, ",")
	}
	return defaultColumns
}

// ToggleColumnURL creates a URL that toggles the visibility of a specific column
func ToggleColumnURL(currentColumns []string, toggleColumn string) safehtml.URL {
	// Create a new column list with the toggled column
	newCols := []string{}
	found := false

	// Check if column exists in current list
	for _, col := range currentColumns {
		if col == toggleColumn {
			found = true
		} else {
			newCols = append(newCols, col)
		}
	}

	// If not found, add it
	if !found {
		newCols = append(newCols, toggleColumn)
	}

	// Build query string
	if len(newCols) > 0 {
		query := "?columns=" + strings.Join(newCols, ",")
		return safehtml.URLSanitized(query)
	}
	return safehtml.URLSanitized("/")
}