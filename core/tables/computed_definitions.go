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

package tables

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/taxinomia/core/aggregates"
	"github.com/google/taxinomia/core/columns"
)

// ComputedDefinition is a computed column declared as part of a table's
// definition (for example in its data source), as opposed to one a user adds
// to a single view. It is evaluated on read from the table's other columns,
// exactly like a view's computed columns; the table only carries the
// definition, and every view of the table gets the column.
type ComputedDefinition struct {
	Name        string // column name; must not clash with a stored column
	DisplayName string // shown in headers and the column pane; Name if empty
	Expression  string // expression over the table's columns (see the expression language)
	EntityType  string // optional entity type, for entity links on its values
}

// SetComputedDefinitions declares the table's computed columns, in order: a
// definition may refer to the stored columns and to computed columns declared
// before it. Names must be unique and must not clash with a stored column.
// Replaces any earlier declaration.
func (dt *DataTable) SetComputedDefinitions(defs []ComputedDefinition) error {
	seen := make(map[string]bool, len(defs))
	for _, d := range defs {
		if d.Name == "" {
			return fmt.Errorf("computed column without a name")
		}
		if strings.ContainsAny(d.Name, "&=:,;.") {
			return fmt.Errorf("computed column name %q contains one of & = : , ; .", d.Name)
		}
		if d.Expression == "" {
			return fmt.Errorf("computed column %q has no expression", d.Name)
		}
		if seen[d.Name] {
			return fmt.Errorf("computed column %q declared twice", d.Name)
		}
		if _, stored := dt.columns[d.Name]; stored {
			return fmt.Errorf("computed column %q has the name of a stored column", d.Name)
		}
		seen[d.Name] = true
	}
	dt.computed = append([]ComputedDefinition(nil), defs...)
	return nil
}

// ComputedDefinitions returns the table's declared computed columns, in
// declaration order (a copy).
func (dt *DataTable) ComputedDefinitions() []ComputedDefinition {
	return append([]ComputedDefinition(nil), dt.computed...)
}

// unreadableLabel is how a value that could not be read is shown and matched:
// columns.UnmatchedLabel for a joined row without a match, columns.ErrorLabel
// otherwise (a computed column whose expression fails on the row).
func unreadableLabel(err error) string {
	if errors.Is(err, columns.ErrUnmatched) {
		return columns.UnmatchedLabel
	}
	return columns.ErrorLabel
}

// countFailed records a row an aggregate had to leave out because its value
// could not be computed. A joined row without a match is not a failure (it
// has no value by design) and is not counted.
func countFailed(state *aggregates.NumericAggState, err error) {
	if !errors.Is(err, columns.ErrUnmatched) {
		state.Failed++
	}
}
