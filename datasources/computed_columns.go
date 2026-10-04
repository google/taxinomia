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

package datasources

import (
	"fmt"

	"github.com/google/taxinomia/core/expr"
	"github.com/google/taxinomia/core/tables"
)

// applyComputedColumns declares the source's computed_columns on the loaded
// table. Nothing is evaluated here: each view evaluates them on read, like
// computed columns added in the UI. A definition that does not parse, or
// whose name is invalid or taken, fails the load, so a broken configuration
// is reported at startup instead of as an empty column.
func applyComputedColumns(source *DataSource, table *tables.DataTable) error {
	declared := source.GetComputedColumns()
	if len(declared) == 0 {
		return nil
	}
	defs := make([]tables.ComputedDefinition, 0, len(declared))
	for _, c := range declared {
		if _, err := expr.Compile(c.GetExpression()); err != nil {
			return fmt.Errorf("computed column %q: %v", c.GetName(), err)
		}
		defs = append(defs, tables.ComputedDefinition{
			Name:        c.GetName(),
			DisplayName: c.GetDisplayName(),
			Expression:  c.GetExpression(),
			EntityType:  c.GetEntityType(),
		})
	}
	return table.SetComputedDefinitions(defs)
}
