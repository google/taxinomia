/*
SPDX-License-Identifier: Apache-2.0

Copyright 2026 The Taxinomia Authors

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

package main

import (
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/tables"
)

// newModel registers the three tables; joins are auto-discovered from the
// entity types (fact.fk_dim ↔ dim.id via "bench.dim", dim.parent ↔ dim2.id
// via "bench.dim2").
func newModel(fact, dim, dim2 *tables.DataTable) *models.DataModel {
	dm := models.NewDataModel()
	dm.AddTable("dim2", dim2)
	dm.AddTable("dim", dim)
	dm.AddTable("fact", fact)
	return dm
}

// newFactOnlyModel registers just the fact table — schemas without join
// dimensions (simple6).
func newFactOnlyModel(fact *tables.DataTable) *models.DataModel {
	dm := models.NewDataModel()
	dm.AddTable("fact", fact)
	return dm
}
