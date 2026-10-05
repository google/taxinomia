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

package navigation

import (
	"strings"
	"testing"
)

func pathString(paths []HierarchyPath) string {
	var parts []string
	for _, p := range paths {
		var cols []string
		for _, l := range p.Levels {
			cols = append(cols, l.Column)
		}
		parts = append(parts, p.Name+":"+strings.Join(cols, ","))
	}
	return strings.Join(parts, " ")
}

func TestTableHierarchyPaths(t *testing.T) {
	n := NewNavigator(testCatalog())
	cases := []struct {
		name  string
		table string
		cols  map[string]string
		want  string
	}{
		{
			// The key level (machine) and below are left out.
			name:  "levels above the key",
			table: "google_machines",
			cols:  map[string]string{"machine": "google.machine", "cluster": "google.cluster", "zone": "google.zone", "region": "google.region", "cpu": ""},
			want:  "google.infrastructure:region,zone,cluster",
		},
		{
			name:  "a level without a column is skipped",
			table: "google_machines",
			cols:  map[string]string{"machine": "google.machine", "cluster": "google.cluster", "region": "google.region"},
			want:  "google.infrastructure:region,cluster",
		},
		{
			name:  "fewer than two levels is no path",
			table: "google_clusters",
			cols:  map[string]string{"cluster": "google.cluster", "zone": "google.zone"},
			want:  "",
		},
		{
			// No key in the hierarchy: every level with a column counts,
			// and the plain column wins over the ancestor column (§).
			name:  "table outside the hierarchy",
			table: "unknown",
			cols:  map[string]string{"§region": "google.region", "region": "google.region", "zone": "google.zone", "cell": "google.cell", "job": "google.job"},
			want:  "google.infrastructure:region,zone google.workloads:cell,job",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pathString(n.TableHierarchyPaths(tc.table, tc.cols)); got != tc.want {
				t.Errorf("paths = %q, want %q", got, tc.want)
			}
		})
	}
}
