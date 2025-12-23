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

package main

import "slices"

// GroupSorter handles all sorting logic for groups and indices
type GroupSorter struct {
	columnViews map[string]*ColumnView
}

// NewGroupSorter creates a new GroupSorter
func NewGroupSorter(columnViews map[string]*ColumnView) *GroupSorter {
	return &GroupSorter{
		columnViews: columnViews,
	}
}

// sortGroup represents a group to be sorted
type sortGroup struct {
	columnDef  *ColumnDef
	columnView *ColumnView
	key        uint32
	asc        bool
}

// indices represents indices to be sorted
type indices struct {
	columnDef  *ColumnDef
	columnView *ColumnView
	value      uint32
	indices    []uint32
	asc        bool
}

// SortGroups sorts a slice of groups using the groupKeyToOrder mapping
func (s *GroupSorter) SortGroups(groups []*sortGroup) {
	slices.SortFunc(groups, s.compareGroups)
}

// compareGroups compares two groups for sorting
func (s *GroupSorter) compareGroups(a *sortGroup, b *sortGroup) int {
	c := a.columnView.groupKeyToOrder[a.key]
	d := b.columnView.groupKeyToOrder[b.key]

	if c == d {
		return 0
	} else if c < d {
		return -1
	}
	return 1
}

// SortIndices sorts a slice of indices using the groupKeyToOrder mapping
func (s *GroupSorter) SortIndices(indices []*indices) {
	slices.SortFunc(indices, s.compareIndices)
}

// compareIndices compares two indices for sorting
func (s *GroupSorter) compareIndices(a *indices, b *indices) int {
	c := a.columnView.groupKeyToOrder[a.columnView.keyToGroupKey[a.value]]
	d := b.columnView.groupKeyToOrder[b.columnView.keyToGroupKey[b.value]]

	if c == d {
		return 0
	} else if c < d {
		return -1
	}
	return 1
}

// PrepareGroupsForSorting converts a Group's groups map into a sortable slice
func (s *GroupSorter) PrepareGroupsForSorting(g *Group) []*sortGroup {
	groups := []*sortGroup{}
	columnView := s.columnViews[g.columnDef.name]

	for k := range g.groups {
		groups = append(groups, &sortGroup{
			columnDef:  g.columnDef,
			columnView: columnView,
			key:        k,
			asc:        g.asc,
		})
	}

	return groups
}

// PrepareIndicesForSorting converts a Group's indices map into a sortable slice
func (s *GroupSorter) PrepareIndicesForSorting(g *Group) []*indices {
	indicesList := []*indices{}
	columnView := s.columnViews[g.columnDef.name]

	for k, v := range g.indices {
		indicesList = append(indicesList, &indices{
			columnDef:  g.columnDef,
			columnView: columnView,
			value:      k,
			indices:    v,
			asc:        g.asc,
		})
	}

	return indicesList
}