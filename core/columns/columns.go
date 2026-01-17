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

package columns

import "errors"

// Display labels for special values
const (
	// ErrorLabel is the display string for values that couldn't be retrieved due to an error.
	ErrorLabel = "[error]"
	// UnmatchedLabel is the display string for values that couldn't be resolved through a join.
	UnmatchedLabel = "[unmatched]"
)

// ErrUnmatched is returned when a join lookup fails to find a matching row.
// This is an expected condition (not a bug) and should be displayed as UnmatchedLabel.
var ErrUnmatched = errors.New("no matching row in joined table")

type Unsigned interface {
	uint8 | uint16 | uint32
}

type IJoiner interface {
	Lookup(index uint32) (uint32, error)
}

type Joiner[T any] struct {
	IJoiner
	FromColumn IDataColumnT[T]
	ToColumn   IDataColumnT[T]
}
type JoinerString struct {
	Joiner[string]
}
type JoinerUint32 struct {
	Joiner[uint32]
}

func (j *Joiner[T]) Lookup(index uint32) (uint32, error) {
	v, err := j.FromColumn.GetValue(index)
	if err != nil {
		return 0, err
	}
	targetIndex, err := j.ToColumn.GetIndex(v)
	if err != nil {
		return 0, err
	}
	return targetIndex, nil
}

func (c *JoinerString) Lookup(index uint32) (uint32, error) {
	v, err := c.FromColumn.GetValue(index)
	if err != nil {
		return 0, err
	}
	targetIndex, err := c.ToColumn.GetIndex(v)
	if err != nil {
		return 0, err
	}
	return targetIndex, nil
}

func (c *JoinerUint32) Lookup(index uint32) (uint32, error) {
	v, err := c.FromColumn.GetValue(index)
	if err != nil {
		return 0, err
	}
	targetIndex, err := c.ToColumn.GetIndex(v)
	if err != nil {
		return 0, err
	}
	return targetIndex, nil
}

type IColumnDef interface {
	Name() string // must not contain any of the following characters: & = : ,
	DisplayName() string
	// entity type of the column, primary or foreign key
	EntityType() string
}

type ColumnDef struct {
	name        string // must not contain any of the following characters: & = : ,
	displayName string
	entityType  string
}

// NewColumnDef creates a new ColumnDef with the given name and display name
func NewColumnDef(name, displayName, entityType string) *ColumnDef {
	return &ColumnDef{
		name:        name,
		displayName: displayName,
		entityType:  entityType,
	}
}

func (cd *ColumnDef) Name() string {
	return cd.name
}

func (cd *ColumnDef) DisplayName() string {
	return cd.displayName
}

func (cd *ColumnDef) EntityType() string {
	return cd.entityType
}

type IDataColumn interface {
	ColumnDef() *ColumnDef
	Length() int
	GetString(i uint32) (string, error)
	IsKey() bool
	// NewJoiner(onColumn IDataColumn) IJoiner
	CreateJoinedColumn(columnDef *ColumnDef, joiner IJoiner) IJoinedDataColumn
	GroupIndices(indices []uint32, columnView *ColumnView) (map[uint32][]uint32, []uint32)
}

type IDataColumnT[T any] interface {
	IDataColumn
	GetValue(uint32) (T, error)
	GetIndex(T) (uint32, error)
}

type DataColumn[T any] struct {
	IDataColumnT[T]
	columnDef *ColumnDef
	data      []T
}

type Filterable[T any] interface {
	Filter(predicate func(T) bool) *[]bool
}

func (c DataColumn[T]) Filter(predicate func(T) bool) []int {
	switch any(c).(type) {
	case DataColumn[string]:
		col := any(c).(DataColumn[string])
		return col.Filter(any(predicate).(func(string) bool))
	case DataColumn[uint32]:
		col := any(c).(DataColumn[uint32])
		return col.Filter(any(predicate).(func(uint32) bool))
	}
	return []int{}
}

type IJoinedDataColumn interface {
	IDataColumn
	// might need to know the join specifics
	// fromTable.fromColunm -> toTable.toColumn
	// the fromColumn will have an index that maps values to indices in the fromTable, these indices can then be used to lookup values in the fromTable. displayed column
	// Joiner() IJoiner
}

type IJoinedColumn[T any] interface {
	IColumn[T]
}

type IColumn[T any] interface {
	ColumnDef() *ColumnDef
	Length() int
}

func NewColumn[T Unsigned](columnDef *ColumnDef) *DataColumn[T] {
	c := DataColumn[T]{
		columnDef: columnDef,
		data:      []T{},
	}
	return &c
}

func (c *DataColumn[T]) Length() int {
	return len(c.data)
}

func (c *DataColumn[T]) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *DataColumn[T]) append(v T) {
	c.data = append(c.data, v)
}

type ColumnView struct {
}
