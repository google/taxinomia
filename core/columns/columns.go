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

import (
	"fmt"
)

type Unsigned interface {
	uint8 | uint16 | uint32
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
	GetString(i int) (string, error)
	IsKey() bool
}

type DataColumn[T any] struct {
	IDataColumn
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

func (c *DataColumn[T]) GetKey(i int) (uint32, error) {
	if i < 0 || i >= len(c.data) {
		return 0, fmt.Errorf("index %d out of range [0:%d)", i, len(c.data))
	}
	// For DataColumn[uint32], T is already uint32
	var zero T
	switch any(zero).(type) {
	case uint32:
		return any(c.data[i]).(uint32), nil
	case uint16:
		return uint32(any(c.data[i]).(uint16)), nil
	case uint8:
		return uint32(any(c.data[i]).(uint8)), nil
	default:
		return 0, fmt.Errorf("unsupported type for GetKey")
	}
}

func (c *DataColumn[T]) ColumnDef() *ColumnDef {
	return c.columnDef
}

func (c *DataColumn[T]) append(v T) {
	c.data = append(c.data, v)
}
