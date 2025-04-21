package main

import (
	"fmt"
	"strings"
	"errors"
)

type Unsigned interface {
	uint8 | uint16 | uint32
}

func NewColumnDef(name string, displayName string) (*ColumnDef, error) {
	colDef := ColumnDef{}
	for _, c := range name {  
		if !strings.Contains("abcdefghijklmnopqrstuvwxyz-_", strings.ToLower(string(c))) {
		    return nil, errors.New("Invalid character in name:" +  "'" + string(c) + "'")
		}
	}	
	colDef.name = name
	colDef.displayName = displayName
	colDef.valueToKey = make(map[string]uint32)
	colDef.keyToValue = make(map[uint32]string)	
	return &colDef, nil
}

// Column oriented data storage
// Values are stored by their integer index/key into a map of strings
type ColumnDef struct {
	name        string // allowed charcaters are a-z, A-Z, 0-9, - _
	displayName string
	valueToKey  map[string]uint32 // maps a string to the uint32 value (key) that will be stored in the column's data
	keyToValue  map[uint32]string // maps the key to a string
}

type Column[T Unsigned] struct {
	columnDef *ColumnDef
	data      []T
}

type IColumn interface {
	ColumnDef() *ColumnDef
}

func NewColumn[T Unsigned](columnDef *ColumnDef) *Column[T] {
	c := Column[T]{columnDef, []T{}}
	return &c
}

func (c *Column[T]) Append(value string) {
	k, x := c.columnDef.valueToKey[value]
	if !x {
		key := uint32(len(c.columnDef.valueToKey))
		c.columnDef.valueToKey[value] = key
		c.columnDef.keyToValue[key] = value
		k = key
	}
	c.data = append(c.data, T(k))
}

func Test() {
    colDef, err := NewColumnDef("@", "A")
	if err == nil {
		panic("Call was expected to return an error")
	}
    colDef, _ = NewColumnDef("a-_", "A")
	col := NewColumn[uint8](colDef)
	col.Append("1")
	col.Append("2")
	if len(col.data) != 2 {
	   fmt.Println("Expected 2, got", len(col.data))	
	   panic("Length error")
	}
	if len(colDef.valueToKey) != 2 {
		fmt.Println("Expected 2, got", len(colDef.valueToKey))	
		panic("Length error")
	}
	if len(colDef.keyToValue) != 2 {
		fmt.Println("Expected 2, got", len(colDef.keyToValue	))	
		panic("Length error")
	}
	col.Append("2")
	if len(col.data) != 3 {
		fmt.Println("Expected 3, got", len(col.data))	
		panic("Length error")
	 }
	 if len(colDef.valueToKey) != 2 {
		 fmt.Println("Expected 2, got", len(colDef.valueToKey))	
		 panic("Length error")
	 }
	 if len(colDef.keyToValue) != 2 {
		 fmt.Println("Expected 2, got", len(colDef.keyToValue	))	
		 panic("Length error")
	 }
}