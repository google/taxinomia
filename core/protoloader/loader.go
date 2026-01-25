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

// Package protoloader provides functionality to load textproto files with
// dynamic schema discovery. It uses a pre-populated proto registry to parse
// textproto files into DataTables.
package protoloader

import (
	"fmt"
	"os"
	"strings"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Loader handles loading textproto files into DataTables using a pre-populated registry.
type Loader struct {
	registry *protoregistry.Files
}

// NewLoader creates a new Loader with the given proto registry.
// The registry should be pre-populated with all required message descriptors.
func NewLoader(registry *protoregistry.Files) *Loader {
	return &Loader{
		registry: registry,
	}
}

// ParseTextproto parses a textproto file into a dynamic protobuf message.
func (l *Loader) ParseTextproto(path string, messageName string) (protoreflect.Message, error) {
	// Find the message descriptor in the registry
	desc, err := l.registry.FindDescriptorByName(protoreflect.FullName(messageName))
	if err != nil {
		return nil, fmt.Errorf("message %q not found in registry: %w", messageName, err)
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message type", messageName)
	}

	// Create a dynamic message instance
	msg := dynamicpb.NewMessage(msgDesc)

	// Read and parse the textproto file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read textproto file: %w", err)
	}

	// Use a resolver that can resolve types from our registry
	opts := prototext.UnmarshalOptions{
		Resolver: l,
	}
	if err := opts.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}

	return msg.ProtoReflect(), nil
}

// FindMessageByName implements protoregistry.MessageTypeResolver
func (l *Loader) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	desc, err := l.registry.FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message type", name)
	}
	return dynamicpb.NewMessageType(msgDesc), nil
}

// FindMessageByURL implements protoregistry.MessageTypeResolver
func (l *Loader) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	// Strip any leading type.googleapis.com/ prefix
	name := protoreflect.FullName(strings.TrimPrefix(url, "type.googleapis.com/"))
	return l.FindMessageByName(name)
}

// FindExtensionByName implements protoregistry.ExtensionTypeResolver
func (l *Loader) FindExtensionByName(name protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return nil, protoregistry.NotFound
}

// FindExtensionByNumber implements protoregistry.ExtensionTypeResolver
func (l *Loader) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return nil, protoregistry.NotFound
}

// HierarchyLevel represents one level in a linear message hierarchy.
type HierarchyLevel struct {
	// FieldDesc is the repeated message field leading to the next level (nil for leaf)
	FieldDesc protoreflect.FieldDescriptor
	// ScalarFields are non-message, non-repeated fields at this level
	ScalarFields []protoreflect.FieldDescriptor
}

// FindLinearHierarchy walks a message descriptor to find a linear chain of nested repeated messages.
// Returns the hierarchy levels from root to leaf.
func (l *Loader) FindLinearHierarchy(msgDesc protoreflect.MessageDescriptor) []HierarchyLevel {
	var levels []HierarchyLevel
	current := msgDesc

	for current != nil {
		level := HierarchyLevel{}
		var nextLevel protoreflect.MessageDescriptor

		fields := current.Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)

			if fd.Kind() == protoreflect.MessageKind && fd.Cardinality() == protoreflect.Repeated {
				// This is the next level in the hierarchy
				level.FieldDesc = fd
				nextLevel = fd.Message()
			} else if fd.Kind() != protoreflect.MessageKind || fd.Cardinality() != protoreflect.Repeated {
				// Scalar field or singular message - include as column
				if fd.Kind() != protoreflect.MessageKind {
					level.ScalarFields = append(level.ScalarFields, fd)
				}
			}
		}

		levels = append(levels, level)
		current = nextLevel
	}

	return levels
}

// RowBuilder accumulates denormalized rows from a hierarchical message.
type RowBuilder struct {
	columns        []string            // Column names in order
	rows           [][]string          // All extracted rows
	current        map[string]string   // Current row being built
	columnsByLevel [][]string          // Column names grouped by hierarchy level
}

// newRowBuilder creates a new RowBuilder with columns derived from hierarchy levels.
func newRowBuilder(hierarchy []HierarchyLevel) *RowBuilder {
	rb := &RowBuilder{
		current:        make(map[string]string),
		columnsByLevel: make([][]string, len(hierarchy)),
	}

	// Build column list from all levels
	for i, level := range hierarchy {
		for _, fd := range level.ScalarFields {
			colName := string(fd.Name())
			rb.columns = append(rb.columns, colName)
			rb.columnsByLevel[i] = append(rb.columnsByLevel[i], colName)
		}
	}

	return rb
}

// clearFromLevel clears all column values at and below the given hierarchy level.
func (rb *RowBuilder) clearFromLevel(level int) {
	for i := level; i < len(rb.columnsByLevel); i++ {
		for _, col := range rb.columnsByLevel[i] {
			rb.current[col] = ""
		}
	}
}

// emitRow adds the current row state to the rows list.
func (rb *RowBuilder) emitRow() {
	row := make([]string, len(rb.columns))
	for i, col := range rb.columns {
		row[i] = rb.current[col]
	}
	rb.rows = append(rb.rows, row)
}

// ExtractRows walks a message hierarchy and extracts denormalized rows.
func (l *Loader) ExtractRows(msg protoreflect.Message, hierarchy []HierarchyLevel) *RowBuilder {
	rb := newRowBuilder(hierarchy)
	l.walkHierarchy(msg, hierarchy, 0, rb)
	return rb
}

// walkHierarchy recursively walks the message hierarchy, extracting values.
func (l *Loader) walkHierarchy(msg protoreflect.Message, hierarchy []HierarchyLevel, depth int, rb *RowBuilder) {
	if depth >= len(hierarchy) {
		return
	}

	level := hierarchy[depth]

	// Extract scalar values at this level
	for _, fd := range level.ScalarFields {
		val := msg.Get(fd)
		rb.current[string(fd.Name())] = formatValue(val, fd)
	}

	// If no more levels or no repeated field, emit a row
	if level.FieldDesc == nil || depth == len(hierarchy)-1 {
		rb.emitRow()
		return
	}

	// Recurse into the repeated field
	list := msg.Get(level.FieldDesc).List()
	if list.Len() == 0 {
		// No children - clear child fields and emit row
		rb.clearFromLevel(depth + 1)
		rb.emitRow()
		return
	}

	for i := 0; i < list.Len(); i++ {
		// Clear child fields before each iteration to avoid stale data
		rb.clearFromLevel(depth + 1)
		childMsg := list.Get(i).Message()
		l.walkHierarchy(childMsg, hierarchy, depth+1, rb)
	}
}

// formatValue converts a protoreflect.Value to its string representation.
func formatValue(val protoreflect.Value, fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		if val.Bool() {
			return "true"
		}
		return "false"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return fmt.Sprintf("%d", val.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return fmt.Sprintf("%d", val.Int())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return fmt.Sprintf("%d", val.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return fmt.Sprintf("%d", val.Uint())
	case protoreflect.FloatKind:
		return fmt.Sprintf("%g", val.Float())
	case protoreflect.DoubleKind:
		return fmt.Sprintf("%g", val.Float())
	case protoreflect.StringKind:
		return val.String()
	case protoreflect.BytesKind:
		return string(val.Bytes())
	case protoreflect.EnumKind:
		// Return enum name if available
		enumVal := fd.Enum().Values().ByNumber(val.Enum())
		if enumVal != nil {
			return string(enumVal.Name())
		}
		return fmt.Sprintf("%d", val.Enum())
	default:
		return val.String()
	}
}

// CreateDataTable creates a DataTable from extracted rows.
func (l *Loader) CreateDataTable(rb *RowBuilder) *tables.DataTable {
	table := tables.NewDataTable()

	// Create a StringColumn for each column
	for i, colName := range rb.columns {
		colDef := columns.NewColumnDef(colName, colName, "")
		col := columns.NewStringColumn(colDef)

		// Add values from each row
		for _, row := range rb.rows {
			col.Append(row[i])
		}

		col.FinalizeColumn()
		table.AddColumn(col)
	}

	return table
}

// LoadTextprotoAsTable loads a textproto file and returns a denormalized DataTable.
// The messageName should be the fully qualified protobuf message name (e.g., "mypackage.Customer").
func (l *Loader) LoadTextprotoAsTable(textprotoPath, messageName string) (*tables.DataTable, error) {
	msg, err := l.ParseTextproto(textprotoPath, messageName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}

	hierarchy := l.FindLinearHierarchy(msg.Descriptor())
	rb := l.ExtractRows(msg, hierarchy)

	if len(rb.rows) == 0 {
		return nil, fmt.Errorf("no rows extracted from textproto")
	}

	return l.CreateDataTable(rb), nil
}

// GetRegisteredMessages returns all message names registered in the loader.
func (l *Loader) GetRegisteredMessages() []string {
	var messages []string
	l.registry.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			messages = append(messages, string(msgs.Get(i).FullName()))
		}
		return true
	})
	return messages
}
