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

// Package protoloader provides functionality to load protobuf data with
// dynamic schema discovery. It uses a pre-populated proto registry to parse
// textproto or binary protobuf content into DataTables.
package protoloader

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Loader handles loading textproto files into DataTables using a pre-populated registry.
type Loader struct {
	registry *protoregistry.Files
}

// ColumnMetadata defines metadata for a single column.
// Used to override display name and entity type when creating DataTables.
type ColumnMetadata struct {
	Name        string // Column name (for matching)
	DisplayName string // Display name shown in UI
	EntityType  string // Entity type for join support
}

// NewLoader creates a new Loader with the given proto registry.
// The registry should be pre-populated with all required message descriptors.
func NewLoader(registry *protoregistry.Files) *Loader {
	return &Loader{
		registry: registry,
	}
}

// ParseTextproto parses textproto content from bytes into a dynamic protobuf message.
func (l *Loader) ParseTextproto(data []byte, messageName string) (protoreflect.Message, error) {
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

	// Use a resolver that can resolve types from our registry
	opts := prototext.UnmarshalOptions{
		Resolver: l,
	}
	if err := opts.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}

	return msg.ProtoReflect(), nil
}

// ParseBinaryProto parses binary protobuf content from bytes into a dynamic protobuf message.
func (l *Loader) ParseBinaryProto(data []byte, messageName string) (protoreflect.Message, error) {
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

	// Use proto.UnmarshalOptions with our resolver
	opts := proto.UnmarshalOptions{
		Resolver: l,
	}
	if err := opts.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to parse binary protobuf: %w", err)
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

			if fd.Kind() == protoreflect.MessageKind && fd.Cardinality() == protoreflect.Repeated && !isTimestampField(fd) {
				// This is the next level in the hierarchy (but not Timestamp which is treated as scalar)
				level.FieldDesc = fd
				nextLevel = fd.Message()
			} else if fd.Kind() != protoreflect.MessageKind {
				// Scalar field - include as column
				level.ScalarFields = append(level.ScalarFields, fd)
			} else if isTimestampField(fd) {
				// Timestamp messages are treated as scalar fields
				level.ScalarFields = append(level.ScalarFields, fd)
			}
		}

		levels = append(levels, level)
		current = nextLevel
	}

	return levels
}

// RowBuilder accumulates denormalized rows from a hierarchical message.
type RowBuilder struct {
	columns        []string                       // Column names in order
	fieldDescs     []protoreflect.FieldDescriptor // Field descriptors for each column
	rows           [][]any                        // All extracted rows (typed values)
	current        map[string]any                 // Current row being built (typed values)
	columnsByLevel [][]string                     // Column names grouped by hierarchy level
}

// Rows returns all extracted rows.
func (rb *RowBuilder) Rows() [][]any {
	return rb.rows
}

// ColumnNames returns column names in order.
func (rb *RowBuilder) ColumnNames() []string {
	return rb.columns
}

// FieldDescs returns field descriptors for each column.
func (rb *RowBuilder) FieldDescs() []protoreflect.FieldDescriptor {
	return rb.fieldDescs
}

// newRowBuilder creates a new RowBuilder with columns derived from hierarchy levels.
func newRowBuilder(hierarchy []HierarchyLevel) *RowBuilder {
	rb := &RowBuilder{
		current:        make(map[string]any),
		columnsByLevel: make([][]string, len(hierarchy)),
	}

	// Build column list from all levels
	for i, level := range hierarchy {
		for _, fd := range level.ScalarFields {
			colName := string(fd.Name())
			rb.columns = append(rb.columns, colName)
			rb.fieldDescs = append(rb.fieldDescs, fd)
			rb.columnsByLevel[i] = append(rb.columnsByLevel[i], colName)
		}
	}

	return rb
}

// clearFromLevel clears all column values at and below the given hierarchy level.
func (rb *RowBuilder) clearFromLevel(level int) {
	for i := level; i < len(rb.columnsByLevel); i++ {
		for _, col := range rb.columnsByLevel[i] {
			rb.current[col] = nil
		}
	}
}

// emitRow adds the current row state to the rows list.
func (rb *RowBuilder) emitRow() {
	row := make([]any, len(rb.columns))
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
		rb.current[string(fd.Name())] = extractTypedValue(val, fd)
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

// extractTypedValue extracts a typed value from a protoreflect.Value.
// Returns the appropriate Go type for use with typed columns.
func extractTypedValue(val protoreflect.Value, fd protoreflect.FieldDescriptor) any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return val.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		// Store as int64 for consistency
		return val.Int()
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return val.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint32(val.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return val.Uint()
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return val.Float()
	case protoreflect.StringKind:
		return val.String()
	case protoreflect.BytesKind:
		return string(val.Bytes())
	case protoreflect.EnumKind:
		// Return enum name as string
		enumVal := fd.Enum().Values().ByNumber(val.Enum())
		if enumVal != nil {
			return string(enumVal.Name())
		}
		return fmt.Sprintf("%d", val.Enum())
	case protoreflect.MessageKind:
		// Handle google.protobuf.Timestamp
		if isTimestampField(fd) {
			return extractTimestamp(val.Message())
		}
		return val.String()
	default:
		return val.String()
	}
}

// extractTimestamp extracts a time.Time from a google.protobuf.Timestamp message
func extractTimestamp(msg protoreflect.Message) time.Time {
	fields := msg.Descriptor().Fields()
	secondsField := fields.ByName("seconds")
	nanosField := fields.ByName("nanos")

	if secondsField == nil {
		return time.Time{}
	}

	seconds := msg.Get(secondsField).Int()
	nanos := int64(0)
	if nanosField != nil {
		nanos = msg.Get(nanosField).Int()
	}

	return time.Unix(seconds, nanos).UTC()
}

// isTimestampField checks if a field is a google.protobuf.Timestamp
func isTimestampField(fd protoreflect.FieldDescriptor) bool {
	if fd.Kind() != protoreflect.MessageKind {
		return false
	}
	return fd.Message().FullName() == "google.protobuf.Timestamp"
}

// CreateDataTable creates a DataTable from extracted rows.
// Creates typed columns based on protobuf field types.
func (l *Loader) CreateDataTable(rb *RowBuilder) *tables.DataTable {
	return l.CreateDataTableWithMetadata(rb, nil)
}

// CreateDataTableWithMetadata creates a DataTable from extracted rows with custom column metadata.
// The columnMeta map is keyed by column name and provides display name and entity type overrides.
func (l *Loader) CreateDataTableWithMetadata(rb *RowBuilder, columnMeta map[string]*ColumnMetadata) *tables.DataTable {
	table := tables.NewDataTable()

	// Create appropriately typed columns based on field descriptors
	for i, colName := range rb.columns {
		fd := rb.fieldDescs[i]

		// Get display name and entity type from metadata, or use defaults
		displayName := colName
		entityType := ""
		if meta, ok := columnMeta[colName]; ok && meta != nil {
			if meta.DisplayName != "" {
				displayName = meta.DisplayName
			}
			entityType = meta.EntityType
		}
		colDef := columns.NewColumnDef(colName, displayName, entityType)

		switch fd.Kind() {
		case protoreflect.BoolKind:
			col := columns.NewBoolColumn(colDef)
			for _, row := range rb.rows {
				if v, ok := row[i].(bool); ok {
					col.Append(v)
				} else {
					col.Append(false)
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
			protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			col := columns.NewInt64Column(colDef)
			for _, row := range rb.rows {
				if v, ok := row[i].(int64); ok {
					col.Append(v)
				} else {
					col.Append(0)
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			col := columns.NewUint32Column(colDef)
			for _, row := range rb.rows {
				if v, ok := row[i].(uint32); ok {
					col.Append(v)
				} else {
					col.Append(uint32(0))
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			col := columns.NewUint64Column(colDef)
			for _, row := range rb.rows {
				if v, ok := row[i].(uint64); ok {
					col.Append(v)
				} else {
					col.Append(uint64(0))
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case protoreflect.FloatKind, protoreflect.DoubleKind:
			col := columns.NewFloat64Column(colDef)
			for _, row := range rb.rows {
				if v, ok := row[i].(float64); ok {
					col.Append(v)
				} else {
					col.Append(0.0)
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case protoreflect.MessageKind:
			if isTimestampField(fd) {
				col := columns.NewDatetimeColumn(colDef)
				for _, row := range rb.rows {
					if v, ok := row[i].(time.Time); ok {
						col.Append(v)
					} else {
						col.Append(time.Time{})
					}
				}
				col.FinalizeColumn()
				table.AddColumn(col)
			} else {
				// Fallback to string for other message types
				col := columns.NewStringColumn(colDef)
				for _, row := range rb.rows {
					if v, ok := row[i].(string); ok {
						col.Append(v)
					} else {
						col.Append("")
					}
				}
				col.FinalizeColumn()
				table.AddColumn(col)
			}

		default:
			// String, bytes, enum, and other types use StringColumn
			col := columns.NewStringColumn(colDef)
			for _, row := range rb.rows {
				if v, ok := row[i].(string); ok {
					col.Append(v)
				} else if row[i] != nil {
					col.Append(fmt.Sprintf("%v", row[i]))
				} else {
					col.Append("")
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)
		}
	}

	return table
}

// LoadTextprotoAsTable loads textproto content from bytes and returns a denormalized DataTable.
// The messageName should be the fully qualified protobuf message name (e.g., "mypackage.Customer").
func (l *Loader) LoadTextprotoAsTable(data []byte, messageName string) (*tables.DataTable, error) {
	return l.LoadTextprotoAsTableWithMetadata(data, messageName, nil)
}

// LoadTextprotoAsTableWithMetadata loads textproto content with custom column metadata.
func (l *Loader) LoadTextprotoAsTableWithMetadata(data []byte, messageName string, columnMeta map[string]*ColumnMetadata) (*tables.DataTable, error) {
	msg, err := l.ParseTextproto(data, messageName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}

	hierarchy := l.FindLinearHierarchy(msg.Descriptor())
	rb := l.ExtractRows(msg, hierarchy)

	if len(rb.rows) == 0 {
		return nil, fmt.Errorf("no rows extracted from textproto")
	}

	return l.CreateDataTableWithMetadata(rb, columnMeta), nil
}

// LoadBinaryProtoAsTable loads binary protobuf content from bytes and returns a denormalized DataTable.
// The messageName should be the fully qualified protobuf message name (e.g., "mypackage.Customer").
func (l *Loader) LoadBinaryProtoAsTable(data []byte, messageName string) (*tables.DataTable, error) {
	return l.LoadBinaryProtoAsTableWithMetadata(data, messageName, nil)
}

// LoadBinaryProtoAsTableWithMetadata loads binary protobuf content with custom column metadata.
func (l *Loader) LoadBinaryProtoAsTableWithMetadata(data []byte, messageName string, columnMeta map[string]*ColumnMetadata) (*tables.DataTable, error) {
	msg, err := l.ParseBinaryProto(data, messageName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse binary protobuf: %w", err)
	}

	hierarchy := l.FindLinearHierarchy(msg.Descriptor())
	rb := l.ExtractRows(msg, hierarchy)

	if len(rb.rows) == 0 {
		return nil, fmt.Errorf("no rows extracted from binary protobuf")
	}

	return l.CreateDataTableWithMetadata(rb, columnMeta), nil
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

// ColumnType represents the data type of a column discovered from proto.
type ColumnType int

const (
	ColTypeString ColumnType = iota
	ColTypeInt64
	ColTypeUint32
	ColTypeUint64
	ColTypeFloat64
	ColTypeBool
	ColTypeDatetime
)

// DiscoveredColumn represents a column discovered from a proto schema.
type DiscoveredColumn struct {
	Name string
	Type ColumnType
}

// DiscoverSchema discovers the table schema from a proto message descriptor.
// Returns column names and types without loading any data.
func (l *Loader) DiscoverSchema(messageName string) ([]DiscoveredColumn, error) {
	// Find the message descriptor in the registry
	desc, err := l.registry.FindDescriptorByName(protoreflect.FullName(messageName))
	if err != nil {
		return nil, fmt.Errorf("message %q not found in registry: %w", messageName, err)
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%q is not a message type", messageName)
	}

	// Find the hierarchy and collect columns
	hierarchy := l.FindLinearHierarchy(msgDesc)

	var columns []DiscoveredColumn
	for _, level := range hierarchy {
		for _, fd := range level.ScalarFields {
			col := DiscoveredColumn{
				Name: string(fd.Name()),
				Type: fieldDescToColumnType(fd),
			}
			columns = append(columns, col)
		}
	}

	return columns, nil
}

// fieldDescToColumnType maps a protobuf field descriptor to a column type.
func fieldDescToColumnType(fd protoreflect.FieldDescriptor) ColumnType {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return ColTypeBool
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return ColTypeInt64
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return ColTypeUint32
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return ColTypeUint64
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return ColTypeFloat64
	case protoreflect.MessageKind:
		if isTimestampField(fd) {
			return ColTypeDatetime
		}
		return ColTypeString
	default:
		return ColTypeString
	}
}
