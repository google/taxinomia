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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/protoloader"
	"github.com/google/taxinomia/core/tables"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ProtoLoader implements DataSourceLoader for protobuf files.
//
// Required config keys:
//   - proto_file: Path to the data file (.textproto or .binpb)
//   - message_type: Fully qualified proto message name
//
// Optional config keys:
//   - descriptor_set: Path to the .pb descriptor set file
//   - format: "textproto" or "binary" (inferred from extension if not specified)
type ProtoLoader struct {
	mu       sync.RWMutex
	registry *protoregistry.Files
	loader   *protoloader.Loader

	// Track loaded descriptor sets to avoid duplicates
	loadedDescriptors map[string]bool
}

// NewProtoLoader creates a new proto loader.
func NewProtoLoader() *ProtoLoader {
	registry := new(protoregistry.Files)
	return &ProtoLoader{
		registry:          registry,
		loader:            protoloader.NewLoader(registry),
		loadedDescriptors: make(map[string]bool),
	}
}

// SourceType returns "proto".
func (l *ProtoLoader) SourceType() string {
	return "proto"
}

// DiscoverSchema discovers the table schema from the proto descriptor.
func (l *ProtoLoader) DiscoverSchema(config map[string]string) (*TableSchema, error) {
	messageType := config["message_type"]
	if messageType == "" {
		return nil, fmt.Errorf("message_type is required")
	}

	// Load descriptor set if specified
	if descriptorSet := config["descriptor_set"]; descriptorSet != "" {
		if err := l.LoadDescriptorSet(descriptorSet); err != nil {
			return nil, fmt.Errorf("failed to load descriptor set: %w", err)
		}
	}

	// Discover schema from the proto message descriptor
	discoveredCols, err := l.loader.DiscoverSchema(messageType)
	if err != nil {
		return nil, err
	}

	// Convert to TableSchema
	schema := &TableSchema{
		Columns: make([]*ColumnSchema, len(discoveredCols)),
	}
	for i, col := range discoveredCols {
		schema.Columns[i] = &ColumnSchema{
			Name: col.Name,
			Type: protoColumnTypeToType(col.Type),
		}
	}

	return schema, nil
}

// protoColumnTypeToType converts protoloader.ColumnType to datasources.ColumnType
func protoColumnTypeToType(t protoloader.ColumnType) ColumnType {
	switch t {
	case protoloader.ColTypeString:
		return TypeString
	case protoloader.ColTypeInt64:
		return TypeInt64
	case protoloader.ColTypeUint32:
		return TypeUint32
	case protoloader.ColTypeUint64:
		return TypeUint64
	case protoloader.ColTypeFloat64:
		return TypeFloat64
	case protoloader.ColTypeBool:
		return TypeBool
	case protoloader.ColTypeDatetime:
		return TypeDatetime
	default:
		return TypeString
	}
}

// Load loads a protobuf file and returns a DataTable.
func (l *ProtoLoader) Load(config map[string]string, enrichedColumns []*EnrichedColumn) (*tables.DataTable, error) {
	protoFile := config["proto_file"]
	if protoFile == "" {
		return nil, fmt.Errorf("proto_file is required")
	}

	messageType := config["message_type"]
	if messageType == "" {
		return nil, fmt.Errorf("message_type is required")
	}

	// Load descriptor set if specified (may already be loaded from DiscoverSchema)
	if descriptorSet := config["descriptor_set"]; descriptorSet != "" {
		if err := l.LoadDescriptorSet(descriptorSet); err != nil {
			return nil, fmt.Errorf("failed to load descriptor set: %w", err)
		}
	}

	// Determine format
	format := config["format"]
	if format == "" {
		if strings.HasSuffix(protoFile, ".textproto") {
			format = "textproto"
		} else {
			format = "binary"
		}
	}

	// Read proto file
	data, err := os.ReadFile(protoFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read proto file: %w", err)
	}

	// Parse the message
	var msg protoreflect.Message
	switch format {
	case "textproto":
		msg, err = l.loader.ParseTextproto(data, messageType)
	case "binary":
		msg, err = l.loader.ParseBinaryProto(data, messageType)
	default:
		return nil, fmt.Errorf("unknown format: %s (expected 'textproto' or 'binary')", format)
	}
	if err != nil {
		return nil, err
	}

	// Extract rows
	hierarchy := l.loader.FindLinearHierarchy(msg.Descriptor())
	rb := l.loader.ExtractRows(msg, hierarchy)

	if len(rb.Rows()) == 0 {
		return nil, fmt.Errorf("no rows extracted from protobuf")
	}

	// Create table with enriched columns
	return l.createTableFromRows(rb, enrichedColumns), nil
}

// createTableFromRows creates a DataTable from extracted rows using enriched column definitions.
func (l *ProtoLoader) createTableFromRows(rb *protoloader.RowBuilder, enrichedColumns []*EnrichedColumn) *tables.DataTable {
	table := tables.NewDataTable()

	// Build a map for quick lookup
	enrichedMap := make(map[string]*EnrichedColumn)
	for _, col := range enrichedColumns {
		enrichedMap[col.Name] = col
	}

	rows := rb.Rows()
	colNames := rb.ColumnNames()
	fieldDescs := rb.FieldDescs()

	for i, colName := range colNames {
		enriched := enrichedMap[colName]
		if enriched == nil {
			// Should not happen if DiscoverSchema was called first
			enriched = &EnrichedColumn{
				Name:        colName,
				Type:        TypeString,
				DisplayName: colName,
			}
		}

		colDef := columns.NewColumnDef(enriched.Name, enriched.DisplayName, enriched.EntityType)
		fd := fieldDescs[i]

		switch enriched.Type {
		case TypeBool:
			col := columns.NewBoolColumn(colDef)
			for _, row := range rows {
				if v, ok := row[i].(bool); ok {
					col.Append(v)
				} else {
					col.Append(false)
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case TypeInt64:
			col := columns.NewInt64Column(colDef)
			for _, row := range rows {
				if v, ok := row[i].(int64); ok {
					col.Append(v)
				} else {
					col.Append(0)
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case TypeUint32:
			col := columns.NewUint32Column(colDef)
			for _, row := range rows {
				if v, ok := row[i].(uint32); ok {
					col.Append(v)
				} else {
					col.Append(uint32(0))
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case TypeUint64:
			col := columns.NewUint64Column(colDef)
			for _, row := range rows {
				if v, ok := row[i].(uint64); ok {
					col.Append(v)
				} else {
					col.Append(uint64(0))
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case TypeFloat64:
			col := columns.NewFloat64Column(colDef)
			for _, row := range rows {
				if v, ok := row[i].(float64); ok {
					col.Append(v)
				} else {
					col.Append(0.0)
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		case TypeDatetime:
			col := columns.NewDatetimeColumn(colDef)
			for _, row := range rows {
				if v, ok := row[i].(time.Time); ok {
					col.Append(v)
				} else {
					col.Append(time.Time{})
				}
			}
			col.FinalizeColumn()
			table.AddColumn(col)

		default:
			// String column (including enums)
			col := columns.NewStringColumn(colDef)
			for _, row := range rows {
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

		// Handle case where we need to check the field descriptor for types not in enriched
		_ = fd // field descriptor available if needed
	}

	return table
}

// LoadDescriptorSet loads a .pb descriptor set file into the registry.
func (l *ProtoLoader) LoadDescriptorSet(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Skip if already loaded
	if l.loadedDescriptors[path] {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read descriptor set: %w", err)
	}

	if err := l.loadDescriptorSetFromBytes(data); err != nil {
		return err
	}

	l.loadedDescriptors[path] = true
	return nil
}

// LoadDescriptorSetFromBytes loads a descriptor set from raw bytes.
func (l *ProtoLoader) LoadDescriptorSetFromBytes(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.loadDescriptorSetFromBytes(data)
}

func (l *ProtoLoader) loadDescriptorSetFromBytes(data []byte) error {
	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(data, fds); err != nil {
		return fmt.Errorf("failed to unmarshal descriptor set: %w", err)
	}

	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return fmt.Errorf("failed to create file descriptors: %w", err)
	}

	// Register each file in our registry
	var registerErr error
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		// Check if already registered to avoid duplicates
		if _, err := l.registry.FindFileByPath(fd.Path()); err == nil {
			return true // Already registered, skip
		}
		if err := l.registry.RegisterFile(fd); err != nil {
			registerErr = err
			return false
		}
		return true
	})

	return registerErr
}

// GetRegisteredMessages returns all message names registered in the loader.
func (l *ProtoLoader) GetRegisteredMessages() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.loader.GetRegisteredMessages()
}
