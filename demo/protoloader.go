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

package demo

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/taxinomia/core/protoloader"
	"github.com/google/taxinomia/core/tables"
	"github.com/google/taxinomia/datasources"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ProtoTableLoader handles loading proto descriptors and textproto files.
// It manages the proto registry and uses the core protoloader for parsing.
type ProtoTableLoader struct {
	registry   *protoregistry.Files
	loader     *protoloader.Loader
	fileReader datasources.FileReader
	dirReader  datasources.DirReader
}

// NewProtoTableLoader creates a new ProtoTableLoader.
func NewProtoTableLoader() *ProtoTableLoader {
	registry := new(protoregistry.Files)
	return &ProtoTableLoader{
		registry: registry,
		loader:   protoloader.NewLoader(registry),
	}
}

// SetFileReader sets the file reader for loading files.
func (l *ProtoTableLoader) SetFileReader(reader datasources.FileReader) {
	l.fileReader = reader
}

// SetDirReader sets the directory reader for listing directories.
func (l *ProtoTableLoader) SetDirReader(reader datasources.DirReader) {
	l.dirReader = reader
}

// LoadDescriptorSet loads a .pb descriptor set file into the registry.
// Requires SetFileReader to be called first.
func (l *ProtoTableLoader) LoadDescriptorSet(path string) error {
	if l.fileReader == nil {
		return fmt.Errorf("file reader not set; call SetFileReader first")
	}
	data, err := l.fileReader(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return l.LoadDescriptorSetFromBytes(data)
}

// LoadDescriptorSetFromBytes loads a descriptor set from raw bytes.
func (l *ProtoTableLoader) LoadDescriptorSetFromBytes(data []byte) error {
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

// LoadDescriptorsFromDirectory loads all .pb (descriptor set) files from a directory.
// Requires SetFileReader and SetDirReader to be called first.
func (l *ProtoTableLoader) LoadDescriptorsFromDirectory(dirPath string) error {
	if l.dirReader == nil {
		return fmt.Errorf("directory reader not set; call SetDirReader first")
	}
	entries, err := l.dirReader(dirPath)
	if err != nil {
		return fmt.Errorf("failed to read descriptor directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if !strings.HasSuffix(entry.Name, ".pb") {
			continue
		}

		path := filepath.Join(dirPath, entry.Name)
		if err := l.LoadDescriptorSet(path); err != nil {
			return fmt.Errorf("failed to load %s: %w", entry.Name, err)
		}
	}

	return nil
}

// LoadTextprotoAsTable loads a textproto file and returns a denormalized DataTable.
// Requires SetFileReader to be called first.
func (l *ProtoTableLoader) LoadTextprotoAsTable(textprotoPath, messageName string) (*tables.DataTable, error) {
	if l.fileReader == nil {
		return nil, fmt.Errorf("file reader not set; call SetFileReader first")
	}
	data, err := l.fileReader(textprotoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read textproto file: %w", err)
	}
	return l.loader.LoadTextprotoAsTable(data, messageName)
}

// LoadTextprotoAsTableFromBytes loads textproto content from bytes and returns a denormalized DataTable.
func (l *ProtoTableLoader) LoadTextprotoAsTableFromBytes(data []byte, messageName string) (*tables.DataTable, error) {
	return l.loader.LoadTextprotoAsTable(data, messageName)
}

// LoadBinaryProtoAsTable loads a binary protobuf file and returns a denormalized DataTable.
// Requires SetFileReader to be called first.
func (l *ProtoTableLoader) LoadBinaryProtoAsTable(protoPath, messageName string) (*tables.DataTable, error) {
	if l.fileReader == nil {
		return nil, fmt.Errorf("file reader not set; call SetFileReader first")
	}
	data, err := l.fileReader(protoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read binary protobuf file: %w", err)
	}
	return l.loader.LoadBinaryProtoAsTable(data, messageName)
}

// LoadBinaryProtoAsTableFromBytes loads binary protobuf content from bytes and returns a denormalized DataTable.
func (l *ProtoTableLoader) LoadBinaryProtoAsTableFromBytes(data []byte, messageName string) (*tables.DataTable, error) {
	return l.loader.LoadBinaryProtoAsTable(data, messageName)
}

// LoadTextprotosFromDirectory loads all .textproto files from a directory.
// Each file should have a corresponding message name derived from the filename or config.
// Returns a map of table name to DataTable.
// Requires SetFileReader and SetDirReader to be called first.
func (l *ProtoTableLoader) LoadTextprotosFromDirectory(dirPath string, messageNameFn func(filename string) string) (map[string]*tables.DataTable, error) {
	if l.dirReader == nil {
		return nil, fmt.Errorf("directory reader not set; call SetDirReader first")
	}
	entries, err := l.dirReader(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read textproto directory: %w", err)
	}

	result := make(map[string]*tables.DataTable)

	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if !strings.HasSuffix(entry.Name, ".textproto") {
			continue
		}

		path := filepath.Join(dirPath, entry.Name)
		tableName := strings.TrimSuffix(entry.Name, ".textproto")
		messageName := messageNameFn(tableName)

		table, err := l.LoadTextprotoAsTable(path, messageName)
		if err != nil {
			return nil, fmt.Errorf("failed to load %s: %w", entry.Name, err)
		}

		result[tableName] = table
	}

	return result, nil
}

// GetRegisteredMessages returns all message names registered in the loader.
func (l *ProtoTableLoader) GetRegisteredMessages() []string {
	return l.loader.GetRegisteredMessages()
}

// LoadTextprotoAsTableWithMetadata loads a textproto file with custom column metadata.
// Requires SetFileReader to be called first.
func (l *ProtoTableLoader) LoadTextprotoAsTableWithMetadata(textprotoPath, messageName string, columnMeta map[string]*protoloader.ColumnMetadata) (*tables.DataTable, error) {
	if l.fileReader == nil {
		return nil, fmt.Errorf("file reader not set; call SetFileReader first")
	}
	data, err := l.fileReader(textprotoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read textproto file: %w", err)
	}
	return l.loader.LoadTextprotoAsTableWithMetadata(data, messageName, columnMeta)
}

// LoadBinaryProtoAsTableWithMetadata loads a binary protobuf file with custom column metadata.
// Requires SetFileReader to be called first.
func (l *ProtoTableLoader) LoadBinaryProtoAsTableWithMetadata(protoPath, messageName string, columnMeta map[string]*protoloader.ColumnMetadata) (*tables.DataTable, error) {
	if l.fileReader == nil {
		return nil, fmt.Errorf("file reader not set; call SetFileReader first")
	}
	data, err := l.fileReader(protoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read binary protobuf file: %w", err)
	}
	return l.loader.LoadBinaryProtoAsTableWithMetadata(data, messageName, columnMeta)
}
