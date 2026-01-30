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

package csvimport

import (
	"fmt"

	"google.golang.org/protobuf/encoding/prototext"
)

// ParseTableSource parses a textproto string into a TableSource proto.
func ParseTableSource(textproto string) (*TableSource, error) {
	source := &TableSource{}
	if err := prototext.Unmarshal([]byte(textproto), source); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}
	return source, nil
}

// protoTypeToCsvColumnType converts proto ColumnType to CsvColumnType.
func protoTypeToCsvColumnType(t ColumnType) CsvColumnType {
	switch t {
	case ColumnType_COLUMN_TYPE_STRING:
		return CsvColumnTypeString
	case ColumnType_COLUMN_TYPE_UINT32:
		return CsvColumnTypeUint32
	case ColumnType_COLUMN_TYPE_BOOL:
		return CsvColumnTypeBool
	default:
		return CsvColumnTypeAuto
	}
}

// TableSourceToColumnSources converts a proto TableSource to a map of CsvColumnSource
// suitable for use with ImportOptions.
func TableSourceToColumnSources(source *TableSource) map[string]CsvColumnSource {
	result := make(map[string]CsvColumnSource)
	for _, col := range source.GetColumns() {
		result[col.GetName()] = CsvColumnSource{
			Name:        col.GetName(),
			DisplayName: col.GetDisplayName(),
			EntityType:  col.GetEntityType(),
			Type:        protoTypeToCsvColumnType(col.GetType()),
		}
	}
	return result
}

// OptionsFromTextproto creates ImportOptions from a textproto configuration string.
func OptionsFromTextproto(textproto string) (ImportOptions, error) {
	source, err := ParseTableSource(textproto)
	if err != nil {
		return ImportOptions{}, err
	}

	options := DefaultOptions()
	options.ColumnSources = TableSourceToColumnSources(source)
	return options, nil
}

// ParseTableSources parses a textproto string containing multiple table sources.
func ParseTableSources(textproto string) (*TableSources, error) {
	sources := &TableSources{}
	if err := prototext.Unmarshal([]byte(textproto), sources); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}
	return sources, nil
}

// OptionsMapFromTextproto creates a map of table name to ImportOptions from a textproto
// containing multiple table sources.
func OptionsMapFromTextproto(textproto string) (map[string]ImportOptions, error) {
	sources, err := ParseTableSources(textproto)
	if err != nil {
		return nil, err
	}

	result := make(map[string]ImportOptions)
	for _, table := range sources.GetTables() {
		options := DefaultOptions()
		options.ColumnSources = TableSourceToColumnSources(table)
		result[table.GetName()] = options
	}
	return result, nil
}
