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

	pb "github.com/google/taxinomia/core/csvimport/proto"
	"google.golang.org/protobuf/encoding/prototext"
)

// ParseTableAnnotation parses a textproto string into a TableAnnotation proto.
func ParseTableAnnotation(textproto string) (*pb.TableAnnotation, error) {
	annotation := &pb.TableAnnotation{}
	if err := prototext.Unmarshal([]byte(textproto), annotation); err != nil {
		return nil, fmt.Errorf("failed to parse textproto: %w", err)
	}
	return annotation, nil
}

// protoTypeToColumnType converts proto ColumnType to csvimport ColumnType.
func protoTypeToColumnType(t pb.ColumnType) ColumnType {
	switch t {
	case pb.ColumnType_COLUMN_TYPE_STRING:
		return ColumnTypeString
	case pb.ColumnType_COLUMN_TYPE_UINT32:
		return ColumnTypeUint32
	default:
		return ColumnTypeAuto
	}
}

// TableAnnotationToColumnAnnotations converts a proto TableAnnotation to a map of ColumnAnnotation
// suitable for use with ImportOptions.
func TableAnnotationToColumnAnnotations(annotation *pb.TableAnnotation) map[string]ColumnAnnotation {
	result := make(map[string]ColumnAnnotation)
	for _, col := range annotation.GetColumns() {
		result[col.GetName()] = ColumnAnnotation{
			Name:        col.GetName(),
			DisplayName: col.GetDisplayName(),
			EntityType:  col.GetEntityType(),
			Type:        protoTypeToColumnType(col.GetType()),
		}
	}
	return result
}

// OptionsFromTextproto creates ImportOptions from a textproto configuration string.
func OptionsFromTextproto(textproto string) (ImportOptions, error) {
	annotation, err := ParseTableAnnotation(textproto)
	if err != nil {
		return ImportOptions{}, err
	}

	options := DefaultOptions()
	options.ColumnAnnotations = TableAnnotationToColumnAnnotations(annotation)
	return options, nil
}
