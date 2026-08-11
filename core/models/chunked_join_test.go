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

package models

import (
	"testing"

	"github.com/google/taxinomia/core/columns"
)

// TestCreateJoinerChunked pins that joiners are created for chunked columns
// and for mixed plain/chunked pairs of the same value type, and still refused
// across value types — phase 3c, where loaders produce chunked tables that
// must keep joining against everything else.
func TestCreateJoinerChunked(t *testing.T) {
	dm := NewDataModel()

	plainStr := columns.NewStringColumn(columns.NewColumnDef("id", "ID", "thing"))
	chunkedStr := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "thing"))
	for _, v := range []string{"a", "b", "c"} {
		plainStr.Append(v)
		chunkedStr.Append(v)
	}
	plainStr.FinalizeColumn()
	chunkedStr.FinalizeColumn()

	plainU32 := columns.NewUint32Column(columns.NewColumnDef("n", "N", "num"))
	chunkedU32 := columns.NewChunkedUint32Column(columns.NewColumnDef("n", "N", "num"))
	for _, v := range []uint32{1, 2, 3} {
		plainU32.Append(v)
		chunkedU32.Append(v)
	}
	plainU32.FinalizeColumn()
	chunkedU32.FinalizeColumn()

	// Dictionary-encoded from-side: per-role encoding selection (phase 4b)
	// turns repetitive string columns — foreign keys prominently — into dict
	// columns, which must keep joining.
	dictStr := columns.NewChunkedDictStringColumn[uint16](columns.NewColumnDef("id", "ID", "thing"))
	for _, v := range []string{"a", "b", "c"} {
		dictStr.Append(v)
	}
	dictStr.FinalizeColumn()

	// Arena and front-coded columns (phase 4c): encoding selection turns
	// loaded string columns into arenas and sorted string primary keys into
	// front-coded storage; both must keep joining, in both directions — the
	// front-coded PK is the common to-side of an FK join.
	arenaStr := columns.NewChunkedArenaStringColumn(columns.NewColumnDef("id", "ID", "thing"))
	fcSrc := columns.NewChunkedStringColumn(columns.NewColumnDef("id", "ID", "thing"))
	for _, v := range []string{"a", "b", "c"} {
		arenaStr.Append(v)
		fcSrc.Append(v)
	}
	arenaStr.FinalizeColumn()
	fcSrc.FinalizeColumn()
	fcStr, ok := columns.FrontCodeChunkedStringColumn(fcSrc)
	if !ok {
		t.Fatal("front coding declined the sorted key test column")
	}

	cases := []struct {
		name     string
		from, to columns.IDataColumn
		want     bool
	}{
		{"chunked-chunked string", chunkedStr, chunkedStr, true},
		{"plain-chunked string", plainStr, chunkedStr, true},
		{"chunked-plain string", chunkedStr, plainStr, true},
		{"chunked-chunked uint32", chunkedU32, chunkedU32, true},
		{"plain-chunked uint32", plainU32, chunkedU32, true},
		{"dict-chunked string", dictStr, chunkedStr, true},
		{"dict-plain string", dictStr, plainStr, true},
		{"arena-frontcoded string", arenaStr, fcStr, true},
		{"frontcoded-arena string", fcStr, arenaStr, true},
		{"dict-frontcoded string", dictStr, fcStr, true},
		{"plain-arena string", plainStr, arenaStr, true},
		{"type mismatch", chunkedStr, chunkedU32, false},
		{"type mismatch reversed", chunkedU32, plainStr, false},
		{"dict-uint32 mismatch", dictStr, chunkedU32, false},
		{"arena-uint32 mismatch", arenaStr, chunkedU32, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := dm.createJoiner(tc.from, tc.to)
			if (j != nil) != tc.want {
				t.Fatalf("createJoiner: got %v, want joiner=%v", j, tc.want)
			}
			if j == nil {
				return
			}
			// The joiner must actually resolve: row 0 of from maps to the
			// row holding the same value in to (identical data, so row 0).
			idx, err := j.Lookup(0)
			if err != nil || idx != 0 {
				t.Fatalf("Lookup(0): (%d, %v)", idx, err)
			}
		})
	}
}

// TestCreateJoinerPerCode pins that a dictionary-encoded from side gets the
// per-code joiner (phase 6a: the join resolves once per distinct FK value)
// while other string representations stay on the per-row joiner.
func TestCreateJoinerPerCode(t *testing.T) {
	dm := NewDataModel()

	dictStr := columns.NewChunkedDictStringColumn[uint16](columns.NewColumnDef("id", "ID", "thing"))
	plainStr := columns.NewStringColumn(columns.NewColumnDef("id", "ID", "thing"))
	for _, v := range []string{"a", "b", "c"} {
		dictStr.Append(v)
		plainStr.Append(v)
	}
	dictStr.FinalizeColumn()
	plainStr.FinalizeColumn()

	if j := dm.createJoiner(dictStr, plainStr); j == nil {
		t.Fatal("no joiner for dict->plain")
	} else if _, ok := j.(*columns.PerCodeJoiner[uint16]); !ok {
		t.Errorf("dict from side: got %T, want *columns.PerCodeJoiner[uint16]", j)
	}
	if j := dm.createJoiner(plainStr, dictStr); j == nil {
		t.Fatal("no joiner for plain->dict")
	} else if _, ok := j.(*columns.Joiner[string]); !ok {
		t.Errorf("plain from side: got %T, want *columns.Joiner[string]", j)
	}
}

// TestGetColumnTypeChunked pins that the system columns table reports the
// same logical data types for chunked columns as for plain ones.
func TestGetColumnTypeChunked(t *testing.T) {
	def := columns.NewColumnDef("c", "C", "")
	cases := []struct {
		col  columns.IDataColumn
		want string
	}{
		{columns.NewChunkedStringColumn(def), "string"},
		{columns.NewChunkedUint32Column(def), "uint32"},
		{columns.NewChunkedInt64Column(def), "int64"},
		{columns.NewChunkedUint64Column(def), "uint64"},
		{columns.NewChunkedFloat64Column(def), "float64"},
		{columns.NewChunkedBoolColumn(def), "bool"},
		{columns.NewChunkedDatetimeColumn(def), "datetime"},
		{columns.NewDictStringColumn[uint8](def), "string"},
		{columns.NewChunkedDictStringColumn[uint16](def), "string"},
		{columns.NewChunkedArenaStringColumn(def), "string"},
		{&columns.ChunkedFrontCodedStringColumn{}, "string"},
	}
	for _, tc := range cases {
		if got := getColumnType(tc.col); got != tc.want {
			t.Errorf("getColumnType(%T) = %q, want %q", tc.col, got, tc.want)
		}
	}
}
