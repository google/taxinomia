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
	"errors"
	"strings"
	"testing"
)

// bom is the UTF-8 byte order mark some editors write at the start of a file.
var bom = string([]byte{0xef, 0xbb, 0xbf})

func kinds(t *TextImport) string {
	var parts []string
	for _, c := range t.Columns {
		parts = append(parts, c.Name+"="+c.Kind)
	}
	return strings.Join(parts, " ")
}

func value(t *testing.T, imp *TextImport, col string, row uint32) (string, error) {
	t.Helper()
	c := imp.Table.GetColumn(col)
	if c == nil {
		t.Fatalf("no column %q", col)
	}
	return c.GetString(row)
}

// A range copied from a spreadsheet: tab-separated, a quoted cell holding
// a tab and a line break, a trailing newline.
func TestImportTextPastedSpreadsheet(t *testing.T) {
	text := "Station\tYear\tRain (l/m²)\tActive\tStart\n" +
		"Bern\t2024\t81.5\tyes\t2024-05-01\n" +
		"\"Zürich\tNord\nOst\"\t2025\t90\tno\t2025-05-01 08:30:00\n"
	imp, err := ImportText([]byte(text), TextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if imp.Delimiter != '\t' || imp.Rows != 2 {
		t.Fatalf("delimiter %q rows %d, want tab and 2", imp.Delimiter, imp.Rows)
	}
	want := "station=text year=whole number rain_l_m2=decimal active=yes/no start=date"
	if got := kinds(imp); got != want {
		t.Errorf("kinds = %s\nwant    %s", got, want)
	}
	if v, _ := value(t, imp, "station", 1); v != "Zürich\tNord\nOst" {
		t.Errorf("quoted cell = %q", v)
	}
	if h := imp.Table.GetColumn("rain_l_m2").ColumnDef().DisplayName(); h != "Rain (l/m²)" {
		t.Errorf("display name = %q, want the header", h)
	}
}

// European CSV: semicolons, decimal commas, NA, codes with leading zeros.
func TestImportTextSemicolonDecimalCommaAndMissing(t *testing.T) {
	text := bom + "PLZ;Gemeinde;Fläche;Einwohner\n" +
		"0123;Aarau;12,34;21000\n" +
		"8001;Zürich;NA;NA\n" +
		"3000;Bern;51,6;134000\n"
	imp, err := ImportText([]byte(text), TextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := "plz=text gemeinde=text flaeche=decimal einwohner=whole number"
	if got := kinds(imp); got != want {
		t.Errorf("kinds = %s\nwant    %s", got, want)
	}
	if v, _ := value(t, imp, "plz", 0); v != "0123" {
		t.Errorf("leading zero lost: %q", v)
	}
	if v, err := value(t, imp, "flaeche", 0); err != nil || v != "12.34" {
		t.Errorf("decimal comma = %q, %v; want 12.34", v, err)
	}
	if _, err := value(t, imp, "einwohner", 1); !errors.Is(err, ErrMissingValue) {
		t.Errorf("NA cell error = %v, want ErrMissingValue", err)
	}
	if imp.Columns[3].Missing != 1 {
		t.Errorf("einwohner missing = %d, want 1", imp.Columns[3].Missing)
	}
}

func TestImportTextNamesAndShapes(t *testing.T) {
	text := "a,a,,2nd value\n1,2,3\n4,5,6,7,8\n"
	imp, err := ImportText([]byte(text), TextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(imp.ColumnNamesInOrder(), ","); got != "a,a_2,col_3,_2nd_value" {
		t.Errorf("names = %s", got)
	}
	// The short row's missing cell is missing; extra cells are dropped.
	if _, err := value(t, imp, "_2nd_value", 0); !errors.Is(err, ErrMissingValue) {
		t.Errorf("short row cell error = %v, want ErrMissingValue", err)
	}
	if v, _ := value(t, imp, "_2nd_value", 1); v != "7" {
		t.Errorf("value = %q, want 7", v)
	}
}

func TestImportTextRefusals(t *testing.T) {
	if _, err := ImportText([]byte("  \n\n"), TextOptions{}); err == nil {
		t.Error("blank text accepted")
	}
	if _, err := ImportText([]byte("a\n\xff\n"), TextOptions{}); err == nil {
		t.Error("invalid UTF-8 accepted")
	}
	if _, err := ImportText([]byte("a\n1\n2\n3\n"), TextOptions{MaxRows: 2}); err == nil {
		t.Error("too many rows accepted")
	}
	// A header alone is a table with no rows.
	imp, err := ImportText([]byte("x,y\n"), TextOptions{})
	if err != nil || imp.Rows != 0 || len(imp.Columns) != 2 {
		t.Errorf("header only: %v rows=%v", err, imp)
	}
}
