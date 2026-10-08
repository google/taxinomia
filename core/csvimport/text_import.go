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
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/tables"
)

// ImportText turns tabular text into a table: what a user pastes (a
// spreadsheet copies cells as tab-separated values) or a CSV or TSV file.
//
// The first line is the header. The delimiter is detected from it (tab,
// comma, semicolon or pipe) unless TextOptions gives one; fields may be
// quoted the way spreadsheets and CSV writers quote them. Each column's
// type is inferred from all of its values: whole number, decimal (a decimal
// comma is recognised when the delimiter is not a comma), yes/no, date and
// time, or text. Numbers with a leading zero ("007", postal codes) stay
// text. Empty cells and the usual missing markers (NA, N/A, NULL, ...) are
// missing values: in a number or date column they are rows whose value
// fails, which aggregates leave out and count, instead of reading as 0.
func ImportText(data []byte, opts TextOptions) (*TextImport, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // UTF-8 byte order mark
	if !utf8.Valid(data) {
		return nil, errors.New("the text is not UTF-8; save the file as UTF-8 and try again")
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("there is nothing to import")
	}
	delim := opts.Delimiter
	if delim == 0 {
		delim = detectDelimiter(data)
	}
	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = delim
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("could not read the table: %w", err)
	}
	// A copied range ends with a newline; drop blank lines.
	kept := records[:0]
	for _, rec := range records {
		if !(len(rec) == 1 && strings.TrimSpace(rec[0]) == "") {
			kept = append(kept, rec)
		}
	}
	records = kept
	if len(records) == 0 {
		return nil, errors.New("there is nothing to import")
	}
	headers, rows := records[0], records[1:]
	if opts.MaxRows > 0 && len(rows) > opts.MaxRows {
		return nil, fmt.Errorf("the table has %d rows; at most %d can be imported", len(rows), opts.MaxRows)
	}
	markers := opts.MissingValues
	if markers == nil {
		markers = DefaultMissingValues
	}
	missing := make(map[string]bool, len(markers))
	for _, m := range markers {
		missing[m] = true
	}
	cell := func(row []string, i int) string {
		if i >= len(row) {
			return ""
		}
		v := strings.TrimSpace(row[i])
		if missing[v] {
			return ""
		}
		return v
	}

	decimalComma := delim != ','
	names := ColumnNames(headers)
	result := &TextImport{Table: tables.NewDataTable(), Rows: len(rows), Delimiter: delim}
	for i, h := range headers {
		kind := inferKind(rows, i, cell, decimalComma)
		def := columns.NewColumnDef(names[i], displayName(h, names[i]), "")
		empty := 0
		for _, row := range rows {
			if cell(row, i) == "" {
				empty++
			}
		}
		switch kind {
		case KindInteger:
			addInt64Column(result.Table, def, rows, i, cell, empty > 0)
		case KindDecimal:
			addFloat64Column(result.Table, def, rows, i, cell, empty > 0, decimalComma)
		case KindBool:
			c := columns.NewBoolColumn(def)
			for _, row := range rows {
				c.Append(parseBool(cell(row, i)))
			}
			c.FinalizeColumn()
			result.Table.AddColumn(c)
		case KindDatetime:
			addDatetimeColumn(result.Table, def, rows, i, cell, empty > 0)
		default:
			c := columns.NewStringColumn(def)
			for _, row := range rows {
				c.Append(cell(row, i))
			}
			c.FinalizeColumn()
			result.Table.AddColumn(c)
		}
		result.Columns = append(result.Columns, TextColumn{Name: names[i], Header: strings.TrimSpace(h), Kind: kind, Missing: empty})
	}
	return result, nil
}

// TextOptions configures ImportText. The zero value detects everything.
type TextOptions struct {
	// Delimiter separates fields; 0 detects it from the header line.
	Delimiter rune
	// MissingValues are the cell texts read as a missing value, besides an
	// empty cell; nil means DefaultMissingValues.
	MissingValues []string
	// MaxRows refuses tables with more data rows; 0 means no limit.
	MaxRows int
}

// DefaultMissingValues are the missing-value markers ImportText knows by
// default: those written by R, spreadsheets and databases.
var DefaultMissingValues = []string{"NA", "N/A", "n/a", "#N/A", "NaN", "NULL", "null", "None"}

// TextImport is the result of ImportText.
type TextImport struct {
	Table     *tables.DataTable
	Columns   []TextColumn // in header order
	Rows      int
	Delimiter rune
}

// ColumnNamesInOrder returns the table's column names in header order.
func (t *TextImport) ColumnNamesInOrder() []string {
	names := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		names[i] = c.Name
	}
	return names
}

// TextColumn describes one imported column.
type TextColumn struct {
	Name    string // usable in filters and formulas
	Header  string // as written in the header line (the display name)
	Kind    string // one of the Kind constants
	Missing int    // rows without a value
}

// The column kinds ImportText infers.
const (
	KindInteger  = "whole number"
	KindDecimal  = "decimal"
	KindBool     = "yes/no"
	KindDatetime = "date"
	KindText     = "text"
)

// ErrMissingValue is the error of a row without a value in an imported
// number or date column.
var ErrMissingValue = errors.New("missing value")

// detectDelimiter picks the candidate occurring most often in the header
// line, outside quotes; tab wins ties, as pasted cells are tab-separated.
// A line with none of them is a single column.
func detectDelimiter(data []byte) rune {
	line := data
	if i := bytes.IndexAny(line, "\r\n"); i >= 0 {
		line = line[:i]
	}
	counts := map[rune]int{}
	quoted := false
	for _, r := range string(line) {
		switch {
		case r == '"':
			quoted = !quoted
		case !quoted && (r == '\t' || r == ',' || r == ';' || r == '|'):
			counts[r]++
		}
	}
	best, bestN := '\t', counts['\t']
	for _, d := range []rune{',', ';', '|'} {
		if counts[d] > bestN {
			best, bestN = d, counts[d]
		}
	}
	if bestN == 0 {
		return ','
	}
	return best
}

// ColumnNames turns header texts into column names usable in expressions:
// lower-case ASCII letters, digits and underscores, starting with a letter
// or underscore, and unique (a repeated name gets _2, _3, ...). Accents are
// folded (Zürich becomes zuerich); a header with nothing usable becomes
// col_<position>.
func ColumnNames(headers []string) []string {
	names := make([]string, len(headers))
	seen := map[string]bool{}
	for i, h := range headers {
		n := Identifier(h)
		if n == "" {
			n = fmt.Sprintf("col_%d", i+1)
		}
		base := n
		for k := 2; seen[n]; k++ {
			n = fmt.Sprintf("%s_%d", base, k)
		}
		seen[n] = true
		names[i] = n
	}
	return names
}

var foldAccents = strings.NewReplacer(
	"ä", "ae", "ö", "oe", "ü", "ue", "ß", "ss",
	"à", "a", "á", "a", "â", "a", "ã", "a", "å", "a",
	"ç", "c", "è", "e", "é", "e", "ê", "e", "ë", "e",
	"ì", "i", "í", "i", "î", "i", "ï", "i", "ñ", "n",
	"ò", "o", "ó", "o", "ô", "o", "õ", "o",
	"ù", "u", "ú", "u", "û", "u",
	"²", "2", "³", "3", "%", "pct", "#", "nr",
)

// Identifier turns a text into a name usable in expressions, as
// ColumnNames does for one header; "" when nothing usable is left.
func Identifier(s string) string {
	s = foldAccents.Replace(strings.ToLower(strings.TrimSpace(s)))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	n := b.String()
	for strings.Contains(n, "__") {
		n = strings.ReplaceAll(n, "__", "_")
	}
	n = strings.Trim(n, "_")
	if n != "" && n[0] >= '0' && n[0] <= '9' {
		n = "_" + n
	}
	return n
}

func displayName(header, name string) string {
	if h := strings.TrimSpace(header); h != "" {
		return h
	}
	return name
}

// inferKind looks at every value of column i.
func inferKind(rows [][]string, i int, cell func([]string, int) string, decimalComma bool) string {
	isInt, isFloat, isBool, isDate, seen := true, true, true, true, false
	for _, row := range rows {
		v := cell(row, i)
		if v == "" {
			continue
		}
		seen = true
		if isInt && !isWholeNumber(v) {
			isInt = false
		}
		_, isNumber := parseDecimal(v, decimalComma)
		if isFloat && !isNumber {
			isFloat = false
		}
		if isBool && !isBoolText(v) {
			isBool = false
		}
		if isDate {
			// A plain number would parse as a Unix timestamp; it is a number.
			if isNumber || looksNumeric(v) {
				isDate = false
			} else if _, err := columns.ParseDatetime(v, nil); err != nil {
				isDate = false
			}
		}
		if !isInt && !isFloat && !isBool && !isDate {
			return KindText
		}
	}
	switch {
	case !seen:
		return KindText
	case isInt:
		return KindInteger
	case isFloat:
		return KindDecimal
	case isBool:
		return KindBool
	case isDate:
		return KindDatetime
	}
	return KindText
}

// looksNumeric: only digits and an optional leading minus (such as a code
// with a leading zero), which ParseDatetime would take for a timestamp.
func looksNumeric(v string) bool {
	v = strings.TrimPrefix(v, "-")
	if v == "" {
		return false
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// isWholeNumber: an int64 written without a leading zero (codes like "007"
// stay text) and without a plus sign.
func isWholeNumber(v string) bool {
	digits := strings.TrimPrefix(v, "-")
	if digits == "" || (len(digits) > 1 && digits[0] == '0') || strings.HasPrefix(v, "+") {
		return false
	}
	_, err := strconv.ParseInt(v, 10, 64)
	return err == nil
}

// parseDecimal reads a plain decimal number: digits, one decimal point (or
// comma, with decimalComma), an optional exponent. "Inf", "NaN" and hex
// are not table numbers; neither is a code with a leading zero ("0123").
func parseDecimal(v string, decimalComma bool) (float64, bool) {
	digits := strings.TrimPrefix(v, "-")
	if len(digits) > 1 && digits[0] == '0' && digits[1] != '.' && digits[1] != ',' {
		return 0, false
	}
	if decimalComma && strings.Count(v, ",") == 1 && !strings.Contains(v, ".") {
		v = strings.Replace(v, ",", ".", 1)
	}
	for _, r := range v {
		if !(r >= '0' && r <= '9') && r != '.' && r != '-' && r != 'e' && r != 'E' && r != '+' {
			return 0, false
		}
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func isBoolText(v string) bool {
	switch strings.ToLower(v) {
	case "true", "false", "yes", "no", "ja", "nein", "oui", "non":
		return true
	}
	return false
}

func parseBool(v string) bool {
	switch strings.ToLower(v) {
	case "true", "yes", "ja", "oui":
		return true
	}
	return false
}

// A number or date column with missing values is served by a computed
// column whose rows without a value fail with ErrMissingValue: aggregates
// leave them out and count them as failed.

func addInt64Column(t *tables.DataTable, def *columns.ColumnDef, rows [][]string, i int, cell func([]string, int) string, gaps bool) {
	vals := make([]int64, len(rows))
	have := make([]bool, len(rows))
	for r, row := range rows {
		if v, err := strconv.ParseInt(cell(row, i), 10, 64); err == nil {
			vals[r], have[r] = v, true
		}
	}
	if gaps {
		t.AddColumn(columns.NewComputedInt64Column(def, len(rows), func(r uint32) (int64, error) {
			if !have[r] {
				return 0, ErrMissingValue
			}
			return vals[r], nil
		}))
		return
	}
	c := columns.NewInt64Column(def)
	for _, v := range vals {
		c.Append(v)
	}
	c.FinalizeColumn()
	t.AddColumn(c)
}

func addFloat64Column(t *tables.DataTable, def *columns.ColumnDef, rows [][]string, i int, cell func([]string, int) string, gaps, decimalComma bool) {
	vals := make([]float64, len(rows))
	have := make([]bool, len(rows))
	for r, row := range rows {
		if v, ok := parseDecimal(cell(row, i), decimalComma); ok {
			vals[r], have[r] = v, true
		}
	}
	if gaps {
		t.AddColumn(columns.NewComputedFloat64Column(def, len(rows), func(r uint32) (float64, error) {
			if !have[r] {
				return 0, ErrMissingValue
			}
			return vals[r], nil
		}))
		return
	}
	c := columns.NewFloat64Column(def)
	for _, v := range vals {
		c.Append(v)
	}
	c.FinalizeColumn()
	t.AddColumn(c)
}

func addDatetimeColumn(t *tables.DataTable, def *columns.ColumnDef, rows [][]string, i int, cell func([]string, int) string, gaps bool) {
	vals := make([]time.Time, len(rows))
	have := make([]bool, len(rows))
	for r, row := range rows {
		if v := cell(row, i); v != "" {
			if tm, err := columns.ParseDatetime(v, nil); err == nil {
				vals[r], have[r] = tm, true
			}
		}
	}
	if gaps {
		t.AddColumn(columns.NewComputedDatetimeColumn(def, len(rows), func(r uint32) (int64, error) {
			if !have[r] {
				return 0, ErrMissingValue
			}
			return vals[r].UnixNano(), nil
		}))
		return
	}
	c := columns.NewDatetimeColumn(def)
	for _, v := range vals {
		c.Append(v)
	}
	c.FinalizeColumn()
	t.AddColumn(c)
}
