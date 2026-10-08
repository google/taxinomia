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

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/google/taxinomia/core/csvimport"
	"github.com/google/taxinomia/web/viewmodel"
)

// Import: users paste a table (cells copied from a spreadsheet arrive as
// tab-separated text) or load a CSV / TSV file, and get it as a table to
// explore. Taxinomia provides the "Import" button, the dialog, the parsing
// (csvimport.ImportText) and this handler; the application turns it on and
// decides whether an import is allowed:
//
//	srv.SetImportURL("/import")
//	mux.Handle("/import", srv.ImportHandler(handlers.ImportOptions{
//	    Accept: func(ctx context.Context, r *http.Request, t *handlers.ImportedTable) error {
//	        return checkUserMayImport(ctx, r) // the application's own rules
//	    },
//	}))
//
// Imported tables live in the server's data model until the process ends,
// visible under every product like the other tables; they are listed on
// landing pages. A product meant as a scratchpad needs no tables of its own.

// ImportOptions configures ImportHandler.
type ImportOptions struct {
	// MaxBytes is the largest accepted request body; 0 means 32 MiB.
	MaxBytes int64
	// MaxRows refuses tables with more data rows; 0 means no limit.
	MaxRows int
	// Accept, when set, is called before the table is added. An error
	// refuses the import and is shown to the user; it may also change
	// t.Name (it is made unique afterwards).
	Accept func(ctx context.Context, r *http.Request, t *ImportedTable) error
}

// ImportedTable is a table about to be added by ImportHandler.
type ImportedTable struct {
	Name     string // table name: the user's, the file's, or "pasted"
	Source   string // "paste" or "file"
	FileName string // the uploaded file's name ("" for a paste)
	Import   *csvimport.TextImport
}

// importRecord is an added import, for landing pages and default columns.
type importRecord struct {
	name     string
	source   string
	fileName string
	rows     int
	columns  []string
}

// SetImportURL sets the address the import dialog posts to and shows the
// "Import" button on table and landing pages; "" hides it again. Serve the
// address with ImportHandler.
func (s *Server) SetImportURL(url string) {
	s.importURL = url
}

// ImportHandler serves the address set with SetImportURL. It accepts a
// POSTed form (multipart for a file) with either a "file" or a "text"
// field, and an optional "name" for the table. Requests asking for JSON
// (Accept: application/json, as the dialog does) get
// {"table", "url", "rows", "columns"} with 200, or {"error"} with 400 for
// text that is not a table, 403 when Accept refuses, 405 for another method
// and 413 for a body over MaxBytes. Plain form posts are redirected (303)
// to the new table under the product path in the "return" field.
func (s *Server) ImportHandler(opts ImportOptions) http.Handler {
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 32 << 20
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantsJSON := strings.Contains(r.Header.Get("Accept"), "application/json")
		fail := func(status int, msg string) {
			if wantsJSON {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck // best effort
				return
			}
			http.Error(w, msg, status)
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			fail(http.StatusMethodNotAllowed, "use POST")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		data, source, fileName, err := readImportBody(r, maxBytes)
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				fail(http.StatusRequestEntityTooLarge, fmt.Sprintf("the table is larger than %d MB", maxBytes>>20))
				return
			}
			fail(http.StatusBadRequest, err.Error())
			return
		}
		imp, err := csvimport.ImportText(data, csvimport.TextOptions{MaxRows: opts.MaxRows})
		if err != nil {
			fail(http.StatusBadRequest, err.Error())
			return
		}
		t := &ImportedTable{Name: importName(r.FormValue("name"), fileName), Source: source, FileName: fileName, Import: imp}
		if opts.Accept != nil {
			if err := opts.Accept(r.Context(), r, t); err != nil {
				fail(http.StatusForbidden, err.Error())
				return
			}
		}
		name := s.addImportedTable(t)
		tableURL := "table?" + url.Values{"table": {name}, "columns": {strings.Join(imp.ColumnNamesInOrder(), ",")}, "limit": {"25"}}.Encode()
		if wantsJSON {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck // best effort
				"table":   name,
				"url":     tableURL,
				"rows":    imp.Rows,
				"columns": imp.Columns,
			})
			return
		}
		http.Redirect(w, r, returnPath(r.FormValue("return"))+tableURL, http.StatusSeeOther)
	})
}

// readImportBody returns the posted table text: the "file" field of a
// multipart form, else its "text" field.
func readImportBody(r *http.Request, maxBytes int64) (data []byte, source, fileName string, err error) {
	// ParseMultipartForm reports a plain form as "not multipart" and drops
	// the form's own read error (a body over the limit), so pick by type.
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		if err := r.ParseMultipartForm(maxBytes); err != nil {
			return nil, "", "", err
		}
	} else if err := r.ParseForm(); err != nil {
		return nil, "", "", err
	}
	if file, header, err := r.FormFile("file"); err == nil {
		defer file.Close()
		data, err := io.ReadAll(file)
		if err != nil {
			return nil, "", "", err
		}
		if len(data) > 0 {
			return data, "file", header.Filename, nil
		}
	}
	if text := r.FormValue("text"); strings.TrimSpace(text) != "" {
		return []byte(text), "paste", "", nil
	}
	return nil, "", "", errors.New("paste a table or choose a file first")
}

// importName is the table name for an import: the user's, else the file's
// name without its extension, else "pasted".
func importName(given, fileName string) string {
	if n := csvimport.Identifier(given); n != "" {
		return n
	}
	if fileName != "" {
		base := path.Base(strings.ReplaceAll(fileName, "\\", "/"))
		if n := csvimport.Identifier(strings.TrimSuffix(base, path.Ext(base))); n != "" {
			return n
		}
	}
	return "pasted"
}

var productPathRE = regexp.MustCompile(`^/[A-Za-z0-9_-]+/$`)

// returnPath accepts only a product path ("/name/"), so the redirect stays
// on this site.
func returnPath(p string) string {
	if productPathRE.MatchString(p) {
		return p
	}
	return "/"
}

// addImportedTable adds the table under a name not yet taken (t.Name,
// else t.Name_2, ...) and returns that name.
func (s *Server) addImportedTable(t *ImportedTable) string {
	s.importsMu.Lock()
	defer s.importsMu.Unlock()
	base := csvimport.Identifier(t.Name)
	if base == "" {
		base = "pasted"
	}
	name := base
	for k := 2; s.dataModel.GetTable(name) != nil; k++ {
		name = fmt.Sprintf("%s_%d", base, k)
	}
	s.dataModel.AddTable(name, t.Import.Table)
	s.imports = append(s.imports, importRecord{
		name:     name,
		source:   t.Source,
		fileName: t.FileName,
		rows:     t.Import.Rows,
		columns:  t.Import.ColumnNamesInOrder(),
	})
	return name
}

// importedColumns returns an imported table's columns in file order (its
// default view), nil for other tables.
func (s *Server) importedColumns(table string) []string {
	s.importsMu.Lock()
	defer s.importsMu.Unlock()
	for _, rec := range s.imports {
		if rec.name == table {
			return rec.columns
		}
	}
	return nil
}

// importedTableInfos lists the imported tables for a landing page, most
// recent first.
func (s *Server) importedTableInfos() []viewmodel.TableInfo {
	s.importsMu.Lock()
	defer s.importsMu.Unlock()
	infos := make([]viewmodel.TableInfo, 0, len(s.imports))
	for i := len(s.imports) - 1; i >= 0; i-- {
		rec := s.imports[i]
		desc := "Pasted table"
		if rec.source == "file" {
			desc = "Loaded from " + rec.fileName
		}
		infos = append(infos, viewmodel.TableInfo{
			Name:           rec.name,
			Description:    desc,
			URL:            "table?" + url.Values{"table": {rec.name}, "limit": {"25"}}.Encode(),
			RecordCount:    rec.rows,
			ColumnCount:    len(rec.columns),
			DefaultColumns: fmt.Sprintf("%d columns", len(rec.columns)),
			Categories:     "Imported",
		})
	}
	return infos
}
