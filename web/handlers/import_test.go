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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/web/urlquery"
)

func newImportServer(t *testing.T) *Server {
	t.Helper()
	srv, err := NewServer(models.NewDataModel())
	if err != nil {
		t.Fatal(err)
	}
	srv.SetImportURL("/import")
	return srv
}

func postForm(h http.Handler, values url.Values, accept string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/import", strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func postFile(t *testing.T, h http.Handler, fileName, content string, fields map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", fileName)
	if err != nil {
		t.Fatal(err)
	}
	fw.Write([]byte(content))
	for k, v := range fields {
		mw.WriteField(k, v)
	}
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/import", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

type importAnswer struct {
	Table   string `json:"table"`
	URL     string `json:"url"`
	Rows    int    `json:"rows"`
	Columns []struct {
		Name    string `json:"Name"`
		Kind    string `json:"Kind"`
		Missing int    `json:"Missing"`
	} `json:"columns"`
	Error string `json:"error"`
}

func decodeAnswer(t *testing.T, rec *httptest.ResponseRecorder) importAnswer {
	t.Helper()
	var a importAnswer
	if err := json.Unmarshal(rec.Body.Bytes(), &a); err != nil {
		t.Fatalf("answer %q: %v", rec.Body.String(), err)
	}
	return a
}

// A pasted range (tab-separated) becomes a table; its link shows every
// column in pasted order, and a link without columns does too.
func TestImportPaste(t *testing.T) {
	srv := newImportServer(t)
	h := srv.ImportHandler(ImportOptions{})
	rec := postForm(h, url.Values{"text": {"Station\tYear\tRain\nBern\t2024\t81.5\nBasel\t2024\tNA\n"}}, "application/json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	a := decodeAnswer(t, rec)
	if a.Table != "pasted" || a.Rows != 2 || len(a.Columns) != 3 {
		t.Fatalf("answer = %+v", a)
	}
	if a.Columns[2].Kind != "decimal" || a.Columns[2].Missing != 1 {
		t.Errorf("rain column = %+v, want decimal with 1 missing", a.Columns[2])
	}
	if !strings.HasPrefix(a.URL, "table?") || !strings.Contains(a.URL, "columns=station%2Cyear%2Crain") {
		t.Errorf("url = %s", a.URL)
	}
	if srv.dataModel.GetTable("pasted") == nil {
		t.Fatal("table not added to the data model")
	}

	u, _ := url.Parse("/scratchpad/table?table=pasted")
	exec, res := srv.Execute(context.Background(), urlquery.NewQuery(u), ExecOptions{})
	if res != nil {
		t.Fatalf("Execute: %+v", res)
	}
	if got := strings.Join(exec.View.Columns, ","); got != "station,year,rain" {
		t.Errorf("default columns = %s, want the pasted order", got)
	}
}

// A file is named after itself; a second import under a taken name gets
// a suffix.
func TestImportFileAndNames(t *testing.T) {
	srv := newImportServer(t)
	h := srv.ImportHandler(ImportOptions{})
	csv := "a;b\n1;x\n2;y\n"
	if a := decodeAnswer(t, postFile(t, h, `C:\data\Rain 2024.csv`, csv, nil)); a.Table != "rain_2024" || a.Rows != 2 {
		t.Fatalf("first answer = %+v", a)
	}
	if a := decodeAnswer(t, postFile(t, h, "other.csv", csv, map[string]string{"name": "Rain 2024"})); a.Table != "rain_2024_2" {
		t.Fatalf("second answer = %+v, want the name made unique", a)
	}
	infos := srv.importedTableInfos()
	// The multipart reader keeps only the file's base name.
	if len(infos) != 2 || infos[0].Name != "rain_2024_2" || infos[1].Description != "Loaded from Rain 2024.csv" {
		t.Errorf("landing infos = %+v", infos)
	}
}

// A plain form post (the landing page's form, no script) is redirected to
// the table under the product it came from, and only to a product path.
func TestImportFormRedirect(t *testing.T) {
	srv := newImportServer(t)
	h := srv.ImportHandler(ImportOptions{})
	rec := postForm(h, url.Values{"text": {"x,y\n1,2\n"}, "name": {"t"}, "return": {"/scratchpad/"}}, "")
	if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/scratchpad/table?") {
		t.Errorf("status %d location %q", rec.Code, rec.Header().Get("Location"))
	}
	rec = postForm(h, url.Values{"text": {"x,y\n1,2\n"}, "return": {"//evil.example/"}}, "")
	if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, "/table?") {
		t.Errorf("foreign return path followed: %q", loc)
	}
}

func TestImportRefusals(t *testing.T) {
	srv := newImportServer(t)
	refuse := srv.ImportHandler(ImportOptions{
		MaxBytes: 1 << 10,
		Accept: func(ctx context.Context, r *http.Request, it *ImportedTable) error {
			if it.Name == "secret" {
				return errors.New("not allowed")
			}
			return nil
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/import", nil)
	rec := httptest.NewRecorder()
	refuse.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d", rec.Code)
	}
	if rec := postForm(refuse, url.Values{}, "application/json"); rec.Code != http.StatusBadRequest || decodeAnswer(t, rec).Error == "" {
		t.Errorf("empty: %d %s", rec.Code, rec.Body.String())
	}
	if rec := postForm(refuse, url.Values{"text": {"a\n1\n"}, "name": {"secret"}}, "application/json"); rec.Code != http.StatusForbidden || decodeAnswer(t, rec).Error != "not allowed" {
		t.Errorf("refused: %d %s", rec.Code, rec.Body.String())
	}
	big := "a\n" + strings.Repeat("123456789\n", 200)
	if rec := postForm(refuse, url.Values{"text": {big}}, "application/json"); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("too big: %d %s", rec.Code, rec.Body.String())
	}
	if srv.dataModel.GetTable("secret") != nil {
		t.Error("a refused table was added")
	}
}
