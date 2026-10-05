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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFeedbackHandler(t *testing.T) {
	var got []FeedbackReport
	failNext := false
	h := FeedbackHandler(func(ctx context.Context, r FeedbackReport) error {
		if failNext {
			return errors.New("back end down")
		}
		got = append(got, r)
		return nil
	})
	post := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/feedback", strings.NewReader(body)))
		return rec
	}

	rec := post(`{"kind":" Bug ","text":"  the drag does nothing  ","page":"http://x/table?t=1","build":"r1"}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid report: status %d (%s)", rec.Code, rec.Body)
	}
	if len(got) != 1 || got[0] != (FeedbackReport{Kind: "bug", Text: "the drag does nothing", Page: "http://x/table?t=1", Build: "r1"}) {
		t.Errorf("submitted %+v", got)
	}

	for _, tc := range []struct {
		body string
		code int
	}{
		{`not json`, http.StatusBadRequest},
		{`{"kind":"praise","text":"x"}`, http.StatusBadRequest},
		{`{"kind":"feature","text":"   "}`, http.StatusBadRequest},
		{`{"kind":"feature","text":"` + strings.Repeat("a", MaxFeedbackText+1) + `"}`, http.StatusBadRequest},
	} {
		if rec := post(tc.body); rec.Code != tc.code || !strings.Contains(rec.Body.String(), `"error"`) {
			t.Errorf("%.40s: status %d body %s, want %d with an error", tc.body, rec.Code, rec.Body, tc.code)
		}
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/feedback", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: status %d, want 405", rec.Code)
	}

	failNext = true
	if rec := post(`{"kind":"feature","text":"pivots"}`); rec.Code != http.StatusInternalServerError {
		t.Errorf("failing back end: status %d, want 500", rec.Code)
	}
	if len(got) != 1 {
		t.Errorf("invalid reports reached the back end: %+v", got)
	}
}
