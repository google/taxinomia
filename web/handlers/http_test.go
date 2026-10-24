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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestCrossSite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{"no browser headers (curl)", nil, false},
		{"same origin", map[string]string{"Sec-Fetch-Site": "same-origin", "Origin": "http://example.com"}, false},
		{"typed or bookmarked", map[string]string{"Sec-Fetch-Site": "none"}, false},
		{"another site", map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "http://evil.example"}, true},
		{"a sibling subdomain", map[string]string{"Sec-Fetch-Site": "same-site"}, true},
		// Without Sec-Fetch-Site (older browsers): Origin against the host.
		{"origin is the host", map[string]string{"Origin": "http://example.com"}, false},
		{"origin is the forwarded host", map[string]string{"Origin": "https://public.example", "X-Forwarded-Host": "public.example"}, false},
		{"origin is another host", map[string]string{"Origin": "http://evil.example"}, true},
		{"null origin", map[string]string{"Origin": "null"}, true},
	} {
		r := httptest.NewRequest(http.MethodPost, "http://example.com/import", nil)
		for k, v := range tc.headers {
			r.Header.Set(k, v)
		}
		if got := crossSite(r); got != tc.want {
			t.Errorf("%s: crossSite = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A form on another site's page cannot import a table or send a report.
func TestCrossSiteRefused(t *testing.T) {
	srv := newImportServer(t)
	req := httptest.NewRequest(http.MethodPost, "/import", strings.NewReader(url.Values{"text": {"a\n1\n"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()
	srv.ImportHandler(ImportOptions{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("import: %d, want 403", rec.Code)
	}
	if srv.dataModel.GetTable("import") != nil || len(srv.imports) != 0 {
		t.Errorf("a cross-site import was added")
	}

	submitted := false
	fb := FeedbackHandler(func(ctx context.Context, r FeedbackReport) error { submitted = true; return nil })
	req = httptest.NewRequest(http.MethodPost, "/feedback", strings.NewReader(`{"kind":"bug","text":"x"}`))
	req.Header.Set("Origin", "http://evil.example")
	rec = httptest.NewRecorder()
	fb.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || submitted {
		t.Errorf("feedback: %d (submitted %v), want 403", rec.Code, submitted)
	}
}
