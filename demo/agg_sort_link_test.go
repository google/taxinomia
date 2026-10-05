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
	"bytes"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

var aggSortLinkRE = regexp.MustCompile(`<a href="([^"]*)" class="agg-sort-toggle-btn`)

// TestGroupSortLinkWhenEveryColumnIsGrouped: the ⟳ button of a grouped
// column steps to sorting its groups by rows, which needs no other column.
// It used to get an empty link whenever every shown column was grouped, so
// clicking it did nothing until some ungrouped column was shown.
func TestGroupSortLinkWhenEveryColumnIsGrouped(t *testing.T) {
	srv, products := goldenSetup(t)
	product := products.Get("default")
	u, err := url.Parse("/default/table?table=orders&columns=region%2Cstatus&grouped=region%2Cstatus&limit=25")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if res := srv.HandleTableRequest(&buf, u, product, func(k, v string) {}); res != nil {
		t.Fatalf("HandleTableRequest failed: %+v", res)
	}
	links := aggSortLinkRE.FindAllStringSubmatch(buf.String(), -1)
	if len(links) != 2 {
		t.Fatalf("found %d group sort buttons, want 2 (one per grouped column)", len(links))
	}
	for i, col := range []string{"region", "status"} {
		href := strings.ReplaceAll(links[i][1], "&amp;", "&")
		if !strings.Contains(href, "groupsort%3A"+col+"=%2B%3Arows") {
			t.Errorf("%s: group sort link = %q, want one that sorts its groups by rows", col, href)
		}
	}
}
