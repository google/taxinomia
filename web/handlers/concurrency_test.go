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
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/google/taxinomia/web/urlquery"
	"github.com/google/taxinomia/web/viewmodel"
)

type testProduct struct{}

func (testProduct) GetName() string                   { return "p" }
func (testProduct) GetTitle() string                  { return "p" }
func (testProduct) GetSubtitle() string               { return "" }
func (testProduct) GetTables() []viewmodel.TableInfo  { return nil }
func (testProduct) GetDefaultColumns(string) []string { return nil }

// Concurrent requests never share a table view: requests of different users
// (each a new cache entry) and of one user (one cache entry) run at once,
// each with its own computed column, and every page shows its own column
// only. Before views were checked out per request, this crashed the process
// (concurrent map writes) or mixed the requests' columns; run it with -race.
func TestConcurrentRequests(t *testing.T) {
	s := journeyTestServer(t)
	const n = 24
	var wg sync.WaitGroup
	pages := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			user := "shared"
			if i%2 == 0 {
				user = fmt.Sprintf("u%d", i)
			}
			raw := fmt.Sprintf("/p/table?table=orders&user=%s&computed=c%d%%3Damount*%d&columns=region,amount,c%d", user, i, i, i)
			if i%3 == 0 {
				raw += "&grouped=region"
			}
			u, _ := url.Parse(raw)
			var buf bytes.Buffer
			if res := s.HandleTableRequest(&buf, u, testProduct{}, func(k, v string) {}); res != nil {
				t.Errorf("request %d: %+v", i, res)
				return
			}
			pages[i] = buf.String()
		}(i)
	}
	wg.Wait()
	for i, page := range pages {
		if !strings.Contains(page, fmt.Sprintf(`data-column="c%d"`, i)) {
			t.Errorf("page %d lacks its computed column c%d", i, i)
		}
		for j := 0; j < n; j++ {
			if j != i && strings.Contains(page, fmt.Sprintf(`data-column="c%d"`, j)) {
				t.Errorf("page %d shows request %d's computed column c%d", i, j, j)
			}
		}
	}
}

// A released view serves the user's next request: its computed column is
// reused, not rebuilt; an execution never released leaves no view behind.
func TestExecutionRelease(t *testing.T) {
	s := journeyTestServer(t)
	u, _ := url.Parse("/p/table?table=orders&user=a&computed=d%3Damount*2")
	exec, res := s.Execute(context.Background(), urlquery.NewQuery(u), ExecOptions{User: "a"})
	if res != nil {
		t.Fatal(res)
	}
	first := exec.TableView
	exec.Release()
	exec.Release() // a second call does nothing

	exec2, res := s.Execute(context.Background(), urlquery.NewQuery(u), ExecOptions{User: "a"})
	if res != nil {
		t.Fatal(res)
	}
	if exec2.TableView != first {
		t.Errorf("the released view was not reused")
	}
	// exec2 is not released: the next request gets a fresh view.
	exec3, res := s.Execute(context.Background(), urlquery.NewQuery(u), ExecOptions{User: "a"})
	if res != nil {
		t.Fatal(res)
	}
	if exec3.TableView == first {
		t.Errorf("a view still checked out was handed to another request")
	}
	if len(exec3.Validation.ComputedColumnErrors) != 0 {
		t.Errorf("fresh view: computed column errors %v", exec3.Validation.ComputedColumnErrors)
	}
}
