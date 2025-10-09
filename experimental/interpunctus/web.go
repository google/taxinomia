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

package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var t = &Table{
	map[string]IColumn{},
}
var v = &View{
	order:        []string{"status", "region", "product_category", "product", "amount", "quantity"},
	sorting:      map[string]bool{"status": true, "region": true, "product_category": true, "product": true, "amount": true, "quantity": true},
	groupOn:      map[string][]string{},
	groupOnOrder: []string{},
	groupSortPos: map[string]int{},
	columnViews:  map[string]*ColumnView{},
}

func generateData() {
	c3 := NewColumn[uint8](&ColumnDef{"c3", "mod 3", map[string]uint32{}, map[uint32]string{}, CompareNumbers, false})
	c5 := NewColumn[uint8](&ColumnDef{"c5", "mod 5s", map[string]uint32{}, map[uint32]string{}, CompareNumbers, true})
	c4 := NewColumn[uint8](&ColumnDef{"c4", "mod 4s", map[string]uint32{}, map[uint32]string{}, CompareNumbers, true})
	c2 := NewColumn[uint8](&ColumnDef{"c2", "mod 2", map[string]uint32{}, map[uint32]string{}, CompareNumbers, false})
	t.columns["c2"] = c2
	t.columns["c3"] = c3
	t.columns["c4"] = c4
	t.columns["c5"] = c5

	for i := 0; i < 30; i++ {
		//for i := 0; i < 1_000_000; i++ {
		c3.Append(fmt.Sprintf("%d", 30+rand.Intn(3)))
		c5.Append(fmt.Sprintf("%d", 50+rand.Intn(5)))
		c4.Append(fmt.Sprintf("%d", 40+rand.Intn(4)))
		c2.Append(fmt.Sprintf("%d", 20+rand.Intn(2)))
	}
}

func serve() {
	// Try to load sample orders first, fallback to GitHub issues, then generated data
	loadedTable, err := LoadSampleOrders("resources/sample_data.json")
	if err != nil {
		fmt.Println("Could not load resources/sample_data.json:", err)
		fmt.Println("Trying vscode_issues.json instead...")
		loadedTable, err = LoadGitHubIssues("vscode_issues.json")
		if err != nil {
			fmt.Println("Could not load vscode_issues.json:", err)
			fmt.Println("Using generated data instead...")
			generateData()
		} else {
			t = loadedTable
		}
	} else {
		t = loadedTable
	}

	running := false
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if running {
			fmt.Println("ALREADY RUNNING...")
		}
		running = true
		start := time.Now()
		fmt.Println(r.URL.RawQuery)

		// Create a fresh View for each request to avoid shared mutable state
		requestView := &View{
			order:        []string{"status", "region", "product_category", "product", "amount", "quantity"},
			sorting:      map[string]bool{"status": true, "region": true, "product_category": true, "product": true, "amount": true, "quantity": true},
			groupOn:      map[string][]string{},
			groupOnOrder: []string{},
			groupSortPos: map[string]int{},
			columnViews:  map[string]*ColumnView{},
		}

		if len(r.URL.RawQuery) > 0 {
			requestView, _ = ParseQuery(r.URL.RawQuery)
		}
		fmt.Println("Apply", requestView.grouping)

		fmt.Println(time.Now().Sub(start))

		indices, g, _ := t.Apply(requestView)

		sb := Render(t, requestView, g, indices)

		fmt.Fprintf(w, sb.String())
		fmt.Println(time.Now().Sub(start))
		running = false
	})

	fmt.Println("Server starting on http://127.0.0.1:8090")
	log.Fatal(http.ListenAndServe("127.0.0.1:8090", nil))
}
