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
	"encoding/json"
	"testing"

	"github.com/google/taxinomia/core/columns"
	"github.com/google/taxinomia/core/engine"
	"github.com/google/taxinomia/core/models"
	"github.com/google/taxinomia/core/tables"
)

func journeyTestServer(t *testing.T) *Server {
	t.Helper()
	dt := tables.NewDataTable()
	for _, name := range []string{"region", "amount"} {
		col := columns.NewChunkedInt64Column(columns.NewColumnDef(name, name, ""))
		col.Append(1)
		col.FinalizeColumn()
		dt.AddColumn(col)
	}
	dm := models.NewDataModel()
	dm.AddTable("orders", dt)
	s, err := NewServer(dm)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func journey(name, link, target string, products ...string) engine.Journey {
	return engine.Journey{Name: name, Title: "T " + name, Products: products,
		Steps: []engine.JourneyStep{{Caption: "c", Link: link, Target: target}}}
}

// Journeys that point at a missing table, a missing column or an unknown
// control are not offered; the others are, filtered by product.
func TestJourneysChecked(t *testing.T) {
	s := journeyTestServer(t)
	s.setJourneys([]engine.Journey{
		journey("ok-column", "table=orders", "column:region"),
		journey("ok-agg", "table=orders&grouped=region", "aggregate:amount:sum"),
		journey("ok-computed", "table=orders&computed=double%3Damount*2", "column:double"),
		journey("ok-none", "table=orders", ""),
		journey("ok-pane", "table=orders", "pane", "other"),
		journey("no-table", "table=missing", "column:region"),
		journey("no-column", "table=orders", "group:nope"),
		journey("bad-target", "table=orders", "wiggle:region"),
		journey("bad-agg", "table=orders", "aggregate:amount"),
		journey("bad name", "table=orders", ""),
	})
	var kept []string
	for _, j := range s.journeys {
		kept = append(kept, j.Name)
	}
	want := []string{"ok-column", "ok-agg", "ok-computed", "ok-none", "ok-pane"}
	if len(kept) != len(want) {
		t.Fatalf("kept %v, want %v", kept, want)
	}
	for i := range want {
		if kept[i] != want[i] {
			t.Fatalf("kept %v, want %v", kept, want)
		}
	}

	var page []struct{ Name string }
	if err := json.Unmarshal([]byte(s.journeysJSON("default")), &page); err != nil {
		t.Fatal(err)
	}
	if len(page) != 4 { // ok-pane is offered in "other" only
		t.Errorf("default product gets %d journeys, want 4: %+v", len(page), page)
	}
	if got := journeyTestServerEmpty(t).journeysJSON("default"); got != "" {
		t.Errorf("no journeys: %q, want empty", got)
	}
}

func journeyTestServerEmpty(t *testing.T) *Server {
	s := journeyTestServer(t)
	s.setJourneys(nil)
	return s
}
