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
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/google/taxinomia/core/engine"
)

// Journeys are guided walks through the data that the product's authors
// declare (DataSourcesConfig.journeys, carried by the catalog). SetCatalog
// checks them against the data model: a journey whose step opens a table
// that does not exist, or points at a column that view does not have, is
// not offered, and the reason is logged, so a schema change cannot leave a
// journey pointing at nothing. The page gets the valid journeys of its
// product (as JSON) and the browser plays them.

var journeyNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// journeyTargetKinds are the controls a step can point at, and whether the
// target names a column.
var journeyTargetKinds = map[string]bool{
	"column": true, "group": true, "sort": true, "filter": true,
	"aggregate": true, "groupsort": true, "pane": false, "help": false,
}

// setJourneys keeps the journeys that check out against the data model.
func (s *Server) setJourneys(journeys []engine.Journey) {
	s.journeys = nil
	for _, j := range journeys {
		if err := s.checkJourney(j); err != nil {
			log.Printf("journey %q not offered: %v", j.Name, err)
			continue
		}
		s.journeys = append(s.journeys, j)
	}
}

func (s *Server) checkJourney(j engine.Journey) error {
	if !journeyNameRE.MatchString(j.Name) {
		return fmt.Errorf("name must be letters, digits, - or _")
	}
	if j.Title == "" || len(j.Steps) == 0 {
		return fmt.Errorf("needs a title and at least one step")
	}
	for i, st := range j.Steps {
		if err := s.checkJourneyStep(st); err != nil {
			return fmt.Errorf("step %d: %v", i+1, err)
		}
	}
	return nil
}

func (s *Server) checkJourneyStep(st engine.JourneyStep) error {
	params, err := url.ParseQuery(st.Link)
	if err != nil {
		return fmt.Errorf("link %q: %v", st.Link, err)
	}
	tableName := params.Get("table")
	table := s.dataModel.GetTable(tableName)
	if table == nil {
		return fmt.Errorf("no table %q", tableName)
	}
	if st.Target == "" {
		return nil
	}
	parts := strings.Split(st.Target, ":")
	namesColumn, known := journeyTargetKinds[parts[0]]
	if !known {
		return fmt.Errorf("unknown target %q", st.Target)
	}
	if !namesColumn {
		return nil
	}
	if len(parts) < 2 || parts[1] == "" || (parts[0] == "aggregate") != (len(parts) == 3) {
		return fmt.Errorf("target %q: expected %s:<column>%s", st.Target, parts[0], map[bool]string{true: ":<type>"}[parts[0] == "aggregate"])
	}
	column := parts[1]
	// The column must exist in that view: a stored column, a computed
	// column of the table, or one the link itself adds (computed= or a
	// joined column in columns=).
	if table.GetColumn(column) != nil {
		return nil
	}
	for _, d := range table.ComputedDefinitions() {
		if d.Name == column {
			return nil
		}
	}
	for _, def := range strings.Split(params.Get("computed"), ";") {
		if strings.HasPrefix(def, column+"=") {
			return nil
		}
	}
	for _, c := range strings.Split(params.Get("columns"), ",") {
		if strings.Split(c, ":")[0] == column {
			return nil
		}
	}
	return fmt.Errorf("table %q has no column %q", tableName, column)
}

// journeysJSON is the page's list of journeys for a product, as JSON
// ("" when there are none).
func (s *Server) journeysJSON(product string) string {
	type step struct {
		Caption string `json:"caption"`
		Link    string `json:"link"`
		Target  string `json:"target,omitempty"`
	}
	type journey struct {
		Name        string `json:"name"`
		Title       string `json:"title"`
		Description string `json:"description,omitempty"`
		Steps       []step `json:"steps"`
	}
	var out []journey
	for _, j := range s.journeys {
		if len(j.Products) > 0 && !containsString(j.Products, product) {
			continue
		}
		jj := journey{Name: j.Name, Title: j.Title, Description: j.Description}
		for _, st := range j.Steps {
			jj.Steps = append(jj.Steps, step{Caption: st.Caption, Link: st.Link, Target: st.Target})
		}
		out = append(out, jj)
	}
	if len(out) == 0 {
		return ""
	}
	b, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(b)
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
