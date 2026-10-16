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

package viewmodel

import (
	"net/url"
	"strings"
	"testing"

	"github.com/google/taxinomia/web/urlquery"
)

func TestPaneClasses(t *testing.T) {
	// Alphabetical, as the pane lists them; the source order is given apart.
	cols := []ColumnInfo{
		{Name: "age", PaneName: "Age", Class: "People", IsVisible: true},
		{Name: "area", PaneName: "Area", Class: "Place"},
		{Name: "city", PaneName: "City", Class: "Place", IsVisible: true},
		{Name: "name", PaneName: "Name", Class: "People", IsVisible: true},
		{Name: "note", PaneName: "Note"},
	}
	order := map[string]int{"name": 0, "age": 1, "city": 2, "area": 3, "note": 4}
	u, _ := url.Parse("/table?table=t&columns=name,age,city&expanded=class:People")
	classes := paneClasses(cols, order, urlquery.NewQuery(u))
	var got []string
	for _, c := range classes {
		var names []string
		for _, col := range c.Columns {
			n := col.Name
			if col.PaneHidden {
				n += "(hidden)"
			}
			names = append(names, n)
		}
		got = append(got, c.Name+":"+strings.Join(names, ","))
	}
	// Classes by first column; columns in source order; Place is collapsed,
	// so area (not shown) is hidden; the unclassified last, under Other.
	if g, want := strings.Join(got, " "), "People:name,age Place:city,area(hidden) Other:note(hidden)"; g != want {
		t.Errorf("classes = %s\nwant      %s", g, want)
	}
	if !classes[0].Expanded || classes[1].Expanded || !classes[0].AllShown || classes[1].AllShown || classes[1].Shown != 1 {
		t.Errorf("state = %+v / %+v", classes[0], classes[1])
	}
	show, _ := url.Parse(classes[1].ShowURL.String())
	if c := strings.Join(urlquery.NewQuery(show).Columns, ","); c != "name,age,city,area" {
		t.Errorf("show all Place = %s", c)
	}
	hide, _ := url.Parse(classes[0].ShowURL.String())
	if c := strings.Join(urlquery.NewQuery(hide).Columns, ","); c != "city" {
		t.Errorf("hide all People = %s", c)
	}

	// No classes, or a single class for every column: a flat pane.
	if paneClasses([]ColumnInfo{{Name: "a"}, {Name: "b"}}, nil, urlquery.NewQuery(u)) != nil {
		t.Error("classes without any class")
	}
	if paneClasses([]ColumnInfo{{Name: "a", Class: "X"}, {Name: "b", Class: "X"}}, nil, urlquery.NewQuery(u)) != nil {
		t.Error("a single class for every column should stay flat")
	}
}
