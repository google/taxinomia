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
	"reflect"
	"testing"
)

func TestBuildLinkBlocks(t *testing.T) {
	link := func(name, area string) EntityURL { return EntityURL{Name: name, Area: area} }
	names := func(b LinkBlock) []string {
		var n []string
		for _, l := range b.Links {
			n = append(n, l.Name)
		}
		return n
	}

	// Areas in the order they first appear, then the links without one.
	// Commands join their area's block (a new one for an area without
	// links), those without an area a block of their own, last.
	blocks := BuildLinkBlocks([]EntityURL{
		link("a", "Ops"), link("x", ""), link("b", "Hardware"), link("c", "Ops"),
	}, []EntityCommand{{Name: "drain", Area: "Ops"}, {Name: "ssh"}, {Name: "fsck", Area: "Disks"}})
	commandNames := func(b LinkBlock) []string {
		var n []string
		for _, c := range b.Commands {
			n = append(n, c.Name)
		}
		return n
	}
	want := []struct {
		area     string
		links    []string
		commands []string
	}{
		{"Ops", []string{"a", "c"}, []string{"drain"}},
		{"Hardware", []string{"b"}, nil},
		{"Disks", nil, []string{"fsck"}},
		{"", []string{"x"}, nil},
		{"", nil, []string{"ssh"}},
	}
	if len(blocks) != len(want) {
		t.Fatalf("got %d blocks, want %d", len(blocks), len(want))
	}
	for i, w := range want {
		if blocks[i].Area != w.area || !reflect.DeepEqual(names(blocks[i]), w.links) || !reflect.DeepEqual(commandNames(blocks[i]), w.commands) {
			t.Errorf("block %d = %q %v %v, want %q %v %v", i, blocks[i].Area, names(blocks[i]), commandNames(blocks[i]), w.area, w.links, w.commands)
		}
	}
	if blocks[0].Hue != AreaHue("Ops") || blocks[3].Hue != -1 {
		t.Errorf("hues = %d, %d; want the area's and -1", blocks[0].Hue, blocks[3].Hue)
	}

	// Links without an area in blocks of even size, at most four.
	for n, sizes := range map[int][]int{1: {1}, 4: {4}, 5: {3, 2}, 9: {3, 3, 3}, 10: {4, 4, 2}} {
		var links []EntityURL
		for i := 0; i < n; i++ {
			links = append(links, link("l", ""))
		}
		var got []int
		for _, b := range BuildLinkBlocks(links, nil) {
			got = append(got, len(b.Links))
		}
		if !reflect.DeepEqual(got, sizes) {
			t.Errorf("%d links: block sizes %v, want %v", n, got, sizes)
		}
	}
}

func TestLinkIcon(t *testing.T) {
	for _, tc := range []struct{ declared, name, want string }{
		{"book", "Zone Dashboard", "book"}, // declared wins
		{"", "Zone Dashboard", "chart"},
		{"", "System Logs", "logs"},
		{"nope", "Wikipedia", "book"}, // unknown declared: inferred
		{"", "Something", "link"},
	} {
		if got := LinkIcon(tc.declared, tc.name); got != tc.want {
			t.Errorf("LinkIcon(%q, %q) = %q, want %q", tc.declared, tc.name, got, tc.want)
		}
	}
}
