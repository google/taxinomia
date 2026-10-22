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
	"hash/fnv"
	"strings"
)

// LinkIcons are the icons a link can show on the entity page; each has a
// symbol "i-<name>" in the entity page's sprite.
var LinkIcons = []string{
	"link", "chart", "logs", "doc", "book", "map", "server", "network",
	"person", "people", "tag", "bolt", "ticket", "code", "search", "globe",
}

// linkIconWords infers an icon from words of a link's name, first match wins.
var linkIconWords = []struct{ word, icon string }{
	{"track", "map"}, {"shipping", "map"},
	{"dashboard", "chart"}, {"monitor", "chart"}, {"metric", "chart"}, {"status", "chart"},
	{"log", "logs"},
	{"wiki", "book"},
	{"map", "map"}, {"location", "map"},
	{"topology", "network"}, {"network", "network"},
	{"hardware", "server"}, {"inventory", "server"}, {"spec", "server"}, {"machine", "server"},
	{"profile", "person"}, {"crm", "person"}, {"owner", "person"}, {"team", "people"},
	{"product", "tag"}, {"promo", "tag"}, {"category", "tag"}, {"catalog", "tag"},
	{"power", "bolt"}, {"energy", "bolt"},
	{"ticket", "ticket"}, {"bug", "ticket"}, {"issue", "ticket"}, {"incident", "ticket"},
	{"code", "code"}, {"source", "code"}, {"config", "code"},
	{"search", "search"}, {"find", "search"},
	{"detail", "doc"}, {"doc", "doc"}, {"report", "doc"},
	{"web", "globe"}, {"site", "globe"},
}

// LinkIcon returns the icon of a link: declared when it is one of
// LinkIcons, else inferred from the link's name, else "link".
func LinkIcon(declared, name string) string {
	for _, icon := range LinkIcons {
		if declared == icon {
			return icon
		}
	}
	lower := strings.ToLower(name)
	for _, w := range linkIconWords {
		if strings.Contains(lower, w.word) {
			return w.icon
		}
	}
	return "link"
}

// LinkBlock is one block of the entity page's link column: the links and
// commands of one area, a few links without an area, or the commands
// without an area.
type LinkBlock struct {
	Area     string // the area's name; "" for a block without one
	Hue      int    // the block's colour as an HSL hue, from the area's name; -1 for no colour
	Links    []EntityURL
	Commands []EntityCommand
}

// linksPerBlock is how many links without an area share one block: few
// enough to take in at a glance.
const linksPerBlock = 4

// areaHues are well-separated hues; an area takes one by its name, so it
// has the same colour on every entity page.
var areaHues = []int{210, 150, 30, 280, 350, 190, 95, 45, 320, 250}

// BuildLinkBlocks groups links and commands for the entity page: one block
// per area (its links, then its commands), in the order the areas first
// appear; then the links without an area in blocks of even size, at most
// linksPerBlock; then the commands without an area in one block.
func BuildLinkBlocks(links []EntityURL, commands []EntityCommand) []LinkBlock {
	var blocks []LinkBlock
	index := make(map[string]int) // area -> its block
	block := func(area string) *LinkBlock {
		i, ok := index[area]
		if !ok {
			i = len(blocks)
			index[area] = i
			blocks = append(blocks, LinkBlock{Area: area, Hue: AreaHue(area)})
		}
		return &blocks[i]
	}
	var loose []EntityURL
	for _, l := range links {
		if l.Area == "" {
			loose = append(loose, l)
			continue
		}
		b := block(l.Area)
		b.Links = append(b.Links, l)
	}
	var looseCommands []EntityCommand
	for _, c := range commands {
		if c.Area == "" {
			looseCommands = append(looseCommands, c)
			continue
		}
		b := block(c.Area)
		b.Commands = append(b.Commands, c)
	}
	// Blocks of even size: 5 links make blocks of 3 and 2, not 4 and 1.
	if n := len(loose); n > 0 {
		numBlocks := (n + linksPerBlock - 1) / linksPerBlock
		size := (n + numBlocks - 1) / numBlocks
		for len(loose) > 0 {
			k := min(size, len(loose))
			blocks = append(blocks, LinkBlock{Hue: -1, Links: loose[:k]})
			loose = loose[k:]
		}
	}
	if len(looseCommands) > 0 {
		blocks = append(blocks, LinkBlock{Hue: -1, Commands: looseCommands})
	}
	return blocks
}

// AreaHue is the hue of an area's blocks, chosen by its name.
func AreaHue(area string) int {
	h := fnv.New32a()
	h.Write([]byte(strings.ToLower(area)))
	return areaHues[h.Sum32()%uint32(len(areaHues))]
}
