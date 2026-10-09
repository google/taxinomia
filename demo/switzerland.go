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
	_ "embed"
	"fmt"

	"github.com/google/taxinomia/core/csvimport"
	"github.com/google/taxinomia/core/tables"
)

// The Switzerland demo: real data from the Swiss Federal Statistical Office
// (commune register, population 2024) and Wikidata, built by tools/swissdata
// (sources and cleaning in demo/data/switzerland/SOURCES.md). Regions,
// cantons, districts and municipalities form the administrative hierarchy;
// mountains and lakes join their canton.

//go:embed data/switzerland/ch_regions.csv
var chRegionsCSV []byte

//go:embed data/switzerland/ch_cantons.csv
var chCantonsCSV []byte

//go:embed data/switzerland/ch_districts.csv
var chDistrictsCSV []byte

//go:embed data/switzerland/ch_municipalities.csv
var chMunicipalitiesCSV []byte

//go:embed data/switzerland/ch_mountains.csv
var chMountainsCSV []byte

//go:embed data/switzerland/ch_lakes.csv
var chLakesCSV []byte

type col = csvimport.TextColumnOptions

var cantonCol = col{DisplayName: "Canton", EntityType: "ch.canton"}
var regionCol = col{DisplayName: "Region", EntityType: "ch.region"}

var swissTables = []struct {
	name     string
	csv      []byte
	columns  map[string]col
	computed []tables.ComputedDefinition
}{
	{"ch_regions", chRegionsCSV, map[string]col{
		"region":     regionCol,
		"local_name": {DisplayName: "Local name"},
	}, nil},
	{"ch_cantons", chCantonsCSV, map[string]col{
		"canton":             cantonCol,
		"name":               {DisplayName: "Name"},
		"capital":            {DisplayName: "Capital"},
		"joined":             {DisplayName: "Joined"},
		"official_languages": {DisplayName: "Official languages"},
		"region":             regionCol,
	}, nil},
	{"ch_districts", chDistrictsCSV, map[string]col{
		"district": {DisplayName: "District", EntityType: "ch.district"},
		"name":     {DisplayName: "Name"},
		"canton":   cantonCol,
	}, nil},
	{"ch_municipalities", chMunicipalitiesCSV, map[string]col{
		"bfs_nr":            {DisplayName: "BFS no.", EntityType: "ch.municipality"},
		"name":              {DisplayName: "Municipality"},
		"canton":            cantonCol,
		"district":          {DisplayName: "District", EntityType: "ch.district"},
		"region":            regionCol,
		"language_region":   {DisplayName: "Language region"},
		"urbanisation":      {DisplayName: "Urbanisation"},
		"municipality_type": {DisplayName: "Type"},
		"population":        {DisplayName: "Population"},
		"swiss":             {DisplayName: "Swiss"},
		"foreign":           {DisplayName: "Foreign"},
		"men":               {DisplayName: "Men"},
		"women":             {DisplayName: "Women"},
		"area_km2":          {DisplayName: "Area (km²)"},
		"elevation_m":       {DisplayName: "Elevation (m)"},
		"latitude":          {DisplayName: "Latitude"},
		"longitude":         {DisplayName: "Longitude"},
		"wikidata":          {DisplayName: "Wikidata", EntityType: "wikidata.item"},
	}, []tables.ComputedDefinition{
		{Name: "density", DisplayName: "Density (per km²)", Expression: "round(population / area_km2)"},
		{Name: "foreign_pct", DisplayName: "Foreign (%)", Expression: "round(foreign * 100 / population, 1)"},
	}},
	{"ch_mountains", chMountainsCSV, map[string]col{
		"mountain":     {DisplayName: "Wikidata", EntityType: "ch.mountain"},
		"name":         {DisplayName: "Mountain"},
		"elevation_m":  {DisplayName: "Elevation (m)"},
		"prominence_m": {DisplayName: "Prominence (m)"},
		"range":        {DisplayName: "Range"},
		"canton":       cantonCol,
		"cantons":      {DisplayName: "Cantons"},
		"latitude":     {DisplayName: "Latitude"},
		"longitude":    {DisplayName: "Longitude"},
	}, nil},
	{"ch_lakes", chLakesCSV, map[string]col{
		"lake":        {DisplayName: "Wikidata", EntityType: "ch.lake"},
		"name":        {DisplayName: "Lake"},
		"area_km2":    {DisplayName: "Area (km²)"},
		"max_depth_m": {DisplayName: "Max depth (m)"},
		"elevation_m": {DisplayName: "Elevation (m)"},
		"volume_km3":  {DisplayName: "Volume (km³)"},
		"canton":      cantonCol,
		"cantons":     {DisplayName: "Cantons"},
		"latitude":    {DisplayName: "Latitude"},
		"longitude":   {DisplayName: "Longitude"},
	}, nil},
}

// CreateSwissTables imports the Switzerland demo tables, by name.
func CreateSwissTables() map[string]*tables.DataTable {
	out := make(map[string]*tables.DataTable, len(swissTables))
	for _, st := range swissTables {
		imp, err := csvimport.ImportText(st.csv, csvimport.TextOptions{Columns: st.columns})
		if err != nil {
			panic(fmt.Sprintf("import %s: %v", st.name, err))
		}
		if len(st.computed) > 0 {
			if err := imp.Table.SetComputedDefinitions(st.computed); err != nil {
				panic(fmt.Sprintf("computed columns of %s: %v", st.name, err))
			}
		}
		out[st.name] = imp.Table
		fmt.Printf("%s: %d rows\n", st.name, imp.Rows)
	}
	return out
}
