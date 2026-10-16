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

// The Switzerland demo: real data, mostly from the Swiss Federal Statistical
// Office, built by tools/swissdata (sources in demo/data/switzerland/
// SOURCES.md). One table of municipalities, the federal popular votes of
// 2021-2025, and each vote's result in each municipality, which joins both.

//go:embed data/switzerland/ch_municipalities.csv
var chMunicipalitiesCSV []byte

//go:embed data/switzerland/ch_votes.csv
var chVotesCSV []byte

//go:embed data/switzerland/ch_vote_results.csv
var chVoteResultsCSV []byte

type col = csvimport.TextColumnOptions

var municipalityCol = col{DisplayName: "Municipality no.", EntityType: "ch.municipality"}
var voteCol = col{DisplayName: "Vote", EntityType: "ch.vote"}

// withClass returns c in the column pane class class.
func withClass(c col, class string) col {
	c.Class = class
	return c
}

var swissTables = []struct {
	name     string
	csv      []byte
	columns  map[string]col
	computed []tables.ComputedDefinition
}{
	{"ch_municipalities", chMunicipalitiesCSV, map[string]col{
		"bfs_nr":            withClass(municipalityCol, "Municipality"),
		"name":              {DisplayName: "Municipality", Class: "Municipality"},
		"canton":            {DisplayName: "Canton", EntityType: "ch.canton", Class: "Administration"},
		"district":          {DisplayName: "District", EntityType: "ch.district", Class: "Administration"},
		"region":            {DisplayName: "Region", EntityType: "ch.region", Class: "Administration"},
		"language_region":   {DisplayName: "Language region", Class: "Classification"},
		"urbanisation":      {DisplayName: "Urbanisation", Class: "Classification"},
		"municipality_type": {DisplayName: "Type", Class: "Classification"},
		"population":        {DisplayName: "Population", Class: "Population 2024"},
		"swiss":             {DisplayName: "Swiss", Class: "Population 2024"},
		"foreign":           {DisplayName: "Foreign", Class: "Population 2024"},
		"men":               {DisplayName: "Men", Class: "Population 2024"},
		"women":             {DisplayName: "Women", Class: "Population 2024"},
		"age_0_19":          {DisplayName: "Aged 0-19", Class: "Age"},
		"age_20_64":         {DisplayName: "Aged 20-64", Class: "Age"},
		"age_65_plus":       {DisplayName: "Aged 65+", Class: "Age"},
		"age_80_plus":       {DisplayName: "Aged 80+", Class: "Age"},
		"median_age":        {DisplayName: "Median age", Class: "Age"},
		"births":            {DisplayName: "Births", Class: "Demography 2024"},
		"deaths":            {DisplayName: "Deaths", Class: "Demography 2024"},
		"immigration":       {DisplayName: "Immigration", Class: "Demography 2024"},
		"emigration":        {DisplayName: "Emigration", Class: "Demography 2024"},
		"net_migration":     {DisplayName: "Net migration", Class: "Demography 2024"},
		"area_km2":          {DisplayName: "Area (km²)", Class: "Geography"},
		"elevation_m":       {DisplayName: "Elevation (m)", Class: "Geography"},
		"latitude":          {DisplayName: "Latitude", Class: "Geography"},
		"longitude":         {DisplayName: "Longitude", Class: "Geography"},
		"wikidata":          {DisplayName: "Wikidata", EntityType: "wikidata.item", Class: "Municipality"},
	}, []tables.ComputedDefinition{
		{Name: "density", DisplayName: "Density (per km²)", Expression: "round(population / area_km2)", Class: "Population 2024"},
		{Name: "foreign_pct", DisplayName: "Foreign (%)", Expression: "round(foreign * 100 / population, 1)", Class: "Population 2024"},
		{Name: "over_65_pct", DisplayName: "Aged 65+ (%)", Expression: "round(age_65_plus * 100 / population, 1)", Class: "Age"},
	}},
	{"ch_votes", chVotesCSV, map[string]col{
		"vote":        voteCol,
		"date":        {DisplayName: "Date"},
		"title":       {DisplayName: "Title"},
		"kind":        {DisplayName: "Kind"},
		"accepted":    {DisplayName: "Accepted"},
		"yes_pct":     {DisplayName: "Yes (%)"},
		"turnout_pct": {DisplayName: "Turnout (%)"},
	}, nil},
	{"ch_vote_results", chVoteResultsCSV, map[string]col{
		"vote":        voteCol,
		"bfs_nr":      municipalityCol,
		"yes_pct":     {DisplayName: "Yes (%)"},
		"turnout_pct": {DisplayName: "Turnout (%)"},
		"yes":         {DisplayName: "Yes"},
		"no":          {DisplayName: "No"},
		"valid":       {DisplayName: "Valid votes"},
		"electorate":  {DisplayName: "Electorate"},
	}, nil},
}

// SwissTableNames lists the Switzerland demo tables, in display order.
var SwissTableNames = []string{"ch_municipalities", "ch_votes", "ch_vote_results"}

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
