//go:build ignore

// Builds the Switzerland demo tables from the downloaded sources (fetch.sh):
//   - FSO official commune register (agvchapp), state 2025-07-01: canton,
//     district and classifications of every commune;
//   - FSO STAT-TAB px-x-0102010000_101: permanent resident population 2024
//     by commune, citizenship, sex and age;
//   - FSO STAT-TAB px-x-0102020000_201: demographic balance 2024 by commune;
//   - Federal popular votes 2021-2025 by commune (votes.go);
//   - Wikidata (CC0): commune area, elevation, coordinates and item.
//
// usage: go run build.go votes.go extra.go <download dir> <output dir>
package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func readCSV(path string) [][]string {
	f, err := os.Open(path)
	must(err)
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	must(err)
	return rows
}

func header(rows [][]string) map[string]int {
	h := map[string]int{}
	for i, c := range rows[0] {
		h[strings.TrimPrefix(c, string(rune(0xFEFF)))] = i
	}
	return h
}

func writeCSV(path string, rows [][]string) {
	f, err := os.Create(path)
	must(err)
	w := csv.NewWriter(f)
	must(w.WriteAll(rows))
	w.Flush()
	must(f.Close())
	fmt.Printf("%s: %d rows\n", filepath.Base(path), len(rows)-1)
}

// num formats a number without trailing zeros; "" for none.
func num(s string, decimals int) string {
	if s == "" {
		return ""
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) {
		return ""
	}
	return strconv.FormatFloat(f, 'f', decimals, 64)
}

func qid(uri string) string { return strings.TrimPrefix(uri, "http://www.wikidata.org/entity/") }

// latlon reads a WKT "Point(lon lat)".
func latlon(p string) (string, string) {
	p = strings.TrimSuffix(strings.TrimPrefix(p, "Point("), ")")
	parts := strings.Fields(p)
	if len(parts) != 2 {
		return "", ""
	}
	return num(parts[1], 4), num(parts[0], 4)
}

var regions = map[string][2]string{ // REGCH code -> English, local name
	"1": {"Lake Geneva Region", "Région lémanique"},
	"2": {"Espace Mittelland", "Espace Mittelland"},
	"3": {"Northwestern Switzerland", "Nordwestschweiz"},
	"4": {"Zurich", "Zürich"},
	"5": {"Eastern Switzerland", "Ostschweiz"},
	"6": {"Central Switzerland", "Zentralschweiz"},
	"7": {"Ticino", "Ticino"},
}

var languages = map[string]string{"1": "German", "2": "French", "3": "Italian", "4": "Romansh"}

var urbanisation = map[string]string{"1": "City", "2": "Town or suburb", "3": "Rural area"}

var communeTypes = map[string]string{
	"11": "Urban, large agglomeration",
	"12": "Urban, medium agglomeration",
	"13": "Urban, small or no agglomeration",
	"21": "Peri-urban, high density",
	"22": "Peri-urban, medium density",
	"23": "Peri-urban, low density",
	"31": "Rural centre",
	"32": "Rural, central",
	"33": "Rural, peripheral",
}


func main() {
	in, out := os.Args[1], os.Args[2]
	must(os.MkdirAll(out, 0o755))

	// --- register: cantons and districts (snapshot), communes (levels) ---
	snap := readCSV(filepath.Join(in, "snapshot_01-07-2025.csv"))
	sh := header(snap)
	cantonCode := map[string]string{} // historical code of the canton -> ZH
	cantonName := map[string]string{} // ZH -> name
	type district struct{ id, name, canton string }
	var districts []district
	districtParent := map[string]string{}
	districtNr := map[string]string{} // historical code of a district -> its BFS number
	for _, r := range snap[1:] {
		if r[sh["ValidTo"]] != "" {
			continue
		}
		switch r[sh["Level"]] {
		case "1":
			cantonCode[r[sh["HistoricalCode"]]] = r[sh["ShortName"]]
			cantonName[r[sh["ShortName"]]] = r[sh["Name"]]
		case "2":
			districtParent[r[sh["HistoricalCode"]]] = r[sh["Parent"]]
			districtNr[r[sh["HistoricalCode"]]] = r[sh["BfsCode"]]
			districts = append(districts, district{id: r[sh["BfsCode"]], name: r[sh["Name"]], canton: r[sh["Parent"]]})
		}
	}
	for i := range districts {
		districts[i].canton = cantonCode[districts[i].canton]
	}

	lev := readCSV(filepath.Join(in, "levels_01-07-2025.csv"))
	lh := header(lev)

	// --- population 2024 by commune, citizenship, sex ---
	pop := readCSV(filepath.Join(in, "pop2024.utf8.csv"))
	type counts struct{ swiss, foreign, men, women int }
	popOf := map[string]*counts{}
	for _, r := range pop[1:] {
		unit := r[1]
		if !strings.HasPrefix(unit, "......") {
			continue
		}
		code := strings.TrimLeft(strings.Fields(strings.TrimPrefix(unit, "......"))[0], "0")
		n, err := strconv.Atoi(r[5])
		must(err)
		c := popOf[code]
		if c == nil {
			c = &counts{}
			popOf[code] = c
		}
		if r[3] == "Switzerland" {
			c.swiss += n
		} else {
			c.foreign += n
		}
		if r[4] == "Male" {
			c.men += n
		} else {
			c.women += n
		}
	}

	// --- Wikidata commune facts by BFS number ---
	wd := readCSV(filepath.Join(in, "mfacts.csv"))
	wh := header(wd)
	facts := map[string][]string{}
	for _, r := range wd[1:] {
		facts[r[wh["bfs"]]] = r
	}

	// --- communes ---
	ageOf := readAges(in)
	balOf := readBalance(in)
	districtName := map[string]string{}
	for _, d := range districts {
		districtName[d.id] = d.name
	}
	communes := [][]string{{"bfs_nr", "name", "canton", "district", "region", "language_region", "urbanisation", "municipality_type",
		"population", "swiss", "foreign", "men", "women", "age_0_19", "age_20_64", "age_65_plus", "age_80_plus", "median_age",
		"births", "deaths", "immigration", "emigration", "net_migration",
		"area_km2", "elevation_m", "latitude", "longitude", "wikidata"}}
	for _, r := range lev[1:] {
		code := r[lh["BfsCode"]]
		canton := cantonCode[r[lh["CantonId"]]]
		if canton == "" {
			panic("no canton for " + code)
		}
		reg := r[lh["REGCH"]]
		c := popOf[code]
		if c == nil {
			panic("no population for " + code)
		}
		f := facts[code]
		area, elev, lat, lon, item := "", "", "", "", ""
		if f != nil {
			area = num(f[wh["area_km2"]], 2)
			elev = num(f[wh["elevation_m"]], 0)
			lat, lon = latlon(f[wh["coord"]])
			item = qid(f[wh["item"]])
		}
		dnr := districtNr[r[lh["DistrictId"]]]
		if dnr == "" {
			panic("no district for " + code)
		}
		a, b := ageOf[code], balOf[code]
		if a == nil || b.births == "" {
			panic("no ages or balance for " + code)
		}
		communes = append(communes, []string{code, r[lh["Name"]], canton, districtName[dnr], regions[reg][0],
			languages[r[lh["SPRGEB2020"]]], urbanisation[r[lh["DEGURB2021"]]], communeTypes[r[lh["GDETYP2012_9"]]],
			strconv.Itoa(c.swiss + c.foreign), strconv.Itoa(c.swiss), strconv.Itoa(c.foreign), strconv.Itoa(c.men), strconv.Itoa(c.women),
			strconv.Itoa(a.under20), strconv.Itoa(a.from20to64), strconv.Itoa(a.over65), strconv.Itoa(a.over80), a.median,
			b.births, b.deaths, b.immigration, b.emigration, b.netMigration,
			area, elev, lat, lon, item})
	}
	sort.Slice(communes[1:], func(i, j int) bool {
		a, _ := strconv.Atoi(communes[1+i][0])
		b, _ := strconv.Atoi(communes[1+j][0])
		return a < b
	})

	writeCSV(filepath.Join(out, "ch_municipalities.csv"), communes)

	votes, results := buildVotes(in)
	writeCSV(filepath.Join(out, "ch_votes.csv"), votes)
	writeCSV(filepath.Join(out, "ch_vote_results.csv"), results)
}
