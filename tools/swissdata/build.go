//go:build ignore

// Builds the Switzerland demo tables from the downloaded sources:
//   - BFS official commune register (agvchapp), state 2025-07-01: cantons,
//     districts, communes and their classifications;
//   - BFS STAT-TAB px-x-0102010000_101: permanent resident population 2024
//     by commune, citizenship and sex;
//   - Wikidata (CC0): commune area, elevation, coordinates and item; mountains
//     of 3500 m and more; lakes of 1 km2 and more.
//
// usage: go run main.go <download dir> <output dir>
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

// Canton facts: capital (the seat of government, as named locally), year of
// joining the Confederation, official languages.
var cantonFacts = map[string][3]string{
	"ZH": {"Zürich", "1351", "German"},
	"BE": {"Bern", "1353", "German, French"},
	"LU": {"Luzern", "1332", "German"},
	"UR": {"Altdorf", "1291", "German"},
	"SZ": {"Schwyz", "1291", "German"},
	"OW": {"Sarnen", "1291", "German"},
	"NW": {"Stans", "1291", "German"},
	"GL": {"Glarus", "1352", "German"},
	"ZG": {"Zug", "1352", "German"},
	"FR": {"Fribourg", "1481", "French, German"},
	"SO": {"Solothurn", "1481", "German"},
	"BS": {"Basel", "1501", "German"},
	"BL": {"Liestal", "1501", "German"},
	"SH": {"Schaffhausen", "1501", "German"},
	"AR": {"Herisau", "1513", "German"},
	"AI": {"Appenzell", "1513", "German"},
	"SG": {"St. Gallen", "1803", "German"},
	"GR": {"Chur", "1803", "German, Romansh, Italian"},
	"AG": {"Aarau", "1803", "German"},
	"TG": {"Frauenfeld", "1803", "German"},
	"TI": {"Bellinzona", "1803", "Italian"},
	"VD": {"Lausanne", "1803", "French"},
	"VS": {"Sion", "1815", "French, German"},
	"NE": {"Neuchâtel", "1815", "French"},
	"GE": {"Genève", "1815", "French"},
	"JU": {"Delémont", "1979", "French"},
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
	communes := [][]string{{"bfs_nr", "name", "canton", "district", "region", "language_region", "urbanisation", "municipality_type",
		"population", "swiss", "foreign", "men", "women", "area_km2", "elevation_m", "latitude", "longitude", "wikidata"}}
	regionVotes := map[string]map[string]int{}
	for _, r := range lev[1:] {
		code := r[lh["BfsCode"]]
		canton := cantonCode[r[lh["CantonId"]]]
		if canton == "" {
			panic("no canton for " + code)
		}
		reg := r[lh["REGCH"]]
		if regionVotes[canton] == nil {
			regionVotes[canton] = map[string]int{}
		}
		regionVotes[canton][reg]++
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
		communes = append(communes, []string{code, r[lh["Name"]], canton, dnr, regions[reg][0],
			languages[r[lh["SPRGEB2020"]]], urbanisation[r[lh["DEGURB2021"]]], communeTypes[r[lh["GDETYP2012_9"]]],
			strconv.Itoa(c.swiss + c.foreign), strconv.Itoa(c.swiss), strconv.Itoa(c.foreign), strconv.Itoa(c.men), strconv.Itoa(c.women),
			area, elev, lat, lon, item})
	}
	sort.Slice(communes[1:], func(i, j int) bool {
		a, _ := strconv.Atoi(communes[1+i][0])
		b, _ := strconv.Atoi(communes[1+j][0])
		return a < b
	})

	// --- cantons, regions, districts ---
	cantons := [][]string{{"canton", "name", "capital", "joined", "official_languages", "region"}}
	var codes []string
	for code := range cantonName {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for _, code := range codes {
		best, n := "", -1
		for reg, v := range regionVotes[code] {
			if v > n {
				best, n = reg, v
			}
		}
		f, ok := cantonFacts[code]
		if !ok {
			panic("no facts for " + code)
		}
		cantons = append(cantons, []string{code, cantonName[code], f[0], f[1], f[2], regions[best][0]})
	}
	regionRows := [][]string{{"region", "local_name"}}
	for k := 1; k <= 7; k++ {
		r := regions[strconv.Itoa(k)]
		regionRows = append(regionRows, []string{r[0], r[1]})
	}
	districtRows := [][]string{{"district", "name", "canton"}}
	sort.Slice(districts, func(i, j int) bool {
		a, _ := strconv.Atoi(districts[i].id)
		b, _ := strconv.Atoi(districts[j].id)
		return a < b
	})
	for _, d := range districts {
		districtRows = append(districtRows, []string{d.id, d.name, d.canton})
	}

	// --- mountains ---
	mt := readCSV(filepath.Join(in, "mountains_raw.csv"))
	mh := header(mt)
	mountains := [][]string{{"mountain", "name", "elevation_m", "prominence_m", "range", "canton", "cantons", "latitude", "longitude"}}
	for _, r := range mt[1:] {
		cs := strings.Split(strings.ReplaceAll(r[mh["cantons"]], "CH-", ""), "|")
		sort.Strings(cs)
		lat, lon := latlon(r[mh["coord"]])
		mountains = append(mountains, []string{qid(r[mh["m"]]), r[mh["name"]], num(r[mh["elevation_m"]], 0), num(r[mh["prominence_m"]], 0),
			r[mh["range"]], cs[0], strings.Join(cs, ", "), lat, lon})
	}

	// --- lakes: only those with a recorded maximum depth (every inflated
	// area in Wikidata belongs to a lake without one), not the parts of
	// larger lakes, implausible volumes and depths cleared ---
	lk := readCSV(filepath.Join(in, "lakes_raw.csv"))
	kh := header(lk)
	partOf := map[string]bool{"Obersee": true, "Untersee": true, "Urnersee": true, "Alpnachersee": true}
	lakes := [][]string{{"lake", "name", "area_km2", "max_depth_m", "elevation_m", "volume_km3", "canton", "cantons", "latitude", "longitude"}}
	for _, r := range lk[1:] {
		name := r[kh["name"]]
		depth := r[kh["max_depth_m"]]
		if depth == "" || partOf[name] {
			continue
		}
		area, _ := strconv.ParseFloat(r[kh["area_m2"]], 64)
		d, _ := strconv.ParseFloat(depth, 64)
		if d > 400 {
			continue // Klingnauer Stausee: 500 m recorded for a river reservoir
		}
		vol := r[kh["volume_m3"]]
		if v, err := strconv.ParseFloat(vol, 64); err == nil {
			if v < area {
				vol = "" // smaller than one metre over the whole area
			} else {
				vol = strconv.FormatFloat(v/1e9, 'f', 3, 64)
			}
		}
		cs := []string{""}
		if r[kh["cantons"]] != "" {
			cs = strings.Split(strings.ReplaceAll(r[kh["cantons"]], "CH-", ""), "|")
			sort.Strings(cs)
		}
		lat, lon := latlon(r[kh["coord"]])
		lakes = append(lakes, []string{qid(r[kh["l"]]), name, strconv.FormatFloat(area/1e6, 'f', 2, 64), num(depth, 0), num(r[kh["elevation_m"]], 0),
			vol, cs[0], strings.Join(cs, ", "), lat, lon})
	}

	writeCSV(filepath.Join(out, "ch_regions.csv"), regionRows)
	writeCSV(filepath.Join(out, "ch_cantons.csv"), cantons)
	writeCSV(filepath.Join(out, "ch_districts.csv"), districtRows)
	writeCSV(filepath.Join(out, "ch_municipalities.csv"), communes)
	writeCSV(filepath.Join(out, "ch_mountains.csv"), mountains)
	writeCSV(filepath.Join(out, "ch_lakes.csv"), lakes)
}
