//go:build ignore

package main

import (
	"path/filepath"
	"strconv"
	"strings"
)

// unitCode returns the BFS number of a STAT-TAB commune unit
// ("......0001 Aeugst am Albis"), "" for cantons, districts, Switzerland.
func unitCode(unit string) string {
	if !strings.HasPrefix(unit, "......") {
		return ""
	}
	return strings.TrimLeft(strings.Fields(strings.TrimPrefix(unit, "......"))[0], "0")
}

type ages struct {
	under20, from20to64, over65, over80 int
	median                              string
}

// readAges sums the single-year ages (age_<first>.csv bands, wide format)
// into groups and the median age, per commune.
func readAges(in string) map[string]*ages {
	byAge := map[string][]int{} // commune -> count per age 0..100
	for _, f := range []string{"age_0.csv", "age_34.csv", "age_67.csv"} {
		rows := readCSV(filepath.Join(in, f))
		h := rows[0]
		for _, r := range rows[1:] {
			code := unitCode(r[1])
			if code == "" {
				continue
			}
			if byAge[code] == nil {
				byAge[code] = make([]int, 101)
			}
			for c := 5; c < len(r); c++ {
				age, err := strconv.Atoi(strings.Fields(h[c])[0])
				must(err)
				n, err := strconv.Atoi(r[c])
				must(err)
				byAge[code][age] += n
			}
		}
	}
	out := map[string]*ages{}
	for code, counts := range byAge {
		a := &ages{}
		total := 0
		for age, n := range counts {
			total += n
			switch {
			case age < 20:
				a.under20 += n
			case age < 65:
				a.from20to64 += n
			default:
				a.over65 += n
			}
			if age >= 80 {
				a.over80 += n
			}
		}
		// Median: the age at which half the residents are younger.
		half, seen := float64(total)/2, 0
		for age, n := range counts {
			if float64(seen+n) >= half && total > 0 {
				a.median = strconv.Itoa(age)
				break
			}
			seen += n
		}
		out[code] = a
	}
	return out
}

type balance struct{ births, deaths, immigration, emigration, netMigration string }

func readBalance(in string) map[string]balance {
	rows := readCSV(filepath.Join(in, "balance2024.utf8.csv"))
	out := map[string]balance{}
	for _, r := range rows[1:] {
		if code := unitCode(r[1]); code != "" {
			out[code] = balance{r[4], r[5], r[6], r[7], r[8]}
		}
	}
	return out
}
