//go:build ignore

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Federal popular votes (Federal Chancellery / FSO, opendata.swiss,
// ogd-static.voteinfo-app.ch): one file per voting day, results per commune.

type voteResult struct {
	YesPct     *float64 `json:"jaStimmenInProzent"`
	Yes        *int     `json:"jaStimmenAbsolut"`
	No         *int     `json:"neinStimmenAbsolut"`
	TurnoutPct *float64 `json:"stimmbeteiligungInProzent"`
	Valid      *int     `json:"gueltigeStimmen"`
	Electorate *int     `json:"anzahlStimmberechtigte"`
}

type voteFile struct {
	Day    string `json:"abstimmtag"`
	Schweiz struct {
		Vorlagen []struct {
			ID       int `json:"vorlagenId"`
			Titel    []struct{ LangKey, Text string } `json:"vorlagenTitel"`
			Accepted bool  `json:"vorlageAngenommen"`
			ArtID    int   `json:"vorlagenArtId"`
			Resultat voteResult `json:"resultat"`
			Kantone  []struct {
				Gemeinden []struct {
					Nr       string     `json:"geoLevelnummer"`
					Resultat voteResult `json:"resultat"`
				} `json:"gemeinden"`
			} `json:"kantone"`
		} `json:"vorlagen"`
	} `json:"schweiz"`
}

var voteKinds = map[int]string{
	1: "Mandatory referendum",
	2: "Optional referendum",
	3: "Popular initiative",
	4: "Counter-proposal",
	5: "Tie-break question",
}

func f2(p *float64) string {
	if p == nil {
		return ""
	}
	return strconv.FormatFloat(*p, 'f', 2, 64)
}

func i2(p *int) string {
	if p == nil {
		return ""
	}
	return strconv.Itoa(*p)
}

func buildVotes(in string) (votes, results [][]string) {
	files, err := filepath.Glob(filepath.Join(in, "votes", "*.json"))
	must(err)
	sort.Strings(files)
	votes = [][]string{{"vote", "date", "title", "kind", "accepted", "yes_pct", "turnout_pct"}}
	results = [][]string{{"vote", "bfs_nr", "yes_pct", "turnout_pct", "yes", "no", "valid", "electorate"}}
	for _, path := range files {
		data, err := os.ReadFile(path)
		must(err)
		var vf voteFile
		must(json.Unmarshal(data, &vf))
		date := vf.Day[:4] + "-" + vf.Day[4:6] + "-" + vf.Day[6:]
		for _, v := range vf.Schweiz.Vorlagen {
			title := ""
			for _, t := range v.Titel {
				if t.LangKey == "en" {
					title = strings.TrimSpace(t.Text)
				}
			}
			id := strconv.Itoa(v.ID)
			kind, ok := voteKinds[v.ArtID]
			if !ok {
				panic("unknown vote kind " + strconv.Itoa(v.ArtID))
			}
			votes = append(votes, []string{id, date, title, kind, strconv.FormatBool(v.Accepted), f2(v.Resultat.YesPct), f2(v.Resultat.TurnoutPct)})
			for _, k := range v.Kantone {
				for _, g := range k.Gemeinden {
					if n, err := strconv.Atoi(g.Nr); err != nil || n >= 9000 {
						continue // Swiss abroad and other non-commune units
					}
					r := g.Resultat
					results = append(results, []string{id, g.Nr, f2(r.YesPct), f2(r.TurnoutPct), i2(r.Yes), i2(r.No), i2(r.Valid), i2(r.Electorate)})
				}
			}
		}
	}
	return votes, results
}
