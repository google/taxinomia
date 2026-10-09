# Sources of the Switzerland demo tables

Built by `tools/swissdata` (see its README to rebuild) from public sources,
mostly the Swiss Federal Statistical Office (FSO):

| Table | Rows | Contents |
|---|---|---|
| `ch_municipalities` | 2115 | every municipality (state 1 July 2025): canton, district, major region, language region, degree of urbanisation, municipality type; permanent resident population 2024 by citizenship, sex and age group, median age; births, deaths and migration in 2024; area, elevation, coordinates |
| `ch_votes` | 42 | the federal popular votes of 2021 to 2025: date, title, kind, accepted, national yes share and turnout |
| `ch_vote_results` | 88410 | each vote's result in each municipality: yes share, turnout, yes and no votes, valid votes, electorate |

Sources:

- **Commune register**: FSO, official commune register, state 1 July 2025
  (`https://www.agvchapp.bfs.admin.ch/`). Open data; credit the source.
- **Population**: FSO, STAT-TAB `px-x-0102010000_101`, permanent resident
  population 2024 by commune, citizenship, sex and age. Open data; credit the
  source.
- **Demographic balance**: FSO, STAT-TAB `px-x-0102020000_201`, 2024: live
  births, deaths, immigration, emigration and net migration (including
  changes of population type). Open data; credit the source.
- **Popular votes**: Federal Chancellery and FSO, results of federal votes
  by commune (`https://ogd-static.voteinfo-app.ch/`, published on
  opendata.swiss). Open data; credit the source.
- **Wikidata** (`https://www.wikidata.org/`, CC0): area, elevation,
  coordinates and Wikidata item of each municipality.

Notes, in `build.go`, `votes.go` and `extra.go`:

- Population per municipality is summed from citizenship x sex, and the age
  groups from single years of age; both add up to the national total
  (9 051 029). The median age is the age at which half the residents are
  younger.
- The vote files are published on the municipality boundaries of
  1 January 2026; two municipality numbers there (84 result rows) are not in
  the 1 July 2025 register, so they join no municipality. Results of the
  Swiss abroad are left out.
- Values Wikidata lacks (area or elevation of 25 municipalities) are empty:
  missing values in the table.

To rebuild:

```sh
sh fetch.sh /tmp/swiss
go run build.go votes.go extra.go /tmp/swiss ../../demo/data/switzerland
```
