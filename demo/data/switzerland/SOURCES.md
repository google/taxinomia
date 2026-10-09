# Sources of the Switzerland demo tables

Built by `tools/swissdata` (see its README to rebuild):

| Table | Rows | From |
|---|---|---|
| `ch_regions` | 7 | the seven major regions (Grossregionen) of the commune register |
| `ch_cantons` | 26 | commune register (codes, names); capital, year of joining the Confederation and official languages added by hand |
| `ch_districts` | 143 | commune register |
| `ch_municipalities` | 2115 | commune register (canton, district, language region, degree of urbanisation, municipality type), population 2024, Wikidata (area, elevation, coordinates, item) |
| `ch_mountains` | 281 | Wikidata: summits of 3500 m and more |
| `ch_lakes` | 41 | Wikidata: lakes of 1 km² and more with a recorded maximum depth |

Sources:

- **Commune register**: Swiss Federal Statistical Office (FSO/BFS), official
  commune register, state 1 July 2025
  (`https://www.agvchapp.bfs.admin.ch/`). Open data; the source must be
  credited.
- **Population**: FSO, STAT-TAB table `px-x-0102010000_101`, permanent
  resident population 2024 by commune, citizenship and sex. Open data; the
  source must be credited.
- **Wikidata**: `https://www.wikidata.org/`, CC0 (public domain).

Cleaning, in `build.go`:

- Population totals per commune are summed from citizenship × sex; they add
  up to the national total (9 051 029).
- Lakes: only those with a recorded maximum depth are kept (the small lakes
  without one carry areas entered in the wrong unit), parts of larger lakes
  (Obersee, Untersee, Urnersee, Alpnachersee) are left out, a depth above
  400 m on a river reservoir and volumes below one metre over the lake's area
  are dropped. A lake's `canton` is the first of its `cantons`
  alphabetically; areas are of the whole lake, abroad included.
- Mountains: every summit of 3500 m and more, subsidiary summits included
  (their prominence shows it); `canton` is the first of `cantons`.
- Values Wikidata lacks are left empty (missing values in the tables).

To rebuild:

```sh
sh fetch.sh /tmp/swiss
go run build.go /tmp/swiss ../../demo/data/switzerland
```
