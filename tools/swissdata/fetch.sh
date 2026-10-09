#!/bin/sh
# Downloads the sources of the Switzerland demo tables into $1 (default
# ./swiss-download), then: go run build.go <that dir> ../../demo/data/switzerland
set -e
D=${1:-swiss-download}
mkdir -p "$D"
UA='taxinomia-demo-data/0.1 (https://github.com/google/taxinomia)'
DATE=01-07-2025   # the commune register state matching the population table
here=$(dirname "$0")

# Official commune register (Swiss Federal Statistical Office)
curl -sfL -A "$UA" "https://www.agvchapp.bfs.admin.ch/api/communes/levels?date=$DATE" -o "$D/levels_$DATE.csv"
curl -sfL -A "$UA" "https://www.agvchapp.bfs.admin.ch/api/communes/snapshot?date=$DATE" -o "$D/snapshot_$DATE.csv"

# Permanent resident population 2024 by commune, citizenship and sex (STAT-TAB)
T=px-x-0102010000_101
curl -sf -A "$UA" -H 'Content-Type: application/json' --data-binary "@$here/population_query.json" \
  "https://www.pxweb.bfs.admin.ch/api/v1/en/$T/$T.px" | iconv -f latin1 -t utf-8 > "$D/pop2024.utf8.csv"

# Wikidata (CC0)
wd() { curl -sf -G 'https://query.wikidata.org/sparql' --data-urlencode "query@$here/$1" -H 'Accept: text/csv' -A "$UA" -o "$D/$2"; }
wd q_mfacts.rq mfacts.csv
wd q_mountains.rq mountains_raw.csv
wd q_lakes.rq lakes_raw.csv
echo "downloaded into $D"
