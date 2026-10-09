#!/bin/sh
# Downloads the sources of the Switzerland demo tables into $1 (default
# ./swiss-download), then:
#   go run build.go votes.go extra.go <that dir> ../../demo/data/switzerland
# STAT-TAB limits request rates and sizes: the age table comes in three
# bands, with pauses.
set -e
D=${1:-swiss-download}
mkdir -p "$D/votes"
UA='taxinomia-demo-data/0.1 (https://github.com/google/taxinomia)'
DATE=01-07-2025   # the commune register state matching the population table
here=$(dirname "$0")
px() { # px <lang> <table> <query file> <out>
  curl -sf -A "$UA" -H 'Content-Type: application/json' --data-binary "@$3" \
    "https://www.pxweb.bfs.admin.ch/api/v1/$1/$2/$2.px" | iconv -f latin1 -t utf-8 > "$4"
  sleep 20
}

# Official commune register (FSO)
curl -sfL -A "$UA" "https://www.agvchapp.bfs.admin.ch/api/communes/levels?date=$DATE" -o "$D/levels_$DATE.csv"
curl -sfL -A "$UA" "https://www.agvchapp.bfs.admin.ch/api/communes/snapshot?date=$DATE" -o "$D/snapshot_$DATE.csv"

# Population 2024 by commune: citizenship x sex, then single years of age
px en px-x-0102010000_101 "$here/population_query.json" "$D/pop2024.utf8.csv"
for band in "0 33" "34 66" "67 100"; do
  set -- $band
  vals=$(seq "$1" "$2" | sed 's/.*/"&"/' | paste -sd, -)
  sed "s/\"Alter\",\"selection\":{\"filter\":\"item\",\"values\":\[[^]]*\]/\"Alter\",\"selection\":{\"filter\":\"item\",\"values\":[$vals]/; s/\"Staatsangehörigkeit (Kategorie)\",\"selection\":{\"filter\":\"item\",\"values\":\[\"1\",\"2\"\]/\"Staatsangehörigkeit (Kategorie)\",\"selection\":{\"filter\":\"item\",\"values\":[\"-99999\"]/; s/\"Geschlecht\",\"selection\":{\"filter\":\"item\",\"values\":\[\"1\",\"2\"\]/\"Geschlecht\",\"selection\":{\"filter\":\"item\",\"values\":[\"-99999\"]/" \
    "$here/population_query.json" > "$D/age_query.json"
  px en px-x-0102010000_101 "$D/age_query.json" "$D/age_$1.csv"
done

# Demographic balance 2024 by commune (births, deaths, migration)
px de px-x-0102020000_201 "$here/balance_query.json" "$D/balance2024.utf8.csv"

# Federal popular votes, results by commune (Federal Chancellery / FSO)
for day in 20210307 20210613 20210926 20211128 20220213 20220515 20220925 \
           20230618 20240303 20240609 20240922 20241124 20250209 20250928; do
  curl -sf -A "$UA" "https://ogd-static.voteinfo-app.ch/v1/ogd/sd-t-17-02-$day-eidgAbstimmung.json" -o "$D/votes/$day.json"
  sleep 1
done

# Wikidata (CC0): area, elevation, coordinates, item, by BFS number
curl -sf -G 'https://query.wikidata.org/sparql' --data-urlencode "query@$here/q_mfacts.rq" \
  -H 'Accept: text/csv' -A "$UA" -o "$D/mfacts.csv"
echo "downloaded into $D"
