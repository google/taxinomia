<!--
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
-->

Taxinomia is a table oriented analytics system. 

Next features
 * sorting
 * aggregation
 * display limit for grouped tables
 * grouping function (can be done with computed columns)
 * filtering function (can be done with computed columns)
 * column optimisations 
 * sub columns, e.g. a column consists of a proto message, expand it on demand
 * when by definition the value of two columns always match, display the value of the other columns when the first one is grouped
 * support extremely large tables - user will have to filter on specific columns to reduce the table before being able to load it, though the column might be grouped
 * break up a column in multiple columns in one go, date => year/quarter/month/week/day/hour, basically a split like function that generates multiple computed columns automatically
 * graphs, etc


CLAUDE.md

Scalability goals are to support up to 1'000'000'000 rows
Latency must be near impercetible for up to 1'000'000 rows, maybe 100 ms
Latency must be small for up to 10'000'000 rows, maybe 1 s

The UI must be simple and clean, no fancy features, must be intuitive. Avoid following heavy handed approaches that are far too common.

Minimize client-side code, everything is encoded in the URL, updates must always go through the back end, with some very limited exceptions only, These should be clearly documented, justsified and validated.

There is no point in displaying more than at the very most a few hundreds of rows, typically it should not be more than 20 - 100.

Everything in the displayed data must be fully deterministic, page refreshes should change the content of the page

The code should be as simple as possible and cleanly implemented with proper abstractions. Avoid over complicating the code, in case of doubts ask.









Column types
 * base table
 * joined columns
 * derived columns 

Aggregation
 * numbers: sum, average, std, 
 * strings: first, last
 * bool: all, none, some/any, count

Filtering
 * Any column can be filtered on and/or grouped 

Sorting
 * on columns in any order between columns
 * on grouped values and aggregates

Some implementation gaps
 * further joins
 * filtering on joined columns
 * grouping of joined columns

Future features
 * filtering on aggregated values
 * grouping on aggregated values ???
 * pivoting
 * materialization 

Expression
 * Filtering expression
 * Grouping expression
 * Expression for derived columns
Same syntax and same interpreter 

Importing data
 * protobuf
 * csv
 * json, ndjson
 * xml
 * parquet
 * Big Query

Data types
 * integers (uint8, 32 64, int8 32 64)
 * floats (single and double)
 * date and time and duration
 * string
 * enums (?)
 * bool










Active    North  A   1  1
Active    North  A   2  3
Active    North  B   3  5
Active    South  A   4  3
Active    South  B   5  2
Active    South  B   6  2
Active    East   Z   6  2
Active    East   C   6  2
Active    East   C   6  2
Inactive  North  A   1  1
Inactive  North  A   2  3
Inactive  North  B   3  5
Inactive  South  A   4  3
Inactive  South  B   5  2
Inactive  South  B   6  2
Inactive  East   Z   6  2
Inactive  East   A   6  2
Inactive  East   Z   6  2
Pending   North  A   1  1
Pending   North  A   2  3
Pending   North  A  3  5
Pending   South  A   4  3
Pending   South  B   5  2
Pending   South  B   6  2
Pending   East   Z   6  2
Pending   East   C   6  2
Pending   East   C   6  2



|-----------|---------|---|
|           |  North  | A |
|           |         |---|
|           |         | B |
|           |---------|---|
| Active    |  South  | A |
|           |         |---|
|           |         | B |
|           |---------|---|
|           |  East   | C |
|           |         |---|
|           |         | Z |
|-----------|---------|---|
|           | North   | A |
|           |         |---|
|           |         | B |
|           |---------|---|
| Inactive  | South   | A |
|           |         |---|
|           |         | B |
|           |---------|---|
|           | East    | A |
|           |         |---|
|           |         | Z |
|-----------|---------|---|
|           | North   | A |
|           |---------|---|
| Pending   | South   | A |
|           |         |---|
|           |         | B |
|           |---------|---|
|           | East    | C |
|           |         |---|
|           |         | Z |
|-----------|---------|---|

