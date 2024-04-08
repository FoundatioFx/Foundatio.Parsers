# Query Syntax

The query syntax is based on Lucene syntax.

## Basic
- `field:value` exact match
- `field:"Eric Smith"` exact match with quoted string value
- `_exists_:field` matches if there is any value for the field
- `_missing_:field` matches if there is not a value for the field

## Ranges
Ranges can be specified for date or numeric fields. Inclusive ranges are specified with square brackets `[min TO max]` and exclusive ranges with curly brackets `{min TO max}`.

- `datefield:[2012-01-01 TO 2012-12-31]` matches all days in 2012 for `datefield`
- `numberfield:[1 TO 5]` matches any number between 1 and 5 on `numberfield`
- `numberfield:1..5` shorthand for above query
- `numberfield:1..5` shorthand for above query
- `datefield:{* TO 2012-01-01}` matches dates before 2012
- `numberfield:[10 TO *]` matches values 10 and above
- `numberfield:[1 TO 5}` matches numbers from 1 up to but not including 5

Ranges with one side unbounded can use the following syntax:

- `age:>10` matches age greater than 10
- `age:>=10` matches age greater or equal to 10
- `age:<10` matches age less than 10
- `age:<=10` matches age less than or equal to 10

## Boolean operators

`AND` and `OR` are used to combine filter criteria. `NOT` can be used to negate filter criteria

`((field:quick AND otherfield:fox) OR (field:brown AND otherfield:fox) OR otherfield:fox) AND NOT thirdfield:news`

## Grouping

Multiple terms or clauses can be grouped together with parentheses, to form sub-queries:

`(field:quick OR field:brown) AND otherfield:fox`

## Date Math

Parameters which accept a formatted date value understand date math.

The expression starts with an anchor date, which can either be `now`, or a date string ending with ||. This anchor date can optionally be followed by one or more maths expressions:

`+1h` Add one hour
`-1d` Subtract one day

Supported units are:

- `y` Years
- `M` Months
- `w` Weeks
- `d` Days
- `h` Hours
- `H` Hours
- `m` Minutes
- `s` Seconds

Assuming now is `2001-01-01 12:00:00`, some examples are:

- `now+1h` now in milliseconds plus one hour. Resolves to: 2001-01-01 13:00:00
- `now-1h` now in milliseconds minus one hour. Resolves to: 2001-01-01 11:00:00

## Geo Proximity Queries

Geo proximity queries allow filtering data by documents that have a geo field value located within a given proximity of a geo coordinate. Locations can be resolved using a provided geo coding function that can translate something like "Dallas, TX" into a geo coordinate. 

Examples:
  - Within 75 miles of abc geohash: geofield:abc~75mi
  - Within 75 miles of 75044: geofield:75044~75mi

## Geo Range Queries

Geo range queries allow filtering data by documents that have a geo field value located within a bounding box. It uses the same syntax as other Lucene range queries, but the range is the top left and bottom right geo coordinates.

Examples:
  - Within coordinates rectangle: geofield:[geohash1..geohash2]

## Nested Document Queries

Elasticsearch does not support querying nested documents using the query_string query, but when using this library queries on those fields should work automatically.
