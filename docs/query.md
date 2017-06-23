# Query Syntax

The query syntax is the same as Lucene and Elasticsearch.

[Query Syntax](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax)

# Geo Proximity Queries

Geo proximity queries allow filtering data by documents that have a geo field value located within a given proximity of a geo coordinate. Locations can be resolved using a provided geo coding function that can translate something like "Dallas, TX" into a geo coordinate. 

Examples:
  - Within 75 miles of abc geohash: geofield:abc~75mi
  - Within 75 miles of 75044: geofield:75044~75mi

# Geo Range Queries

Geo range queries allow filtering data by documents that have a geo field value located within a bounding box. It uses the same syntax as other Lucene range queries, but the range is the top left and bottom right geo coordinates.

Examples:
  - Within coordinates rectangle: geofield:[geohash1..geohash2]

# Nested Document Queries

Elasticsearch does not support querying nested documents using the query_string query, but when using this library queries on those fields should work automatically.
