# Aggregation Syntax

Examples:
- Single metric: `min:field`
- Multiple metric: `min:field max:field`
- Nested bucketed: `terms:(field min:field max:field)`
  - Example result:
    ```json
    {
      "aggregations": {
        "terms_field1": {
          "buckets": [
            {
              "key": "value1",
              "doc_count": 102,
              "min_field2": {
                "value": 2
              },
              "max_field2": {
                "value": 30
              }
            },
            {
              "key": "value2",
              "doc_count": 76,
              "min_field2": {
                "value": 0
              },
              "max_field2": {
                "value": 87
              }
            }
        }
      }
    }
    ```
- Multiple levels nested: `terms:(field1 min:field2 max:field2 terms:(field2 min:field2 max:field2))`
- Terms sorted by nested max: `terms:(field min:field +max:field)`

# Metric Aggregations
The aggregations in this family compute metrics based on values extracted in one way or another from the documents that are being aggregated.

## `min`
A single-value metrics aggregation that keeps track and returns the minimum value among numeric values extracted from the aggregated documents.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `min:field`
- Missing value: `min:field~0`

## `max`
A single-value metrics aggregation that keeps track and returns the maximum value among the numeric values extracted from the aggregated documents.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `max:field`
- Missing value: `max:field~0`

## `avg`
A single-value metrics aggregation that computes the average of numeric values that are extracted from the aggregated documents.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `avg:field`
- Missing value: `avg:field~1`

## `sum`
A single-value metrics aggregation that sums up numeric values that are extracted from the aggregated documents.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `sum:field`
- Missing value: `sum:field~1`

## `stats`
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents.

The stats that are returned consist of: `min`, `max`, `sum`, `count` and `avg`.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `stats:field`
- Missing value: `stats:field~0`

## `exstats`
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents.

The `exstats` aggregations is an extended version of the `stats` aggregation, where additional metrics are added such as `sum_of_squares`, `variance`, `std_deviation` and `std_deviation_bounds`.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `exstats:field`
- Missing value: `exstats:field~0`

## `cardinality`
A single-value metrics aggregation that calculates an approximate count of distinct values.

Modifiers:
- `~` Sets the value to use when documents are missing a value.

Examples:
- Basic: `cardinality:field`
- Missing value: `cardinality:field~0`

## `percentiles`
A multi-value metrics aggregation that calculates one or more percentiles over numeric values extracted from the aggregated documents.

Percentiles show the point at which a certain percentage of observed values occur. For example, the 95th percentile is the value which is greater than 95% of the observed values.

Percentiles are often used to find outliers. In normal distributions, the 0.13th and 99.87th percentiles represents three standard deviations from the mean. Any data which falls outside three standard deviations is often considered an anomaly.

When a range of percentiles are retrieved, they can be used to estimate the data distribution and determine if the data is skewed, bimodal, etc.

Assume your data consists of website load times. The average and median load times are not overly useful to an administrator. The max may be interesting, but it can be easily skewed by a single slow response.

By default, the percentile metric will generate a range of percentiles: `1, 5, 25, 50, 75, 95, 99`

Modifiers:
- `~` Sets a list of percentiles to override the defaults. Percentiles are `,` delimited.

Examples:
- Basic: `percentiles:field`
- Custom percentile buckets: `percentiles:field~25,50,75`

# Bucketed Aggregations

Bucket aggregations don’t calculate metrics over fields like the metrics aggregations do, but instead, they create buckets of documents. Each bucket is associated with a criterion (depending on the aggregation type) which determines whether or not a document in the current context "falls" into it. In other words, the buckets effectively define document sets. In addition to the buckets themselves, the bucket aggregations also compute and return the number of documents that "fell into" each bucket.

Bucket aggregations, as opposed to metrics aggregations, can hold sub-aggregations. These sub-aggregations will be aggregated for the buckets created by their "parent" bucket aggregation.

## `histogram`
A multi-bucket values source based aggregation that can be applied on numeric values extracted from the documents. It dynamically builds fixed size (a.k.a. interval) buckets over the values. For example, if the documents have a field that holds a price (numeric), we can configure this aggregation to dynamically build buckets with interval 5 (in case of price it may represent $5). When the aggregation executes, the price field of every document will be evaluated and will be rounded down to its closest bucket - for example, if the price is 32 and the bucket size is 5 then the rounding will yield 30 and thus the document will "fall" into the bucket that is associated with the key 30.

Modifiers:
- `~` Sets the interval. Must be a positive decimal.

Examples:
- Basic: `histogram:field`
- Interval of 5: `histogram:field~5`

## `date`
A multi-bucket aggregation similar to the histogram except it can only be applied on date values. 

Modifiers:
- `~` Sets the interval. Available expressions are: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`
  - Time values can also be specified via abbreviations. `d` = days, `h` = hours, `m` = minutes, `s` = seconds, `ms` = milliseconds. Examples: `90m`, `1d`
- `^` Sets the time zone. Time zones may either be specified as an ISO 8601 UTC offset (e.g. `+01:00` or `-08:00`) or as a timezone id, an identifier used in the TZ database like `America/Los_Angeles`.
- `@missing` Value to use when documents are missing a value for the field.
- `@offset` The offset parameter is used to change the start value of each bucket by the specified positive (+) or negative offset (-) duration, such as `1h` for an hour, or `1d` for a day.

Examples:
- Basic: `date:field`
- 1 hour interval: `date:field~1h`
- Year interval: `date:field~year`
- 2h timezone: `date:field^2h`
- 1 hour interval and -5h timezone: `date:field~year^-5h`
- 1 hour offset: `date:(field @offset:1h)`

## `geogrid`
A multi-bucket aggregation that works on geo fields and groups points into buckets that represent cells in a grid. The resulting grid can be sparse and only contains cells that have matching data. Each cell is labeled using a geohash which is of user-definable precision.

High precision geohashes have a long string length and represent cells that cover only a small area.
Low precision geohashes have a short string length and represent cells that each cover a large area.
Geohashes used in this aggregation can have a choice of precision between 1 and 12.

Modifiers:
- `~` Sets the precision. Must be a number between 1 and 12.

Examples:
- Basic: `geogrid:field`
- Precision of 5: `geogrid:field~5`

## `terms`
A multi-bucket value source based aggregation where buckets are dynamically built - one per unique value.

Modifiers:
- `~` Sets the size to define how many term buckets should be returned out of the overall terms list.
- `^` Sets the minimum number of matching documents that must exist for the term to be included.
- `@include` Terms that match this pattern will be included in the result.
- `@exclude` Terms that match this pattern will be excluded from the result.
- `@missing` Value to use when documents are missing a value for the field.
- `@min` Sets the minimum number of matching documents that must exist for the term to be included.

Examples:
- Basic: `terms:field`
- Return top 5 terms: `terms:field~5`
- Return terms excluding "value": `terms:(field @exclude:value)`
- Sorted descending by nested max: `terms:(field -max:field)`

## `missing`
A field data based single bucket aggregation, that creates a bucket of all documents in the current document set context that are missing a field value (effectively, missing a field or having the configured NULL value set). This aggregator will often be used in conjunction with other field data bucket aggregators (such as ranges) to return information for all the documents that could not be placed in any of the other buckets due to missing field data values.

Examples:
- Basic: `missing:field`

# Other Aggregations

## `tophits`
Returns a list of the top hits for the parent aggregation. Since a field name is not used with this aggregation, the specified field name (`tophits:_`) will be ignored.

Modifiers:
- `~` Sets the size to define how many term buckets should be returned out of the overall terms list.
- `@include` Will include the specified field in the resulting top hits document.
- `@exclude` Will exclude the specified field in the resulting top hits document.

Examples:
- Basic: `terms:(field tophits:_)`
- Return terms excluding "value": `terms:(field tophits:(_ @exclude:value))`
