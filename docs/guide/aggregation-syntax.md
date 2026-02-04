# Aggregation Syntax

Foundatio.Parsers supports dynamic aggregation expressions that compile to Elasticsearch aggregations. This enables end users to build custom analytics, charts, and dashboards.

## Overview

Aggregation expressions follow a simple syntax:

```
operation:field
operation:(field modifiers subaggregations)
```

### Quick Examples

```csharp
using Foundatio.Parsers.ElasticQueries;

var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// Single metric
var aggs = await parser.BuildAggregationsAsync("min:price");

// Multiple metrics
aggs = await parser.BuildAggregationsAsync("min:price max:price avg:price");

// Nested bucket with metrics
aggs = await parser.BuildAggregationsAsync("terms:(category min:price max:price)");
```

## Metric Aggregations

Metric aggregations compute values from document fields.

### min

Returns the minimum value among numeric values.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
min:field
min:field~0
```

**Example:**

```csharp
var aggs = await parser.BuildAggregationsAsync("min:price");
var aggs = await parser.BuildAggregationsAsync("min:price~0"); // Use 0 for missing
```

### max

Returns the maximum value among numeric values.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
max:field
max:field~0
```

### avg

Computes the average of numeric values.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
avg:field
avg:field~0
```

### sum

Sums numeric values.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
sum:field
sum:field~0
```

### stats

Multi-value aggregation returning `min`, `max`, `sum`, `count`, and `avg`.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
stats:field
stats:field~0
```

**Response structure:**

```json
{
  "stats_price": {
    "count": 100,
    "min": 10.0,
    "max": 500.0,
    "avg": 125.5,
    "sum": 12550.0
  }
}
```

### exstats

Extended stats including `sum_of_squares`, `variance`, `std_deviation`, and `std_deviation_bounds`.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
exstats:field
exstats:field~0
```

### cardinality

Approximate count of distinct values.

| Modifier | Description |
|----------|-------------|
| `~` | Value for missing documents |

```
cardinality:field
cardinality:field~0
```

### percentiles

Calculates percentiles over numeric values.

| Modifier | Description |
|----------|-------------|
| `~` | Comma-separated percentile values (default: 1, 5, 25, 50, 75, 95, 99) |

```
percentiles:field
percentiles:field~25,50,75,90,99
```

**Example:**

```csharp
// Default percentiles
var aggs = await parser.BuildAggregationsAsync("percentiles:response_time");

// Custom percentiles
aggs = await parser.BuildAggregationsAsync("percentiles:response_time~50,90,95,99");
```

## Bucket Aggregations

Bucket aggregations group documents into buckets. They can contain sub-aggregations.

### terms

Creates buckets for each unique value.

| Modifier | Description |
|----------|-------------|
| `~` | Number of buckets to return (size) |
| `^` | Minimum document count |
| `@include` | Include pattern |
| `@exclude` | Exclude pattern |
| `@missing` | Value for missing documents |
| `@min` | Minimum document count |

**Sorting:**
- `+field` - Sort ascending by nested aggregation
- `-field` - Sort descending by nested aggregation

```
terms:field
terms:field~10
terms:(field @exclude:value)
terms:(field -max:amount)
```

**Examples:**

```csharp
// Top 10 categories
var aggs = await parser.BuildAggregationsAsync("terms:category~10");

// Exclude specific values
aggs = await parser.BuildAggregationsAsync("terms:(status @exclude:deleted)");

// With nested metrics, sorted by max amount descending
aggs = await parser.BuildAggregationsAsync("terms:(category -max:amount min:amount)");

// Include only matching patterns
aggs = await parser.BuildAggregationsAsync("terms:(status @include:active* @include:pending)");
```

**Response structure:**

```json
{
  "terms_category": {
    "buckets": [
      {
        "key": "electronics",
        "doc_count": 150,
        "max_amount": { "value": 999.99 },
        "min_amount": { "value": 9.99 }
      },
      {
        "key": "clothing",
        "doc_count": 120,
        "max_amount": { "value": 299.99 },
        "min_amount": { "value": 19.99 }
      }
    ]
  }
}
```

### date

Date histogram aggregation for time-series data.

| Modifier | Description |
|----------|-------------|
| `~` | Interval: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, or duration (`1h`, `30m`, `1d`) |
| `^` | Timezone (e.g., `+01:00`, `America/Los_Angeles`) |
| `@missing` | Value for missing documents |
| `@offset` | Bucket offset (e.g., `+6h`, `-1d`) |

```
date:field
date:field~month
date:field~1h
date:field~day^America/New_York
date:(field~week @offset:1d)
```

**Examples:**

```csharp
// Daily buckets
var aggs = await parser.BuildAggregationsAsync("date:created~day");

// Hourly buckets with timezone
aggs = await parser.BuildAggregationsAsync("date:created~1h^America/New_York");

// Monthly with nested metrics
aggs = await parser.BuildAggregationsAsync("date:(created~month min:amount max:amount sum:amount)");

// With offset (start buckets at 6am instead of midnight)
aggs = await parser.BuildAggregationsAsync("date:(created~day @offset:6h)");
```

### histogram

Numeric histogram with fixed intervals.

| Modifier | Description |
|----------|-------------|
| `~` | Interval (positive decimal) |

```
histogram:field
histogram:field~10
histogram:field~0.5
```

**Examples:**

```csharp
// Price ranges in $10 increments
var aggs = await parser.BuildAggregationsAsync("histogram:price~10");

// With nested metrics
aggs = await parser.BuildAggregationsAsync("histogram:(price~50 avg:quantity)");
```

### geogrid

Geohash grid aggregation for geographic data.

| Modifier | Description |
|----------|-------------|
| `~` | Precision (1-12, higher = smaller cells) |

```
geogrid:field
geogrid:field~5
```

**Examples:**

```csharp
// Default precision
var aggs = await parser.BuildAggregationsAsync("geogrid:location");

// Higher precision (smaller cells)
aggs = await parser.BuildAggregationsAsync("geogrid:location~7");
```

### missing

Creates a bucket for documents missing a field value.

```
missing:field
```

**Example:**

```csharp
// Count documents without a category
var aggs = await parser.BuildAggregationsAsync("missing:category");
```

## Other Aggregations

### tophits

Returns top documents for each bucket. Use `_` as the field name.

| Modifier | Description |
|----------|-------------|
| `~` | Number of hits to return |
| `@include` | Fields to include in results |
| `@exclude` | Fields to exclude from results |

```
tophits:_
tophits:_~5
tophits:(_ @include:title @include:date)
tophits:(_ @exclude:large_field)
```

**Examples:**

```csharp
// Top 3 documents per category
var aggs = await parser.BuildAggregationsAsync("terms:(category tophits:_~3)");

// With field filtering
aggs = await parser.BuildAggregationsAsync("terms:(category tophits:(_ ~5 @include:title @include:price))");
```

## Nested Aggregations

Bucket aggregations can contain sub-aggregations:

```csharp
// Terms with metrics
var aggs = await parser.BuildAggregationsAsync("terms:(category min:price max:price avg:price)");

// Multiple levels of nesting
aggs = await parser.BuildAggregationsAsync(
    "terms:(category terms:(subcategory min:price max:price))");

// Date histogram with terms breakdown
aggs = await parser.BuildAggregationsAsync(
    "date:(created~month terms:(status count:_))");
```

**Response structure for nested aggregations:**

```json
{
  "terms_category": {
    "buckets": [
      {
        "key": "electronics",
        "doc_count": 150,
        "min_price": { "value": 9.99 },
        "max_price": { "value": 999.99 },
        "avg_price": { "value": 249.99 }
      }
    ]
  }
}
```

## Sorting Bucket Aggregations

Sort buckets by nested metric aggregations:

| Prefix | Direction |
|--------|-----------|
| `+` | Ascending |
| `-` | Descending |

```csharp
// Sort by max price descending
var aggs = await parser.BuildAggregationsAsync("terms:(category -max:price)");

// Sort by min price ascending
aggs = await parser.BuildAggregationsAsync("terms:(category +min:price)");

// Sort by count (default is descending by doc_count)
aggs = await parser.BuildAggregationsAsync("terms:(category~10)");
```

## Field Aliases

Aggregations support [field aliases](./field-aliases):

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.identity" },
        { "amount", "transaction.amount" }
    }));

// Uses aliased fields
var aggs = await parser.BuildAggregationsAsync("terms:(user sum:amount)");
```

## Complete Example

```csharp
using Foundatio.Parsers.ElasticQueries;
using Nest;

var client = new ElasticClient();

var parser = new ElasticQueryParser(c => c
    .SetLoggerFactory(loggerFactory)
    .UseMappings(client, "orders"));

// Build complex aggregation
var aggs = await parser.BuildAggregationsAsync(@"
    date:(created~month 
        terms:(category~5 
            sum:amount 
            avg:amount 
            cardinality:customer_id
        )
        sum:amount
    )
");

// Execute search with aggregations
var response = await client.SearchAsync<Order>(s => s
    .Index("orders")
    .Size(0)  // Only aggregations, no hits
    .Query(q => q.Range(r => r.Field(f => f.Created).GreaterThan("now-1y")))
    .Aggregations(aggs));

// Process results
foreach (var monthBucket in response.Aggregations.DateHistogram("date_created").Buckets)
{
    Console.WriteLine($"Month: {monthBucket.KeyAsString}");
    
    foreach (var categoryBucket in monthBucket.Terms("terms_category").Buckets)
    {
        var sum = categoryBucket.Sum("sum_amount").Value;
        var avg = categoryBucket.Average("avg_amount").Value;
        var uniqueCustomers = categoryBucket.Cardinality("cardinality_customer_id").Value;
        
        Console.WriteLine($"  {categoryBucket.Key}: ${sum:N2} total, ${avg:N2} avg, {uniqueCustomers} customers");
    }
}
```

## Validation

Validate aggregation expressions:

```csharp
var parser = new ElasticQueryParser(c => c
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "category", "price", "created" },
        AllowedOperations = { "terms", "date", "min", "max", "avg" }
    }));

var result = await parser.ValidateAggregationsAsync("terms:(category min:price)");

if (!result.IsValid)
{
    Console.WriteLine($"Invalid: {result.Message}");
}
```

## Next Steps

- [Query Syntax](./query-syntax) - Query expression reference
- [Elasticsearch Integration](./elastic-query-parser) - Full ElasticQueryParser guide
- [Validation](./validation) - Restrict allowed operations
