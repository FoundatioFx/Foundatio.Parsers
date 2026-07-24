# Elasticsearch Mappings

The `ElasticMappingResolver` provides intelligent field resolution based on Elasticsearch index mappings. It automatically handles analyzed vs non-analyzed fields, nested documents, and field types.

## Overview

When you configure `UseMappings()`, the parser:

1. Loads field mappings from your Elasticsearch index
2. Resolves field names to their correct paths
3. Automatically uses keyword sub-fields for sorting and aggregations
4. Detects nested fields for proper query wrapping
5. Identifies field types for appropriate query generation

## Configuration

### From Elasticsearch Client

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index"));
```

### From Type Mapping

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings<MyDocument>(client));
```

### With Custom Mapping Builder

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings<MyDocument>(
        mappingBuilder: m => m
            .Properties(p => p
                .Text(n => n.Title, t => t
                    .Fields(f => f.Keyword("keyword")))
                .Keyword(n => n.Status)
                .Date(n => n.Created)
                .Nested(x => x.Comments, n => n.Properties(np => np))),
        client,
        "my-index"));
```

### From Mapping Function

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(
        getMapping: () => GetCachedMapping(),
        inferrer: client.Infer));
```

## Field Resolution

### Automatic Keyword Field Detection

For text fields with keyword sub-fields, the resolver automatically uses the keyword field for:

- Sorting
- Aggregations
- Exact match queries

```csharp
// Mapping:
// "title": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }

var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// For queries - uses analyzed "title" field
var query = await parser.BuildQueryAsync("title:search terms");

// For aggregations - automatically uses "title.keyword"
var aggs = await parser.BuildAggregationsAsync("terms:title");

// For sort - automatically uses "title.keyword"
var sort = await parser.BuildSortAsync("title");
```

### Field Type Detection

The resolver detects field types for appropriate query handling:

```csharp
var resolver = parser.Configuration.MappingResolver;

// Check field types
bool isNested = resolver.IsNestedPropertyType("comments");
bool isGeo = resolver.IsGeoPropertyType("location");
bool isNumeric = resolver.IsNumericPropertyType("price");
bool isDate = resolver.IsDatePropertyType("created");
bool isBoolean = resolver.IsBooleanPropertyType("active");
bool isAnalyzed = resolver.IsPropertyAnalyzed("description");
```

## ElasticMappingResolver API

### Getting Field Information

```csharp
var resolver = parser.Configuration.MappingResolver;

// Get full field mapping
var mapping = resolver.GetMapping("user.name");
if (mapping.Found)
{
    Console.WriteLine($"Full path: {mapping.FullPath}");
    Console.WriteLine($"Property type: {mapping.Property?.GetType().Name}");
}

// Get the Elasticsearch property
IProperty property = resolver.GetMappingProperty("status");

// Get resolved field name
string resolved = resolver.GetResolvedField("user");

// Get non-analyzed field for sorting
string sortField = resolver.GetSortFieldName("title");

// Get non-analyzed field for aggregations
string aggField = resolver.GetAggregationsFieldName("category");

// Get field type enum
FieldType fieldType = resolver.GetFieldType("price");
```

### Field Type Enum

`GetFieldType` returns a `FieldType` enum with values such as:

- **Text**: `text`, `match_only_text`, `search_as_you_type` (legacy `string` also supported)
- **Keyword**: `keyword`, `constant_keyword`, `wildcard`
- **Numeric**: `long`, `unsigned_long`, `integer`, `short`, `byte`, `double`, `float`, `half_float`, `scaled_float`
- **Date**: `date`, `date_nanos`
- **Range**: `integer_range`, `float_range`, `long_range`, `double_range`, `date_range`, `ip_range`
- **Geo**: `geo_point`, `geo_shape`, `point`, `shape`
- **Structured**: `nested`, `object`, `flattened`, `join`
- **Other**: `boolean`, `ip`, `binary`, `completion`, `murmur3`, `token_count`, `percolator`, `alias`, `rank_feature`, `rank_features`, `histogram`, `dense_vector`, `version`

Unrecognized types return `FieldType.None`.

```csharp
FieldType fieldType = resolver.GetFieldType("price");
if (fieldType == FieldType.Double || fieldType == FieldType.Float)
{
    // Handle numeric field
}
```

## Nested Document Handling

For a detailed explanation of how visitors traverse nested query structures, field scoping rules, and the full AST breakdown, see [Nested Queries and Visitor Traversal](./nested-queries).

### Automatic Nested Query Wrapping

When `UseNested()` is enabled, queries on nested fields are automatically wrapped. This includes individual field queries, grouped queries, negated groups, exists/missing queries, wildcard queries, aggregations, and sorting:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNested());

// Query on nested field
var query = await parser.BuildQueryAsync("comments.author:john");

// Automatically generates:
// {
//   "nested": {
//     "path": "comments",
//     "query": {
//       "term": { "comments.author": "john" }
//     }
//   }
// }

// Negated nested groups also produce correct structure
query = await parser.BuildQueryAsync("NOT comments:(comments.author:spammer)");

// Exists/missing on nested fields are wrapped automatically
query = await parser.BuildQueryAsync("_exists_:comments.author");
```

### Nested Field Detection

```csharp
var resolver = parser.Configuration.MappingResolver;

// Check if field is nested
bool isNested = resolver.IsNestedPropertyType("comments");

// Get the nested path for a field
// "comments.author" -> "comments"
```

### Filtered Nested Queries

When multiple logical types share a nested array, use `UseNestedFilter()` to inject a discriminator filter:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNestedFilter((nestedPath, originalField, resolvedField, context) =>
    {
        if (nestedPath is "comments")
            return new TermQuery { Field = "comments.type", Value = "public" };

        return null;
    })
    .UseNested());

// Query: comments.author:john
// Produces:
// {
//   "nested": {
//     "path": "comments",
//     "query": {
//       "bool": {
//         "must": [
//           { "term": { "comments.author": "john" } },
//           { "term": { "comments.type": "public" } }
//         ]
//       }
//     }
//   }
// }

// Aggregation: max:comments.rating
// Produces: nested > filter(comments.type=public) > max(comments.rating)

// Sort: -comments.rating
// Produces: sort with nested(path=comments, filter=term(comments.type, public))
```

## Mapping Extensions

### Adding Keyword Sub-Fields

Use extension methods to add standard sub-fields to your mappings:

```csharp
using Foundatio.Parsers.ElasticQueries.Extensions;

var createIndexResponse = await client.Indices.CreateAsync("my-index", c => c
    .Map<MyDocument>(m => m
        .Properties(p => p
            // Add .keyword sub-field
            .Text(n => n.Title, t => t.AddKeywordField())

            // Add .sort sub-field with lowercase normalizer
            .Text(n => n.Name, t => t.AddSortField())


            // Add both .keyword and .sort sub-fields
            .Text(n => n.Description, t => t.AddKeywordAndSortFields())
        )));
```

### Sub-Field Names

```csharp
using Foundatio.Parsers.ElasticQueries.Extensions;

// Default sub-field names
string keywordField = ElasticMappingExtensions.KeywordFieldName; // "keyword"
string sortField = ElasticMappingExtensions.SortFieldName;       // "sort"
```

### Sort Normalizer

Add a lowercase normalizer for case-insensitive sorting:

```csharp
var createIndexResponse = await client.Indices.CreateAsync("my-index", c => c
    .Settings(s => s.Analysis(a => a.AddSortNormalizer()))
    .Map<MyDocument>(m => m
        .Properties(p => p
            .Text(n => n.Name, t => t.AddSortField())
        )));
```

## Refreshing Mappings

Mappings are reloaded from Elasticsearch automatically. The resolver uses two independent throttles so that
dynamically created fields become visible quickly without turning every query into a `GetMapping` call:

| Trigger | Setting | Default | Behavior |
| --- | --- | --- | --- |
| The loaded mapping is stale | `MappingRefreshInterval` | 1 minute | Maximum age of the loaded mapping before an ordinary resolution reloads it. |
| A field could not be resolved | `UnmappedFieldRefreshInterval` | 1 second | A resolution failure is the strongest signal the index mapping changed, so it reloads on a much shorter interval. |
| A reload is already running | `FetchJoinTimeout` | 30 seconds | How long a resolution waits to join an in-flight reload instead of issuing its own. |

A field that cannot be resolved is the normal outcome for fields created at runtime — dynamic templates
(including the `idx.*` custom field templates used by Foundatio.Repositories) only add a field to the index
mapping after the first document that uses it is indexed. Because of that, an unresolved field reloads the
server mapping on its own short interval and is never blocked by the mapping having been loaded at startup.

Reloads that still do not resolve the field back off exponentially (1s, 2s, 4s, 8s, up to
`MappingRefreshInterval`), so a flood of queries against fields that genuinely do not exist cannot hammer the
cluster. The interval resets to the base value as soon as a reload does resolve a field. Concurrent lookups of
an unmapped field are coalesced into a single `GetMapping` call, so the worst case reload rate is one per
`UnmappedFieldRefreshInterval` per resolver, and only while unresolved fields are actually being queried.

```csharp
var resolver = parser.Configuration.MappingResolver;

// Reload sooner (or set to TimeSpan.Zero to always reload when a field cannot be resolved)
resolver.UnmappedFieldRefreshInterval = TimeSpan.FromMilliseconds(250);
resolver.MappingRefreshInterval = TimeSpan.FromMinutes(5);
```

### Waiting For An In-Flight Reload

Only one mapping reload runs at a time. Other resolutions that need a fresh mapping wait for that reload
rather than issuing their own, because waiting is never more expensive than performing the fetch. If the wait
exceeds `FetchJoinTimeout`, the resolution gives up and treats the field as unmapped, and a warning is logged.

`FetchJoinTimeout` must therefore comfortably exceed how long your `GetMapping` call takes. It defaults to 30
seconds, which is far above a normal round trip, but the Elasticsearch client permits a request to run for up
to its own request timeout (10 minutes by default). Raise it if your mapping fetch can legitimately run
longer, or set it to a negative value to wait indefinitely:

```csharp
resolver.FetchJoinTimeout = TimeSpan.FromMinutes(2);
```

### Residual Staleness

Elasticsearch does not expose a cheap way to ask whether an index mapping has changed, so the resolver cannot
detect a schema change without fetching the whole mapping. That means a field created between reloads can
still resolve as unmapped for up to `UnmappedFieldRefreshInterval`. If your workload cannot tolerate any
window, use `InvalidateFieldMapping` at the point the field is created (see below) or set
`UnmappedFieldRefreshInterval` to `TimeSpan.Zero` to reload on every unresolved field.

### Invalidating a Single Field

When you know a specific field was just created, invalidate only that field instead of discarding the whole
cache. This drops the cached resolution for the field and allows the next resolution to reload the mapping
immediately:

```csharp
resolver.InvalidateFieldMapping("idx.string-000001");
```

### Forcing a Full Refresh

`RefreshMapping()` bypasses both throttles and discards the entire field cache. It is intended for unit tests
that create or modify indices and need immediate visibility of the change. Prefer `InvalidateFieldMapping` in
production code — clearing the whole cache re-resolves and re-merges every field on a large mapping.

```csharp
// Force refresh from Elasticsearch (primarily for unit tests)
resolver.RefreshMapping();
```

### Bounding Cache Memory

Field names come from user supplied queries, so the resolved field cache is bounded. Once
`MaxCachedFields` (default 10,000) is exceeded the cache is cleared and a warning is logged.
`CachedFieldCount` reports the approximate current size.

```csharp
resolver.MaxCachedFields = 50_000;
```

## Custom Mapping Resolver

Create a custom resolver for special cases:

```csharp
var customResolver = ElasticMappingResolver.Create(
    getMapping: () => {
        // Return cached or custom mapping
        return _cachedMapping;
    },
    inferrer: client.Infer,
    logger: logger);

var parser = new ElasticQueryParser(c => c
    .UseMappings(customResolver));
```

## Field Mapping Structure

The `FieldMapping` class contains:

```csharp
public class FieldMapping
{
    // Whether the field was found in mappings
    public bool Found { get; }

    // The full resolved path (e.g., "data.user.name")
    public string FullPath { get; }

    // The Elasticsearch IProperty for the field
    public IProperty Property { get; }
}
```

## Best Practices

### 1. Use Consistent Sub-Field Naming

```csharp
// Always use .keyword for exact matching
// Always use .sort for case-insensitive sorting
.Text(n => n.Title, t => t
    .Fields(f => f
        .Keyword("keyword", k => k.IgnoreAbove(256))
        .Keyword("sort", k => k.Normalizer("lowercase"))))
```

### 2. Cache Mapping Resolution

The resolver caches mappings automatically, but you can also:

```csharp
// Create resolver once and reuse
var resolver = ElasticMappingResolver.Create(client, "my-index");

var parser1 = new ElasticQueryParser(c => c.UseMappings(resolver));
var parser2 = new ElasticQueryParser(c => c.UseMappings(resolver));
```

### 3. Handle Dynamic Mappings

For indices with dynamic mappings:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        // Allow fields not in current mapping
        AllowUnresolvedFields = true
    }));
```

### 4. Log Mapping Issues

```csharp
var parser = new ElasticQueryParser(c => c
    .SetLoggerFactory(loggerFactory)
    .UseMappings(client, "my-index"));

// Mapping resolution issues will be logged
```

## Troubleshooting

### Field Not Found

```csharp
var resolver = parser.Configuration.MappingResolver;
var mapping = resolver.GetMapping("unknown_field");

if (!mapping.Found)
{
    // Field doesn't exist in mapping
    // Check: spelling, case sensitivity, nested path
}
```

### Wrong Field Type Used

```csharp
// Check what type the resolver sees
var fieldType = resolver.GetFieldType("my_field");
Console.WriteLine($"Field type: {fieldType}");

// Check if analyzed
bool isAnalyzed = resolver.IsPropertyAnalyzed("my_field");
Console.WriteLine($"Is analyzed: {isAnalyzed}");
```

### Nested Queries Not Working

```csharp
// Ensure UseNested() is configured
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNested());  // Required for nested support

// Verify field is detected as nested
bool isNested = resolver.IsNestedPropertyType("comments");
```

## Next Steps

- [Nested Queries and Visitor Traversal](./nested-queries) - How visitors handle nested document queries
- [Elasticsearch Integration](./elastic-query-parser) - Full parser guide
- [Query Syntax](./query-syntax) - Query syntax reference
- [Aggregation Syntax](./aggregation-syntax) - Aggregation reference
