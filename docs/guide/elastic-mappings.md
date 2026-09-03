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

The resolver loads the server mapping on first use. After that, an unresolved field is the signal that the
mapping may have changed, so it can trigger a rate-limited reload:

| Trigger | Setting | Default | Behavior |
| --- | --- | --- | --- |
| A field could not be resolved | `UnmappedFieldRefreshInterval` | 5 seconds | Minimum time between completed miss-driven reload attempts. Must be greater than zero. |
| A reload is already running | `MappingRefreshWaitTimeout` | 30 seconds | How long a resolution waits to join an in-flight reload instead of issuing its own. |

A field that cannot be resolved is the normal outcome for fields created at runtime — dynamic templates
(including the `idx.*` custom field templates used by Foundatio.Repositories) only add a field to the index
mapping after the first document that uses it is indexed. Because of that, an unresolved field reloads the
server mapping on its own short interval and is never blocked by the mapping having been loaded at startup.

Concurrent reload attempts are coalesced into a single server mapping fetch per resolver. This protection is
local to the resolver instance, so create one long-lived resolver per concrete index and process. Creating a
resolver per request defeats both caching and request coalescing. With the five-second default, each resolver
that is continuously receiving misses can make up to about 12 reload attempts per minute, excluding cold
starts and explicit refreshes. There is no distributed cache or cross-process coordination, so this ceiling
applies independently to each resolver in each process.

Before this miss-specific cooldown, successful and null mapping fetches were normally limited by the
one-minute mapping refresh interval, while a callback exception could be retried by every sequential miss.
The five-second default therefore improves dynamic-field discovery while placing the same finite ceiling on
successful, null, and exceptional automatic reloads.

```csharp
var resolver = parser.Configuration.MappingResolver;

// Reduce reload pressure for typo-heavy workloads.
resolver.UnmappedFieldRefreshInterval = TimeSpan.FromSeconds(30);
```

### Waiting For An In-Flight Reload

Only one automatic mapping reload runs at a time. Other resolutions that need a fresh mapping wait for that
reload rather than issuing their own. An explicit `RefreshMapping()` supersedes an obsolete in-flight load,
so the next resolution can fetch the replacement mapping without waiting for the old callback to drain. That
hard-invalidation case can temporarily overlap the obsolete physical callback. If the wait exceeds
`MappingRefreshWaitTimeout`, the resolution gives up and
treats the field as unmapped, and a warning is logged. A single resolution joins at most once; it does not
repeat the same wait as both an initial load and a miss-driven reload.

The mapping callback must have its own finite timeout and must not call back into the same resolver. The
built-in client factories use the Elasticsearch client's configured request timeout. Configure that timeout
below `MappingRefreshWaitTimeout` if every joining resolution must observe the fetch result; otherwise a
joining resolution can time out while the single fetch continues.

```csharp
resolver.MappingRefreshWaitTimeout = TimeSpan.FromMinutes(2);
```

### Residual Staleness

The five-second default is a cooldown, not a correctness deadline. Callback latency, failures, and a reload
that began before the field was created can extend the stale window. If an automatic reload fails or returns
no mapping, the resolver retains its last known mapping and throttles the next attempt. During that window an
unresolved field is still treated as unmapped. Set `AllowUnresolvedFields` to `false` when failing validation
is safer than generating a query from incomplete mapping information. This catches unresolved query fields,
but the resolver's boolean type checks do not expose a separate "mapping unknown" state, so use explicit
refresh for application-controlled changes that require immediate correctness.

Successful field resolutions do not trigger periodic reloads. New fields and sub-fields create misses and
are discovered automatically, but changes to an already resolved alias or runtime-field definition require
an explicit refresh.

### Forcing a Full Refresh

`RefreshMapping()` bypasses the miss throttle and discards the current mapping snapshot. Call it after
Elasticsearch acknowledges an application-controlled mapping change; the next resolution fetches or joins
the new mapping.

```csharp
resolver.RefreshMapping();
```

Do not call `RefreshMapping()` after every indexed document. Dynamically materialized fields already use the
miss-driven reload path, while repeated full invalidation discards useful caches and can continually
supersede in-flight fetches. Foundatio.Repositories should keep its long-lived per-index resolver and rely on
that path for ordinary custom-field saves.

### Cache Memory

Successful resolutions are cached by canonical field path for the lifetime of the mapping snapshot, which
keeps the cache bounded by the mapping itself no matter how many distinct spellings callers ask for.
Unknown field names are not cached in the long-lived snapshot, so caller-controlled misses cannot grow
process state. A parser operation remembers its own positive and negative resolutions only until that operation
finishes, preventing downstream visitors from repeating the same network-backed lookup. Direct query, sort,
and aggregation helpers also release their temporary results on return or failure, so reusing a visitor context
does not retain stale mappings across operations. Cache keys preserve exact field-name casing. Reloading the mapping
publishes a new snapshot and atomically discards resolutions derived from the old one.

The built-in client factories expect one concrete index, or a target whose indices have equivalent mappings.
They do not merge heterogeneous mappings from rollover aliases or data streams.

## Custom Mapping Resolver

Use an asynchronous loader when obtaining the mapping requires network I/O. The parser's asynchronous query,
sort, and aggregation paths await this loader without blocking a request thread. Cancelling one resolver
lookup cancels only that caller's wait; the shared fetch continues for other callers and is bounded by the
resolver lifetime and the loader's transport timeout.

Custom asynchronous loaders use the named `CreateWithAsyncLoader` and `UseMappingsWithAsyncLoader` entry
points. Keeping them separate from the synchronous delegate overloads preserves source compatibility for
existing callers.

```csharp
var customResolver = ElasticMappingResolver.CreateWithAsyncLoader(
    getMappingAsync: cancellationToken => LoadMappingAsync(cancellationToken),
    inferrer: client.Infer,
    logger: logger);
```

Synchronous loader overloads remain available for compatibility. An asynchronous parser call configured
with a synchronous loader must invoke that loader synchronously, and an explicit synchronous resolver call
configured with only an asynchronous loader waits synchronously for it. That compatibility path dispatches
the single shared load to the thread pool to avoid synchronization-context deadlocks, but the caller still
blocks. A synchronous `Parse` call made under a custom synchronization context or task scheduler similarly
isolates the compatibility call; server request paths should use the asynchronous resolver and parser APIs.
The built-in Elasticsearch client factories supply both forms: asynchronous parser paths call
`Indices.GetMappingAsync`, while synchronous resolver calls retain `Indices.GetMapping`.

On the default task scheduler, the asynchronous loader starts inline and does not use `Task.Run`. When a
custom synchronization context or task scheduler is active, only the loader boundary is dispatched to the
thread pool so a synchronous caller joining the same shared fetch cannot block the loader's captured
continuation. Library-owned awaits still use `AnyContext()`.

The loaded mapping and its successful field memoization belong to an immutable resolver-local snapshot; an
external cache client is neither required nor used. `RefreshMapping()` remains synchronous because it only
invalidates that local snapshot and performs no I/O.

For an already synchronous or in-memory source, use the compatibility overload:

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

For correctness-sensitive indices with dynamic mappings, fail validation if a field is still unresolved
after the bounded reload attempt rather than generating a query from incomplete mapping information:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        AllowUnresolvedFields = false
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
