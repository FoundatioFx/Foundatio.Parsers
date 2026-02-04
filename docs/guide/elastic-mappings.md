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
                .Text(t => t.Name(n => n.Title)
                    .Fields(f => f.Keyword(k => k.Name("keyword"))))
                .Keyword(k => k.Name(n => n.Status))
                .Date(d => d.Name(n => n.Created))
                .Nested<Comment>(n => n.Name(x => x.Comments))),
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

// Get the NEST property
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

```csharp
public enum FieldType
{
    Unknown,
    Text,
    Keyword,
    Date,
    Boolean,
    Long,
    Integer,
    Short,
    Byte,
    Double,
    Float,
    HalfFloat,
    ScaledFloat,
    GeoPoint,
    GeoShape,
    Nested,
    Object,
    // ... other types
}
```

## Nested Document Handling

### Automatic Nested Query Wrapping

When `UseNested()` is enabled, queries on nested fields are automatically wrapped:

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
```

### Nested Field Detection

```csharp
var resolver = parser.Configuration.MappingResolver;

// Check if field is nested
bool isNested = resolver.IsNestedPropertyType("comments");

// Get the nested path for a field
// "comments.author" -> "comments"
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
            .Text(t => t.Name(n => n.Title).AddKeywordField())
            
            // Add .sort sub-field with lowercase normalizer
            .Text(t => t.Name(n => n.Name).AddSortField())
            
            // Add both .keyword and .sort sub-fields
            .Text(t => t.Name(n => n.Description).AddKeywordAndSortFields())
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
    .Settings(s => s.AddSortNormalizer())
    .Map<MyDocument>(m => m
        .Properties(p => p
            .Text(t => t.Name(n => n.Name).AddSortField())
        )));
```

## Refreshing Mappings

Mappings are automatically refreshed from Elasticsearch at most once per minute. In most production scenarios, this automatic refresh is sufficient.

For unit tests where you're creating or modifying indices and need immediate visibility of changes, you can force a refresh:

```csharp
var resolver = parser.Configuration.MappingResolver;

// Force refresh from Elasticsearch (primarily for unit tests)
resolver.RefreshMapping();
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
    
    // The NEST IProperty for the field
    public IProperty Property { get; }
}
```

## Best Practices

### 1. Use Consistent Sub-Field Naming

```csharp
// Always use .keyword for exact matching
// Always use .sort for case-insensitive sorting
.Text(t => t.Name(n => n.Title)
    .Fields(f => f
        .Keyword(k => k.Name("keyword").IgnoreAbove(256))
        .Keyword(k => k.Name("sort").Normalizer("lowercase"))))
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

- [Elasticsearch Integration](./elastic-query-parser) - Full parser guide
- [Query Syntax](./query-syntax) - Query syntax reference
- [Aggregation Syntax](./aggregation-syntax) - Aggregation reference
