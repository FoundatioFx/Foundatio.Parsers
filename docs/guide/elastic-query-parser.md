# Elasticsearch Integration

The `ElasticQueryParser` extends the base Lucene parser to build NEST query objects for Elasticsearch. It provides a powerful replacement for Elasticsearch's `query_string` query with additional features.

## Installation

```bash
dotnet add package Foundatio.Parsers.ElasticQueries
```

## Basic Usage

```csharp
using Foundatio.Parsers.ElasticQueries;
using Nest;

var client = new ElasticClient();

var parser = new ElasticQueryParser(c => c
    .SetLoggerFactory(loggerFactory)
    .UseMappings(client, "my-index"));

// Build a query
QueryContainer query = await parser.BuildQueryAsync("status:active AND created:>2024-01-01");

// Use in search
var response = await client.SearchAsync<MyDocument>(s => s
    .Index("my-index")
    .Query(_ => query));
```

## Configuration

### Basic Configuration

```csharp
var parser = new ElasticQueryParser(c => c
    // Logging
    .SetLoggerFactory(loggerFactory)
    
    // Default fields for unqualified terms
    .SetDefaultFields(new[] { "title", "description", "content" })
    
    // Field mappings from Elasticsearch
    .UseMappings(client, "my-index")
    
    // Field aliases
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.identity" },
        { "created", "metadata.createdAt" }
    })
    
    // Query includes
    .UseIncludes(new Dictionary<string, string> {
        { "active", "status:active AND deleted:false" }
    })
    
    // Validation
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "status", "name", "created" }
    })
    
    // Geo query support
    .UseGeo(location => ResolveGeoLocation(location))
    
    // Nested document support
    .UseNested());
```

### Configuration Methods

| Method | Description |
|--------|-------------|
| `SetLoggerFactory(factory)` | Set logging factory |
| `SetDefaultFields(fields)` | Default fields for unqualified terms |
| `UseMappings(client, index)` | Load mappings from Elasticsearch |
| `UseFieldMap(map)` | Static field alias map |
| `UseFieldResolver(resolver)` | Dynamic field resolver |
| `UseIncludes(includes)` | Query include definitions |
| `UseGeo(resolver)` | Geo location resolver |
| `UseNested()` | Enable nested document support |
| `UseNestedFilter(resolver)` | Nested filter resolver for injecting filters into nested queries/aggs/sorts |
| `SetValidationOptions(options)` | Validation configuration |
| `UseRuntimeFieldResolver(resolver)` | Runtime field support |

## Building Queries

### Simple Queries

```csharp
var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// Term query
var query = await parser.BuildQueryAsync("status:active");

// Range query
query = await parser.BuildQueryAsync("price:[100 TO 500]");

// Boolean query
query = await parser.BuildQueryAsync("status:active AND category:electronics");

// Phrase query
query = await parser.BuildQueryAsync("title:\"quick brown fox\"");
```

### With Scoring

By default, queries are wrapped in a `bool` filter (no scoring). Enable scoring for relevance:

```csharp
var context = new ElasticQueryVisitorContext { UseScoring = true };
var query = await parser.BuildQueryAsync("title:search terms", context);
```

Or use the search mode helper:

```csharp
var context = new ElasticQueryVisitorContext().UseSearchMode();
var query = await parser.BuildQueryAsync("search terms", context);
```

### From Parsed AST

```csharp
// Parse first
var ast = await parser.ParseAsync("status:active");

// Build query from AST
var query = await parser.BuildQueryAsync(ast, context);
```

## Building Aggregations

```csharp
var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// Build aggregations
AggregationContainer aggs = await parser.BuildAggregationsAsync(
    "terms:(category min:price max:price avg:price)");

// Use in search
var response = await client.SearchAsync<MyDocument>(s => s
    .Index("my-index")
    .Size(0)
    .Aggregations(aggs));
```

### Complex Aggregations

```csharp
// Date histogram with nested terms
var aggs = await parser.BuildAggregationsAsync(
    "date:(created~month terms:(status sum:amount))");

// Multiple aggregations
aggs = await parser.BuildAggregationsAsync(
    "min:price max:price avg:price terms:category~10");
```

## Building Sort

```csharp
var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// Build sort (- for descending, + for ascending)
IEnumerable<IFieldSort> sort = await parser.BuildSortAsync("-created +name");

// Use in search
var response = await client.SearchAsync<MyDocument>(s => s
    .Index("my-index")
    .Sort(sort));
```

## Validation

### Validate Before Building

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "status", "name", "created" },
        AllowLeadingWildcards = false
    }));

// Validate query
var result = await parser.ValidateQueryAsync("status:active");
if (!result.IsValid)
{
    Console.WriteLine($"Invalid: {result.Message}");
    return;
}

// Validate aggregations
result = await parser.ValidateAggregationsAsync("terms:category");

// Validate sort
result = await parser.ValidateSortAsync("-created");
```

### Automatic Validation

`BuildQueryAsync` automatically validates and throws on failure:

```csharp
try
{
    var query = await parser.BuildQueryAsync("invalid:field");
}
catch (QueryValidationException ex)
{
    Console.WriteLine($"Validation failed: {ex.Message}");
}
```

## Visitor Management

### Adding Custom Visitors

```csharp
var parser = new ElasticQueryParser(c => c
    // Add to all visitor chains (query, aggregation, sort)
    .AddVisitor(new MyCustomVisitor(), priority: 100)
    
    // Add only to query visitor chain
    .AddQueryVisitor(new QueryOnlyVisitor(), priority: 50)
    
    // Add only to aggregation visitor chain
    .AddAggregationVisitor(new AggOnlyVisitor(), priority: 50)
    
    // Add only to sort visitor chain
    .AddSortVisitor(new SortOnlyVisitor(), priority: 50));
```

### Visitor Chain Management

```csharp
var parser = new ElasticQueryParser(c => c
    // Remove a visitor
    .RemoveVisitor<ValidationVisitor>()
    
    // Replace a visitor
    .ReplaceVisitor<FieldResolverQueryVisitor>(new MyFieldResolver(), newPriority: 15)
    
    // Add before/after specific visitor
    .AddVisitorBefore<ValidationVisitor>(new PreValidationVisitor())
    .AddVisitorAfter<FieldResolverQueryVisitor>(new PostResolverVisitor()));
```

## Geo Queries

### Configuration

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseGeo(async location => {
        // Resolve location string to coordinates
        if (location.Length == 5 && int.TryParse(location, out _))
        {
            // Zip code lookup
            var coords = await geocoder.ResolveZipCode(location);
            return $"{coords.Lat},{coords.Lon}";
        }
        return location;
    }));
```

### Proximity Queries

```csharp
// Within 75 miles of zip code
var query = await parser.BuildQueryAsync("location:75044~75mi");

// Within 10 kilometers of coordinates
query = await parser.BuildQueryAsync("location:51.5,-0.1~10km");
```

### Bounding Box Queries

```csharp
// Bounding box
var query = await parser.BuildQueryAsync("location:[51.5,-0.2 TO 51.4,-0.1]");
```

## Nested Documents

Enable nested document support:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNested());
```

Calling `UseNested()` registers the `NestedVisitor` at priority 300 in the visitor chain. This visitor checks each `GroupNode` with a non-empty `Field` against the Elasticsearch mapping to determine if the field is a nested type. If it is, the visitor tags the node with a `NestedQuery` containing the resolved nested path.

Later, `CombineQueriesVisitor` (priority 10000) assembles child queries and wraps them inside the `NestedQuery`, producing the correct Elasticsearch nested query structure automatically.

### Grouped Nested Queries

When querying nested fields, you can group them to produce a single nested query block:

```csharp
// Multiple nested fields grouped together
var query = await parser.BuildQueryAsync(
    "field1:value1 nested:(nested.field1:value1 nested.field4:4)");
```

This produces a top-level bool query combining a match on `field1` with a single `nested` query wrapping the inner terms.

### Individual Nested Field Queries

Queries on individual nested fields (without an explicit group) are also automatically wrapped:

```csharp
var query = await parser.BuildQueryAsync("nested.field4:5");
```

### Nested Aggregations

Aggregations on nested fields are automatically wrapped in a nested aggregation:

```csharp
var aggs = await parser.BuildAggregationsAsync("terms:nested.field1 max:nested.field4");
```

### Negated Nested Queries

Negated nested groups are fully supported. Use `NOT` to exclude nested documents matching specific criteria:

```csharp
// Exclude documents where nested.field1 matches "excluded_value"
var query = await parser.BuildQueryAsync("NOT nested:(nested.field1:excluded_value)");
```

This produces a `bool > must_not > nested` query structure.

You can also combine negation with other nested conditions inside a single group:

```csharp
// Negated inner nested group OR'd with another nested field
var query = await parser.BuildQueryAsync(
    "nested:(-nested:(nested.field1:excluded) OR nested.field4:10)");
```

### Nested Sort

Sorting by nested fields is automatically handled with the correct `nested` context:

```csharp
// Sort descending by a nested field
var sort = await parser.BuildSortAsync("-nested.field4");
```

### Nested Filter Resolver

When multiple logical types share a single nested array (e.g., different reseller tiers), you can inject a discriminator filter inside nested queries, aggregations, and sorts using `UseNestedFilter()`:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNestedFilter((nestedPath, originalField, resolvedField, context) =>
    {
        if (nestedPath == "resellers")
            return new TermQuery { Field = "resellers.type", Value = "official" };
        return null;
    })
    .UseNested());
```

The resolver is called once per nested field node during visitor traversal. Return `null` to skip filtering for a given field.

**Queries** -- The filter is AND-ed into the nested query's inner query:

```csharp
// Input: resellers.price:10
// Output: nested(path=resellers, query=(term(resellers.price, 10) AND term(resellers.type, official)))
```

**Aggregations** -- Each nested aggregation is wrapped in a `FilterAggregation`:

```csharp
// Input: max:resellers.price
// Output: nested(path=resellers) > filter(term(resellers.type, official)) > max(resellers.price)
```

**Sorts** -- The filter is set on `NestedSort.Filter`:

```csharp
// Input: -resellers.price
// Output: sort(resellers.price, desc, nested(path=resellers, filter=term(resellers.type, official)))
```

A synchronous overload is also available for resolvers that don't need async:

```csharp
.UseNestedFilter((path, orig, resolved, ctx) =>
    path == "resellers" ? new TermQuery { Field = "resellers.type", Value = "official" } : null)
```

**Call order safety**: `UseNestedFilter()` and `UseNested()` can be called in any order. The resolver is always passed to the `NestedVisitor` correctly.

### Exists and Missing on Nested Fields

Exists and missing queries on nested fields are automatically wrapped in nested queries:

```csharp
// Check if a specific nested sub-field exists
var query = await parser.BuildQueryAsync("_exists_:nested.field1");

// Check if the nested object itself exists
query = await parser.BuildQueryAsync("_exists_:nested");

// Missing queries also work
query = await parser.BuildQueryAsync("_missing_:nested.field1");
```

### Wildcards on Nested Fields

Wildcard queries on nested fields are wrapped appropriately based on the field's analysis type:

```csharp
// Wildcard on an analyzed (text) field -- produces query_string inside nested
var query = await parser.BuildQueryAsync("nested.field1:val*");

// Wildcard on a non-analyzed (keyword) field -- produces prefix inside nested
query = await parser.BuildQueryAsync("nested.field5:val*");
```

### Default Fields with Nested Types

When default fields include both nested and non-nested fields, queries are automatically split and combined:

```csharp
parser.SetDefaultFields(["field1", "nested.field1", "nested.field2"]);

// Unqualified search term produces:
// match(field1, "term") OR nested(multi_match(nested.field1, nested.field2, "term"))
var query = await parser.BuildQueryAsync("searchterm");
```

### Known Limitations

- **Multi-level deeply nested types** -- Fields nested more than one level deep (e.g., `parent.child.field1` where both `parent` and `parent.child` are nested types) are wrapped at the outermost nested path only. The inner nested wrapper required by Elasticsearch for multi-level nesting is not generated automatically.

For a detailed explanation of how visitors traverse nested query structures, field scoping rules, and the full AST breakdown, see [Nested Queries and Visitor Traversal](./nested-queries).

## Runtime Fields

### Opt-In Runtime Fields

```csharp
var parser = new ElasticQueryParser(c => c
    .UseOptInRuntimeFieldResolver(async field => {
        // Return runtime field definition if field should be runtime
        if (field == "full_name")
        {
            return new ElasticRuntimeField {
                Name = "full_name",
                FieldType = ElasticRuntimeFieldType.Keyword,
                Script = "emit(doc['first_name'].value + ' ' + doc['last_name'].value)"
            };
        }
        return null;
    }));

// Enable runtime fields for specific query
var context = new ElasticQueryVisitorContext();
context.EnableRuntimeFieldResolver(true);

var query = await parser.BuildQueryAsync("full_name:John*", context);

// Access runtime fields to include in search
var runtimeFields = context.RuntimeFields;
```

### Always-On Runtime Fields

```csharp
var parser = new ElasticQueryParser(c => c
    .UseRuntimeFieldResolver(async field => {
        // Always check for runtime fields
        return await GetRuntimeFieldDefinition(field);
    }));
```

## Complete Example

```csharp
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Nest;

public class SearchService
{
    private readonly IElasticClient _client;
    private readonly ElasticQueryParser _parser;

    public SearchService(IElasticClient client, ILoggerFactory loggerFactory)
    {
        _client = client;
        _parser = new ElasticQueryParser(c => c
            .SetLoggerFactory(loggerFactory)
            .UseMappings(client, "products")
            .SetDefaultFields(new[] { "name", "description" })
            .UseFieldMap(new Dictionary<string, string> {
                { "category", "category.keyword" },
                { "brand", "brand.keyword" }
            })
            .UseIncludes(new Dictionary<string, string> {
                { "available", "status:active AND inventory:>0" },
                { "sale", "discount:>0" }
            })
            .UseGeo(ResolveLocation)
            .UseNested()
            .SetValidationOptions(new QueryValidationOptions {
                AllowedFields = { "name", "description", "category", "brand", 
                                  "price", "status", "inventory", "location" },
                AllowLeadingWildcards = false,
                AllowedMaxNodeDepth = 10
            }));
    }

    public async Task<SearchResult> SearchAsync(
        string query, 
        string aggregations = null,
        string sort = null,
        int page = 1,
        int pageSize = 20)
    {
        // Validate inputs
        var validation = await _parser.ValidateQueryAsync(query);
        if (!validation.IsValid)
            throw new ArgumentException(validation.Message);

        // Build query with scoring for relevance
        var context = new ElasticQueryVisitorContext().UseSearchMode();
        var esQuery = await _parser.BuildQueryAsync(query, context);

        // Build search request
        var searchDescriptor = new SearchDescriptor<Product>()
            .Index("products")
            .From((page - 1) * pageSize)
            .Size(pageSize)
            .Query(_ => esQuery);

        // Add aggregations if provided
        if (!string.IsNullOrEmpty(aggregations))
        {
            var aggs = await _parser.BuildAggregationsAsync(aggregations);
            searchDescriptor.Aggregations(aggs);
        }

        // Add sort if provided
        if (!string.IsNullOrEmpty(sort))
        {
            var sortFields = await _parser.BuildSortAsync(sort);
            searchDescriptor.Sort(sortFields);
        }

        var response = await _client.SearchAsync<Product>(searchDescriptor);

        return new SearchResult
        {
            Total = response.Total,
            Items = response.Documents.ToList(),
            Aggregations = response.Aggregations
        };
    }

    private string ResolveLocation(string location)
    {
        // Implement location resolution
        return location;
    }
}
```

## Next Steps

- [Nested Queries and Visitor Traversal](./nested-queries) - Nested query handling and traversal details
- [Elasticsearch Mappings](./elastic-mappings) - Mapping resolver details
- [Aggregation Syntax](./aggregation-syntax) - Aggregation expression reference
- [Query Syntax](./query-syntax) - Query syntax reference
- [Custom Visitors](./custom-visitors) - Create custom visitors
