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

// Queries on nested fields are automatically wrapped
var query = await parser.BuildQueryAsync("comments.author:john");
```

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

- [Elasticsearch Mappings](./elastic-mappings) - Mapping resolver details
- [Aggregation Syntax](./aggregation-syntax) - Aggregation expression reference
- [Query Syntax](./query-syntax) - Query syntax reference
- [Custom Visitors](./custom-visitors) - Create custom visitors
