# Getting Started

This guide walks you through installing Foundatio.Parsers and using it to parse your first query.

## Installation

Install the package for your use case:

::: code-group

```bash [Lucene Parser Only]
dotnet add package Foundatio.Parsers.LuceneQueries
```

```bash [Elasticsearch Integration]
dotnet add package Foundatio.Parsers.ElasticQueries
```

```bash [SQL/EF Core Integration]
dotnet add package Foundatio.Parsers.SqlQueries
```

:::

## Basic Usage

### Parsing a Query

The `LuceneQueryParser` parses query strings into an Abstract Syntax Tree (AST):

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();

// Parse a query string
var result = parser.Parse("field:value AND other:[1 TO 10]");

// Inspect the AST structure
Console.WriteLine(DebugQueryVisitor.Run(result));
```

Output:

```
Group:
  Left - Group:
    Operator: And
    Left - Term:
        Field: field
        Term: value
    Right - Term:
        Field: other
        TermMin: 1
        TermMax: 10
        MinInclusive: True
        MaxInclusive: True
```

### Regenerating the Query

Use `GenerateQueryVisitor` to convert the AST back to a query string:

```csharp
var parser = new LuceneQueryParser();
var result = parser.Parse("field:[1 TO 2]");

string generatedQuery = GenerateQueryVisitor.Run(result);
Console.WriteLine(generatedQuery); // Output: field:[1 TO 2]
```

### Async Parsing

For async workflows, use `ParseAsync`:

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("status:active");

string query = await GenerateQueryVisitor.RunAsync(result);
```

## Elasticsearch Integration

The `ElasticQueryParser` extends the base parser to build NEST query objects:

```csharp
using Foundatio.Parsers.ElasticQueries;
using Nest;

var client = new ElasticClient();

var parser = new ElasticQueryParser(c => c
    .SetLoggerFactory(loggerFactory)
    .UseMappings(client, "my-index"));

// Build a query
QueryContainer query = await parser.BuildQueryAsync("status:active AND created:>2024-01-01");

// Use in a search
var response = await client.SearchAsync<MyDocument>(s => s
    .Index("my-index")
    .Query(_ => query));
```

### Building Aggregations

```csharp
var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// Build aggregations from expression
AggregationContainer aggs = await parser.BuildAggregationsAsync(
    "terms:(status min:amount max:amount avg:amount)");

var response = await client.SearchAsync<MyDocument>(s => s
    .Index("my-index")
    .Size(0)
    .Aggregations(aggs));
```

### Building Sort

```csharp
var parser = new ElasticQueryParser(c => c.UseMappings(client, "my-index"));

// Build sort from expression (- for descending, + for ascending)
var sort = await parser.BuildSortAsync("-created +name");

var response = await client.SearchAsync<MyDocument>(s => s
    .Index("my-index")
    .Sort(sort));
```

## SQL/Entity Framework Core Integration

The `SqlQueryParser` generates Dynamic LINQ expressions for EF Core:

```csharp
using Foundatio.Parsers.SqlQueries;
using Microsoft.EntityFrameworkCore;

var parser = new SqlQueryParser(c => c
    .SetDefaultFields(["Name", "Description"]));

// Get context from your DbContext
await using var db = new MyDbContext();
var context = parser.GetContext(db.Products.EntityType);

// Convert query to Dynamic LINQ
string dynamicLinq = await parser.ToDynamicLinqAsync(
    "status:active AND price:>100", context);

// Use with EF Core
var results = await db.Products
    .Where(parser.ParsingConfig, dynamicLinq)
    .ToListAsync();
```

## Field Aliases

Map user-friendly field names to actual field paths:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.identity" },
        { "created", "metadata.createdAt" }
    }));

// Query uses aliases
var query = await parser.BuildQueryAsync("user:john AND created:>2024-01-01");
// Internally resolves to: data.user.identity:john AND metadata.createdAt:>2024-01-01
```

## Query Validation

Validate queries before execution:

```csharp
var parser = new ElasticQueryParser(c => c
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "status", "name", "created" },
        AllowLeadingWildcards = false,
        AllowedMaxNodeDepth = 5
    }));

var result = await parser.ValidateQueryAsync("status:active");

if (!result.IsValid)
{
    Console.WriteLine($"Invalid query: {result.Message}");
    foreach (var error in result.ValidationErrors)
    {
        Console.WriteLine($"  - {error.Message} at position {error.Index}");
    }
}
```

## Query Includes

Define reusable query macros:

```csharp
var includes = new Dictionary<string, string> {
    { "active", "status:active AND deleted:false" },
    { "recent", "created:>now-7d" }
};

var parser = new ElasticQueryParser(c => c
    .UseIncludes(includes));

// Expands @include:active to (status:active AND deleted:false)
var query = await parser.BuildQueryAsync("@include:active AND @include:recent");
```

## Next Steps

- [Query Syntax](./query-syntax) - Complete query syntax reference
- [Aggregation Syntax](./aggregation-syntax) - Aggregation expression reference
- [Field Aliases](./field-aliases) - Advanced field mapping
- [Validation](./validation) - Query validation options
- [Visitors](./visitors) - Understanding the visitor pattern

## LLM-Friendly Documentation

For AI assistants and Large Language Models, we provide optimized documentation formats:

- [LLMs Index](/llms.txt) - Quick reference with links to all sections
- [Complete Documentation](/llms-full.txt) - All docs in one LLM-friendly file

These files follow the [llmstxt.org](https://llmstxt.org/) standard.
