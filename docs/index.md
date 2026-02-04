---
layout: home

hero:
  name: Foundatio Parsers
  text: Extensible Query Parser
  tagline: Lucene-style query parsing with Elasticsearch and SQL support
  image:
    src: https://raw.githubusercontent.com/FoundatioFx/Foundatio/main/media/foundatio-icon.png
    alt: Foundatio Parsers
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/FoundatioFx/Foundatio.Parsers

features:
  - icon: 🔍
    title: Lucene Query Syntax
    details: Parse standardized Lucene-style queries with support for terms, ranges, boolean operators, and wildcards.
    link: /guide/query-syntax
  - icon: ⚡
    title: Elasticsearch Integration
    details: Enhanced query_string replacement that builds NEST QueryContainer, AggregationContainer, and sort expressions.
    link: /guide/elastic-query-parser
  - icon: 🗄️
    title: SQL/EF Core Integration
    details: Generate Dynamic LINQ queries from parsed expressions for Entity Framework Core applications.
    link: /guide/sql-query-parser
  - icon: 🏷️
    title: Field Aliases
    details: Map field names with static dictionaries or dynamic resolvers for flexible query translation.
    link: /guide/field-aliases
  - icon: 📦
    title: Query Includes
    details: Define reusable query macros that expand inline, with recursive include detection.
    link: /guide/query-includes
  - icon: ✅
    title: Validation System
    details: Validate syntax, restrict fields, limit operations, and control nesting depth.
    link: /guide/validation
  - icon: 🔧
    title: Visitor Pattern
    details: Extensible AST traversal with chainable visitors for custom query transformations.
    link: /guide/visitors
  - icon: 📊
    title: Aggregation Expressions
    details: Dynamic aggregation parsing for metrics, buckets, date histograms, and nested aggregations.
    link: /guide/aggregation-syntax
  - icon: 🌍
    title: Geo Queries
    details: Support for geo proximity and bounding box queries with location resolution.
    link: /guide/query-syntax#geo-proximity-queries
  - icon: 📁
    title: Nested Documents
    details: Automatic handling of Elasticsearch nested document queries.
    link: /guide/elastic-mappings
---

## Quick Example

Parse a query and inspect its structure:

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();
var result = parser.Parse("field:[1 TO 2]");

// Debug the AST structure
Console.WriteLine(DebugQueryVisitor.Run(result));

// Regenerate the query string
string query = GenerateQueryVisitor.Run(result);
// Output: "field:[1 TO 2]"
```

Build Elasticsearch queries:

```csharp
using Foundatio.Parsers.ElasticQueries;

var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.name" }
    }));

// Build a NEST QueryContainer
var query = await parser.BuildQueryAsync("user:john AND status:active");

// Build aggregations
var aggs = await parser.BuildAggregationsAsync("terms:(status min:created max:created)");

// Build sort
var sort = await parser.BuildSortAsync("-created +name");
```
