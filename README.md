![Foundatio](https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio-dark-bg.svg#gh-dark-mode-only "Foundatio")![Foundatio](https://raw.githubusercontent.com/FoundatioFx/Foundatio/master/media/foundatio.svg#gh-light-mode-only "Foundatio")

[![Build status](https://github.com/FoundatioFx/Foundatio.Parsers/workflows/Build/badge.svg)](https://github.com/FoundatioFx/Foundatio.Parsers/actions)
[![NuGet Version](http://img.shields.io/nuget/v/Foundatio.Parsers.LuceneQueries.svg?style=flat)](https://www.nuget.org/packages/Foundatio.Parsers.LuceneQueries/)
[![feedz.io](https://img.shields.io/badge/endpoint.svg?url=https%3A%2F%2Ff.feedz.io%2Ffoundatio%2Ffoundatio%2Fshield%2FFoundatio.Parsers.LuceneQueries%2Flatest)](https://f.feedz.io/foundatio/foundatio/packages/Foundatio.Parsers.LuceneQueries/latest/download)
[![Discord](https://img.shields.io/discord/715744504891703319)](https://discord.gg/6HxgFCx)

A lucene style query parser that is extensible and allows additional syntax features. Also includes an Elasticsearch query_string query replacement that greatly enhances its capabilities for dynamic queries.

## Getting Started (Development)

[This package](https://www.nuget.org/packages/Foundatio.Parsers.LuceneQueries/) can be installed via the [NuGet package manager](https://docs.nuget.org/consume/Package-Manager-Dialog). If you need help, please contact us via in-app support or [open an issue](https://github.com/exceptionless/Foundatio.Parsers/issues/new). We’re always here to help if you have any questions!

1. You will need to have [Visual Studio Code](https://code.visualstudio.com) installed.
2. Open the `Foundatio.Parsers.sln` Visual Studio solution file.

## Using LuceneQueryParser

Below is a small sampling of the things you can accomplish with LuceneQueryParser, so check it out! We use this library extensively in [Exceptionless](https://github.com/exceptionless/Exceptionless)!

In the sample below we will parse a query and output it's structure using the `DebugQueryVisitor` and then generate the same exact query using the parse result.

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();
var result = parser.Parse("field:[1 TO 2]");
Debug.WriteLine(DebugQueryVisitor.Run(result));
```

Here is the parse result as shown from the `DebugQueryVisitor`
```
Group:
  Left - Term: 
      TermMax: 2
      TermMin: 1
      MinInclusive: True
      MaxInclusive: True
      Field: 
          Name: field
```

Finally, lets translate the parse result back into the original query.
```csharp
var generatedQuery = GenerateQueryVisitor.Run(result);
System.Diagnostics.Debug.Assert(query == generatedQuery);
```

## [Aggregation Syntax](docs/aggregations.md)

## Features
- Lucene Query Syntax Parser
  - Parsers fairly standardized syntax from [Lucene](https://lucene.apache.org/core/2_9_4/queryparsersyntax.html) and [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html).
  - Visitors for extensibility
- Field Aliases (static and dynamic)
- Query Includes
  - Define stored queries that can be included inside other queries as macros that will be expanded
- Validation
  - Validate query syntax
  - Restrict access to specific fields
  - Restrict the number of operations allowed
  - Restrict nesting depth
- Elasticsearch
  - Elastic query string query replacement on steriods
  - Dynamic search and filter expressions
  - Dynamic aggregation expressions
    - Supported bucket aggregations: terms, geo grid, date histogram, numeric histogram
      - Bucket aggregations allow nesting other dynamic aggregations inside
    - Supported metric aggregations: min, max, avg, sum, stats, extended stats, cardinality, missing, percentiles
  - Dynamic sort expressions
  - Dynamic expressions can be exposed to end users to allow for custom searches, filters, sorting and aggregations
    - Enables allowing users to build custom views, charts and dashboards
    - Enables powerful APIs that allow users to do things you never thought of
  - Supports geo queries (proximity and radius)
    - mygeo:75044~75mi
      - Returns all documents that have a value in the mygeo field that is within a 75 mile radius of the 75044 zip code
  - Supports nested document mappings
  - Automatically resolves non-analyzed keyword sub-fields for sorting and aggregations
  - Aliases can be defined right on your NEST mappings
    - Supports both root and inner field name aliases
    
## Thanks to all the people who have contributed

[![contributors](https://contributors-img.web.app/image?repo=FoundatioFx/Foundatio.Parsers)](https://github.com/FoundatioFx/Foundatio.Parsers/graphs/contributors)
