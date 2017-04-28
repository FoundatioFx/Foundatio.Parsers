# Foundatio.Parsers
[![Build status](https://ci.appveyor.com/api/projects/status/05lsubm8sjvpoenx/branch/master?svg=true)](https://ci.appveyor.com/project/Exceptionless/foundatio-parsers/branch/master)
[![NuGet Version](http://img.shields.io/nuget/v/Foundatio.Parsers.LuceneQueries.svg?style=flat)](https://www.nuget.org/packages/Foundatio.Parsers.LuceneQueries/) 
[![Slack Status](https://slack.exceptionless.com/badge.svg)](https://slack.exceptionless.com)

A lucene style query parser that is extensible and allows additional syntax features. Also includes an Elasticsearch query_string query replacement that greatly enhances its capabilities for dynamic queries.

## Getting Started (Development)

[This package](https://www.nuget.org/packages/Foundatio.Parsers.LuceneQueries/) can be installed via the [NuGet package manager](https://docs.nuget.org/consume/Package-Manager-Dialog). If you need help, please contact us via in-app support or [open an issue](https://github.com/exceptionless/Foundatio.Parsers/issues/new). We’re always here to help if you have any questions!

1. You will need to have [Visual Studio 2015](http://www.visualstudio.com/products/visual-studio-community-vs) installed.
2. Open the `Foundatio.Parsers.sln` Visual Studio solution file.

## Using LuceneQueryParser

Below is a small sampling of the things you can accomplish with LuceneQueryParser, so check it out! We use this library in [Exceptionless](https://github.com/exceptionless/Exceptionless) to [ensure the query is valid before executing it, check to see if you are trying to a basic or premium search query and much more](https://github.com/exceptionless/Exceptionless/blob/master/Source/Core/Filter/QueryProcessorVisitor.cs)!

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

## Features
- Lucene Query Syntax Parser
  - Visitors for extensibility
- Field Aliases
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
