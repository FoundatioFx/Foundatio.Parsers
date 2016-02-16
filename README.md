# Exceptionless.LuceneQueryParser
[![Build status](https://ci.appveyor.com/api/projects/status/c92r9f5jhf6pl4hs/branch/master?svg=true)](https://ci.appveyor.com/project/Exceptionless/exceptionless-lucenequeryparser)
[![NuGet Version](http://img.shields.io/nuget/v/Exceptionless.LuceneQueryParser.svg?style=flat)](https://www.nuget.org/packages/Exceptionless.LuceneQueryParser/) 
[![NuGet Downloads](http://img.shields.io/nuget/dt/Exceptionless.LuceneQueryParser.svg?style=flat)](https://www.nuget.org/packages/Exceptionless.LuceneQueryParser/) 
[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/exceptionless/Discuss)
[![Donate](https://img.shields.io/badge/donorbox-donate-blue.svg)](https://donorbox.org/exceptionless) 

A lucene style query parser that is extensible and allows additional syntax features.

## Getting Started (Development)

[This package](https://www.nuget.org/packages/Exceptionless.LuceneQueryParser/) can be installed via the [NuGet package manager](https://docs.nuget.org/consume/Package-Manager-Dialog). If you need help, please contact us via in-app support or [open an issue](https://github.com/exceptionless/Exceptionless.LuceneQueryParser/issues/new). We’re always here to help if you have any questions!

1. You will need to have [Visual Studio 2013](http://www.visualstudio.com/products/visual-studio-community-vs) installed.
2. Open the `Exceptionless.LuceneQueryParser.sln` Visual Studio solution file.

## Using LuceneQueryParser

Below is a small sampling of the things you can accomplish with LuceneQueryParser, so check it out! We use this library in [Exceptionless](https://github.com/exceptionless/Exceptionless) to [ensure the query is valid before executing it, check to see if you are trying to a basic or premium search query and much more](https://github.com/exceptionless/Exceptionless/blob/master/Source/Core/Filter/QueryProcessorVisitor.cs)!

In the sample below we will parse a query and output it's structure using the `DebugQueryVisitor` and then generate the same exact query using the parse result.

```csharp
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Visitor;

var parser = new QueryParser();
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
