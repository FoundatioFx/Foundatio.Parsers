# Troubleshooting

This guide covers common issues and their solutions when using Foundatio.Parsers.

## Parse Errors

### Unexpected Character

**Error:** `Unexpected character at position X`

**Cause:** The query contains invalid syntax.

**Solution:**

```csharp
try
{
    var result = await parser.ParseAsync(query);
}
catch (FormatException ex)
{
    // Get position information
    var cursor = ex.Data["cursor"] as Cursor;
    if (cursor != null)
    {
        Console.WriteLine($"Error at line {cursor.Line}, column {cursor.Column}");
    }
    Console.WriteLine($"Message: {ex.Message}");
}
```

**Common causes:**
- Unbalanced parentheses: `(status:active`
- Invalid range syntax: `field:[1 TO]`
- Unescaped special characters: `url:https://example.com`

**Fix:** Escape special characters:

```csharp
// Escape colons in values
"url:https\\://example.com"

// Or use quotes
"url:\"https://example.com\""
```

### Empty Query

**Error:** Query returns no results or empty AST.

**Solution:** Check for empty or whitespace-only queries:

```csharp
if (string.IsNullOrWhiteSpace(query))
{
    // Handle empty query
    return new MatchAllQuery();
}

var result = await parser.ParseAsync(query);
```

## Validation Errors

### Field Not Allowed

**Error:** `Field 'X' is not allowed`

**Cause:** Field is not in the `AllowedFields` list.

**Solution:**

```csharp
var options = new QueryValidationOptions();
options.AllowedFields.Add("status");
options.AllowedFields.Add("name");
options.AllowedFields.Add("created");
// Add all fields users should be able to query
```

### Field Restricted

**Error:** `Field 'X' is restricted`

**Cause:** Field is in the `RestrictedFields` list.

**Solution:** Remove from restricted list or use a different field:

```csharp
var options = new QueryValidationOptions();
options.RestrictedFields.Remove("fieldname");
```

### Leading Wildcard Not Allowed

**Error:** `Leading wildcards are not allowed`

**Cause:** Query contains `*value` and `AllowLeadingWildcards` is false.

**Solution:**

```csharp
// Option 1: Allow leading wildcards
var options = new QueryValidationOptions {
    AllowLeadingWildcards = true
};

// Option 2: Rewrite query to use trailing wildcard
// Instead of: *smith
// Use: smith* (if appropriate for your use case)
```

### Query Too Deep

**Error:** `Query exceeds maximum depth of X`

**Cause:** Query nesting exceeds `AllowedMaxNodeDepth`.

**Solution:**

```csharp
var options = new QueryValidationOptions {
    AllowedMaxNodeDepth = 15  // Increase limit
};
```

### Unresolved Field

**Error:** `Field 'X' could not be resolved`

**Cause:** Field doesn't exist in Elasticsearch mapping and `AllowUnresolvedFields` is false.

**Solution:**

```csharp
// Option 1: Allow unresolved fields
var options = new QueryValidationOptions {
    AllowUnresolvedFields = true
};

// Option 2: Add field alias
var parser = new ElasticQueryParser(c => c
    .UseFieldMap(new Dictionary<string, string> {
        { "user_field", "actual.field.path" }
    }));

// Option 3: Refresh mappings (if recently added field, wait for auto-refresh or force it)
parser.Configuration.MappingResolver.RefreshMapping();
```

## Elasticsearch Issues

### Query Returns No Results

**Possible causes:**

1. **Analyzed vs keyword field:**

```csharp
// Check if field is analyzed
var resolver = parser.Configuration.MappingResolver;
bool isAnalyzed = resolver.IsPropertyAnalyzed("title");

// For exact matches on analyzed fields, use .keyword
"title.keyword:\"Exact Title\""
```

2. **Case sensitivity:**

```csharp
// Elasticsearch is case-sensitive by default
// Use lowercase or configure analyzer
"status:Active"  // May not match "active"
```

3. **Filter vs query context:**

```csharp
// By default, queries are wrapped in filter (no scoring)
// For full-text search, enable scoring
var context = new ElasticQueryVisitorContext { UseScoring = true };
var query = await parser.BuildQueryAsync("search terms", context);
```

### Nested Query Not Working

**Cause:** Nested support not enabled or field not detected as nested.

**Solution:**

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNested());  // Enable nested support

// Verify field is nested
var resolver = parser.Configuration.MappingResolver;
bool isNested = resolver.IsNestedPropertyType("comments");
```

### Geo Query Fails

**Cause:** Geo resolver not configured or returns invalid coordinates.

**Solution:**

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseGeo(location => {
        // Ensure valid coordinates are returned
        Console.WriteLine($"Resolving: {location}");
        var coords = ResolveLocation(location);
        Console.WriteLine($"Resolved to: {coords}");
        return coords;
    }));
```

### Aggregation Field Error

**Error:** `Fielddata is disabled on text fields by default`

**Cause:** Trying to aggregate on an analyzed text field.

**Solution:**

```csharp
// The parser should automatically use .keyword sub-field
// If not, check your mapping has keyword sub-field:
// "title": { "type": "text", "fields": { "keyword": { "type": "keyword" } } }

// Or use field alias
var parser = new ElasticQueryParser(c => c
    .UseFieldMap(new Dictionary<string, string> {
        { "title", "title.keyword" }
    }));
```

## SQL/EF Core Issues

### Dynamic LINQ Parse Error

**Error:** `No property or field 'X' exists in type 'Y'`

**Cause:** Field name doesn't match entity property.

**Solution:**

```csharp
// Check available fields
var context = parser.GetContext(db.Products.EntityType);
foreach (var field in context.Fields)
{
    Console.WriteLine($"{field.FullName} ({field.GetType().Name})");
}

// Use correct casing (EF Core is case-sensitive)
"Status:active"  // Not "status:active"
```

### Navigation Property Error

**Error:** Query on navigation property fails.

**Solution:**

```csharp
// Increase navigation depth
var parser = new SqlQueryParser(c => c
    .SetFieldDepth(3));

// Check if navigation is included
var context = parser.GetContext(db.Products.EntityType);
var navFields = context.Fields.Where(f => f.IsNavigation);
```

### Full-Text Search Not Working

**Cause:** Full-text catalog not configured or fields not indexed.

**Solution:**

```csharp
// Ensure full-text catalog exists
// CREATE FULLTEXT CATALOG ftCatalog AS DEFAULT;

// Ensure full-text index exists on table
// CREATE FULLTEXT INDEX ON Products(Name, Description) KEY INDEX PK_Products;

// Configure parser
var parser = new SqlQueryParser(c => c
    .SetFullTextFields(new[] { "Name", "Description" }));
```

## Include Issues

### Include Not Expanding

**Cause:** Include name not found in resolver.

**Solution:**

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(async name => {
        var include = await GetInclude(name);
        if (include == null)
        {
            Console.WriteLine($"Include not found: {name}");
        }
        return include;
    }));
```

### Recursive Include Detected

**Error:** `Recursive include detected: X`

**Cause:** Include references itself directly or indirectly.

**Solution:**

```csharp
// Check your include definitions for cycles
var includes = new Dictionary<string, string> {
    { "a", "@include:b" },
    { "b", "@include:a" }  // Cycle!
};

// Fix by removing the cycle
var includes = new Dictionary<string, string> {
    { "a", "@include:b" },
    { "b", "status:active" }  // No cycle
};
```

## Performance Issues

### Slow Query Parsing

**Cause:** Complex queries or many visitors.

**Solution:**

```csharp
// Reuse parser instance
private readonly ElasticQueryParser _parser;

public MyService()
{
    _parser = new ElasticQueryParser(c => c
        .UseMappings(client, "my-index"));
}

// Don't create new parser for each request
```

### Slow Mapping Resolution

**Cause:** Mapping fetched from Elasticsearch on every query.

**Solution:**

```csharp
// Mappings are cached by default (auto-refresh at most once per minute)
// Manual refresh is typically only needed in unit tests:
parser.Configuration.MappingResolver.RefreshMapping();

// For production, create resolver once and share
var resolver = ElasticMappingResolver.Create(client, "my-index");
var parser1 = new ElasticQueryParser(c => c.UseMappings(resolver));
var parser2 = new ElasticQueryParser(c => c.UseMappings(resolver));
```

## Debugging

### Enable Logging

```csharp
var parser = new ElasticQueryParser(c => c
    .SetLoggerFactory(loggerFactory));

// Or for LuceneQueryParser with tracing
var parser = new LuceneQueryParser {
    Tracer = new LoggingTracer(logger)
};
```

### Inspect AST

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync(query);

// Print AST structure
string debug = DebugQueryVisitor.Run(result);
Console.WriteLine(debug);
```

### Inspect Generated Query

```csharp
// Regenerate query string from AST
string regenerated = GenerateQueryVisitor.Run(result);
Console.WriteLine($"Parsed as: {regenerated}");
```

### Inspect Elasticsearch Request

```csharp
var response = await client.SearchAsync<MyDoc>(s => s
    .Index("my-index")
    .Query(_ => query));

// Get the actual request sent
string request = response.GetRequest();
Console.WriteLine(request);

// Check for errors
if (!response.IsValid)
{
    Console.WriteLine(response.GetErrorMessage());
}
```

## Getting Help

If you're still having issues:

1. **Check the tests** - The test files contain many examples:
   - `tests/Foundatio.Parsers.LuceneQueries.Tests/`
   - `tests/Foundatio.Parsers.ElasticQueries.Tests/`
   - `tests/Foundatio.Parsers.SqlQueries.Tests/`

2. **Enable detailed logging** - Use `LogLevel.Trace` for maximum detail

3. **Open an issue** - [GitHub Issues](https://github.com/FoundatioFx/Foundatio.Parsers/issues)

4. **Join Discord** - [Foundatio Discord](https://discord.gg/6HxgFCx)
