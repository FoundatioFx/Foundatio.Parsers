# Validation

Foundatio.Parsers provides comprehensive query validation to ensure queries are safe, well-formed, and within allowed boundaries. This is essential when accepting queries from untrusted sources like user input or APIs.

## Basic Validation

### Syntax Validation

Validate query syntax without executing:

```csharp
using Foundatio.Parsers.LuceneQueries;

var result = await QueryValidator.ValidateQueryAsync("status:active AND created:>2024-01-01");

if (result.IsValid)
{
    Console.WriteLine("Query is valid");
}
else
{
    Console.WriteLine($"Invalid: {result.Message}");
    foreach (var error in result.ValidationErrors)
    {
        Console.WriteLine($"  Position {error.Index}: {error.Message}");
    }
}
```

### Throw on Invalid

Use `ValidateQueryAndThrowAsync` to throw an exception for invalid queries:

```csharp
try
{
    await QueryValidator.ValidateQueryAndThrowAsync("invalid::");
}
catch (QueryValidationException ex)
{
    Console.WriteLine($"Validation failed: {ex.Message}");
    Console.WriteLine($"Errors: {ex.Result.ValidationErrors.Count}");
}
```

## Validation Options

Configure validation behavior with `QueryValidationOptions`:

```csharp
var options = new QueryValidationOptions
{
    AllowedFields = { "status", "name", "created" },
    RestrictedFields = { "password", "secret" },
    AllowLeadingWildcards = false,
    AllowUnresolvedFields = false,
    AllowUnresolvedIncludes = false,
    AllowedMaxNodeDepth = 10,
    AllowedOperations = { "terms", "date", "min", "max" },
    RestrictedOperations = { "tophits" }
};

var result = await QueryValidator.ValidateQueryAsync(query, options);
```

### Option Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `AllowedFields` | `HashSet<string>` | Empty (all allowed) | Whitelist of allowed field names |
| `RestrictedFields` | `HashSet<string>` | Empty | Blacklist of restricted field names |
| `AllowLeadingWildcards` | `bool` | `true` | Allow `*value` wildcards |
| `AllowUnresolvedFields` | `bool` | `true` | Allow fields not in mapping |
| `AllowUnresolvedIncludes` | `bool` | `false` | Allow includes that don't resolve |
| `AllowedMaxNodeDepth` | `int` | `0` (unlimited) | Maximum query nesting depth |
| `AllowedOperations` | `HashSet<string>` | Empty (all allowed) | Whitelist of aggregation operations |
| `RestrictedOperations` | `HashSet<string>` | Empty | Blacklist of aggregation operations |
| `ShouldThrow` | `bool` | `false` | Throw exception on validation failure |

## Field Restrictions

### Allowed Fields (Whitelist)

Only allow specific fields:

```csharp
var options = new QueryValidationOptions();
options.AllowedFields.Add("status");
options.AllowedFields.Add("name");
options.AllowedFields.Add("created");

// Valid
var result = await QueryValidator.ValidateQueryAsync("status:active", options);
// result.IsValid == true

// Invalid - field not in whitelist
result = await QueryValidator.ValidateQueryAsync("password:secret", options);
// result.IsValid == false
```

### Restricted Fields (Blacklist)

Block specific fields:

```csharp
var options = new QueryValidationOptions();
options.RestrictedFields.Add("password");
options.RestrictedFields.Add("apiKey");
options.RestrictedFields.Add("secret");

// Valid
var result = await QueryValidator.ValidateQueryAsync("status:active", options);
// result.IsValid == true

// Invalid - field is restricted
result = await QueryValidator.ValidateQueryAsync("password:test", options);
// result.IsValid == false
```

### Field Aliases and Validation

When using field aliases, validation uses the alias names (not resolved names):

```csharp
var parser = new ElasticQueryParser(c => c
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.identity" }
    })
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "user", "status" }  // Use alias names
    }));

// Valid - uses allowed alias
var query = await parser.BuildQueryAsync("user:john");

// Invalid - uses resolved name directly
// (unless "data.user.identity" is also in AllowedFields)
await Assert.ThrowsAsync<QueryValidationException>(() =>
    parser.BuildQueryAsync("data.user.identity:john"));
```

## Wildcard Restrictions

### Disable Leading Wildcards

Leading wildcards (`*value`) can be expensive. Disable them:

```csharp
var options = new QueryValidationOptions
{
    AllowLeadingWildcards = false
};

// Valid - trailing wildcard
var result = await QueryValidator.ValidateQueryAsync("name:john*", options);
// result.IsValid == true

// Invalid - leading wildcard
result = await QueryValidator.ValidateQueryAsync("name:*smith", options);
// result.IsValid == false
// result.Message contains "wildcard"
```

## Depth Restrictions

Limit query nesting to prevent complex queries:

```csharp
var options = new QueryValidationOptions
{
    AllowedMaxNodeDepth = 5
};

// Valid - shallow nesting
var result = await QueryValidator.ValidateQueryAsync("(a:1 AND b:2)", options);
// result.IsValid == true

// Invalid - too deep
result = await QueryValidator.ValidateQueryAsync(
    "((((((a:1 AND b:2))))))", options);
// result.IsValid == false
```

## Unresolved Field Handling

Control behavior when fields don't exist in mappings:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        AllowUnresolvedFields = false
    }));

// If "nonexistent" is not in the index mapping
var result = await parser.ValidateQueryAsync("nonexistent:value");
// result.IsValid == false
// result.UnresolvedFields contains "nonexistent"
```

## Aggregation Validation

### Allowed Operations

Whitelist specific aggregation operations:

```csharp
var options = new QueryValidationOptions();
options.AllowedOperations.Add("terms");
options.AllowedOperations.Add("date");
options.AllowedOperations.Add("min");
options.AllowedOperations.Add("max");

var result = await QueryValidator.ValidateAggregationsAsync(
    "terms:(status min:amount)", options);
// result.IsValid == true

result = await QueryValidator.ValidateAggregationsAsync(
    "tophits:_", options);
// result.IsValid == false - tophits not in allowed list
```

### Restricted Operations

Blacklist specific operations:

```csharp
var options = new QueryValidationOptions();
options.RestrictedOperations.Add("tophits");  // Expensive operation

var result = await QueryValidator.ValidateAggregationsAsync(
    "terms:(status tophits:_)", options);
// result.IsValid == false
```

## Validation Result

The `QueryValidationResult` provides detailed information:

```csharp
var result = await QueryValidator.ValidateQueryAsync(query, options);

// Basic status
bool isValid = result.IsValid;
string message = result.Message;

// Detailed errors
foreach (var error in result.ValidationErrors)
{
    Console.WriteLine($"Error at position {error.Index}: {error.Message}");
}

// Referenced fields
foreach (var field in result.ReferencedFields)
{
    Console.WriteLine($"Field used: {field}");
}

// Unresolved fields (not found in mapping)
foreach (var field in result.UnresolvedFields)
{
    Console.WriteLine($"Unknown field: {field}");
}

// Referenced includes
foreach (var include in result.ReferencedIncludes)
{
    Console.WriteLine($"Include used: {include}");
}

// Unresolved includes
foreach (var include in result.UnresolvedIncludes)
{
    Console.WriteLine($"Missing include: {include}");
}

// Query depth
int depth = result.MaxNodeDepth;

// Operations used (for aggregations)
foreach (var op in result.Operations)
{
    Console.WriteLine($"Operation: {op.Key}, Count: {op.Value}");
}
```

## With ElasticQueryParser

### Validate Before Building

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "status", "name", "created" },
        AllowLeadingWildcards = false,
        AllowedMaxNodeDepth = 10
    }));

// Validate query
var validation = await parser.ValidateQueryAsync("status:active");
if (!validation.IsValid)
{
    return BadRequest(validation.Message);
}

// Build query (also validates and throws if invalid)
var query = await parser.BuildQueryAsync("status:active");
```

### Validate Aggregations

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        AllowedOperations = { "terms", "date", "min", "max", "avg" }
    }));

var validation = await parser.ValidateAggregationsAsync(
    "terms:(status min:amount)");

if (!validation.IsValid)
{
    return BadRequest(validation.Message);
}
```

### Validate Sort

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "created", "name", "status" }
    }));

var validation = await parser.ValidateSortAsync("-created +name");

if (!validation.IsValid)
{
    return BadRequest(validation.Message);
}
```

## Context-Based Validation

Access validation results from the context:

```csharp
var parser = new ElasticQueryParser(c => c
    .SetValidationOptions(options));

var context = new ElasticQueryVisitorContext();
var query = await parser.BuildQueryAsync("status:active", context);

// Get validation result from context
var validation = context.GetValidationResult();

// Check validity
if (!context.IsValid())
{
    var errors = context.GetValidationErrors();
    var message = context.GetValidationMessage();
}

// Throw if invalid
context.ThrowIfInvalid();
```

## Custom Validation

Add custom validation errors:

```csharp
var context = new ElasticQueryVisitorContext();

// Add custom validation error
context.AddValidationError("Custom error message", index: 5);

// Check result
var validation = context.GetValidationResult();
// validation.IsValid == false
```

## Best Practices

### 1. Always Validate User Input

```csharp
public async Task<IActionResult> Search([FromQuery] string query)
{
    var validation = await parser.ValidateQueryAsync(query);
    if (!validation.IsValid)
    {
        return BadRequest(new {
            error = "Invalid query",
            details = validation.Message,
            errors = validation.ValidationErrors
        });
    }
    
    // Safe to execute
    var results = await ExecuteSearch(query);
    return Ok(results);
}
```

### 2. Use Whitelists Over Blacklists

```csharp
// Preferred - explicit whitelist
var options = new QueryValidationOptions {
    AllowedFields = { "status", "name", "created", "category" }
};

// Less secure - blacklist can miss fields
var options = new QueryValidationOptions {
    RestrictedFields = { "password" }  // What about "apiKey"?
};
```

### 3. Limit Query Complexity

```csharp
var options = new QueryValidationOptions {
    AllowedMaxNodeDepth = 10,
    AllowLeadingWildcards = false
};
```

### 4. Log Validation Failures

```csharp
var validation = await parser.ValidateQueryAsync(query);
if (!validation.IsValid)
{
    logger.LogWarning(
        "Invalid query from user {UserId}: {Query}. Errors: {Errors}",
        userId, query, validation.Message);
    
    return BadRequest(validation.Message);
}
```

### 5. Provide Helpful Error Messages

```csharp
var validation = await parser.ValidateQueryAsync(query);
if (!validation.IsValid)
{
    var response = new {
        error = "Query validation failed",
        message = validation.Message,
        allowedFields = options.AllowedFields,
        errors = validation.ValidationErrors.Select(e => new {
            position = e.Index,
            message = e.Message
        })
    };
    
    return BadRequest(response);
}
```

## Next Steps

- [Field Aliases](./field-aliases) - Map field names
- [Query Includes](./query-includes) - Reusable query macros
- [Visitors](./visitors) - Custom query transformations
