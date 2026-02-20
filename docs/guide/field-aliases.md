# Field Aliases

Field aliases allow you to map user-friendly field names to actual field paths in your data. This is useful for:

- Hiding complex nested field paths from users
- Providing backward compatibility when field names change
- Creating domain-specific query languages
- Abstracting internal data structures

## Static Field Maps

The simplest approach is a static dictionary mapping aliases to actual field names:

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("user:john");

// Define field mappings
var fieldMap = new FieldMap {
    { "user", "data.user.identity" },
    { "created", "metadata.createdAt" },
    { "status", "workflow.currentStatus" }
};

// Apply field resolution
var resolved = await FieldResolverQueryVisitor.RunAsync(result, fieldMap);

// Result: data.user.identity:john
Console.WriteLine(resolved.ToString());
```

### With ElasticQueryParser

```csharp
using Foundatio.Parsers.ElasticQueries;

var parser = new ElasticQueryParser(c => c
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.identity" },
        { "created", "metadata.createdAt" },
        { "status", "workflow.currentStatus" }
    }));

// Query uses aliases
var query = await parser.BuildQueryAsync("user:john AND status:active");
// Internally resolves to: data.user.identity:john AND workflow.currentStatus:active
```

## Hierarchical Field Resolution

For nested field structures, use hierarchical resolution. This resolves parent paths and preserves child segments:

```csharp
using Foundatio.Parsers.LuceneQueries.Visitors;

var fieldMap = new Dictionary<string, string> {
    { "user", "data.profile.user" },
    { "user.address", "data.profile.user.location" }
};

// Convert to hierarchical resolver
var resolver = fieldMap.ToHierarchicalFieldResolver();

// Resolution examples:
// "user" -> "data.profile.user"
// "user.name" -> "data.profile.user.name" (parent resolved, child preserved)
// "user.address" -> "data.profile.user.location" (exact match)
// "user.address.city" -> "data.profile.user.location.city" (parent resolved)
```

### How Hierarchical Resolution Works

1. Check for exact match in the map
2. If not found, split the field by `.` and check for parent matches
3. Replace the matched parent with its mapping, preserve remaining segments

```csharp
var map = new Dictionary<string, string> {
    { "original", "replacement" },
    { "original.nested", "otherreplacement" }
};

var resolver = map.ToHierarchicalFieldResolver();

// Examples:
await resolver("notmapped", null);           // "notmapped" (no change)
await resolver("original", null);            // "replacement"
await resolver("original.hey", null);        // "replacement.hey"
await resolver("original.nested", null);     // "otherreplacement"
await resolver("original.nested.hey", null); // "otherreplacement.hey"
```

## Dynamic Field Resolvers

For complex resolution logic, use a custom resolver function:

```csharp
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new ElasticQueryParser(c => c
    .UseFieldResolver(async (field, context) => {
        // Custom resolution logic
        if (field.StartsWith("custom."))
        {
            return field.Replace("custom.", "data.custom_fields.");
        }
        
        // Check a database or external service
        var mapping = await GetFieldMappingFromDatabase(field);
        if (mapping != null)
            return mapping;
        
        // Return null to keep original field name
        return null;
    }));
```

### Resolver Signature

```csharp
public delegate Task<string> QueryFieldResolver(string field, IQueryVisitorContext context);
```

The resolver receives:
- `field` - The field name to resolve
- `context` - The visitor context with additional data

Return:
- The resolved field name, or
- `null` to keep the original field name

## Combining Static and Dynamic Resolution

You can combine static maps with dynamic resolution:

```csharp
var staticMap = new Dictionary<string, string> {
    { "user", "data.user.identity" },
    { "status", "workflow.status" }
};

var parser = new ElasticQueryParser(c => c
    .UseFieldResolver(async (field, context) => {
        // First check static map
        if (staticMap.TryGetValue(field, out var mapped))
            return mapped;
        
        // Then apply dynamic logic
        if (field.StartsWith("meta."))
            return field.Replace("meta.", "metadata.");
        
        // Check hierarchical resolution for nested fields
        var hierarchical = staticMap.ToHierarchicalFieldResolver();
        return await hierarchical(field, context);
    }));
```

## Field Resolution in Groups

Field aliases work with grouped queries:

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("(user.name:john OR user.email:john@example.com) status:active");

var fieldMap = new FieldMap {
    { "user.name", "profile.fullName" },
    { "user.email", "profile.emailAddress" },
    { "status", "account.status" }
};

var resolved = await FieldResolverQueryVisitor.RunAsync(result, fieldMap);
// Result: (profile.fullName:john OR profile.emailAddress:john@example.com) account.status:active
```

## Field Resolution with Aggregations

Field aliases apply to aggregations as well:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseFieldMap(new Dictionary<string, string> {
        { "category", "product.category.keyword" },
        { "price", "product.pricing.amount" }
    }));

// Aggregation uses aliases
var aggs = await parser.BuildAggregationsAsync("terms:(category min:price max:price)");
// Resolves to: terms:(product.category.keyword min:product.pricing.amount max:product.pricing.amount)
```

## Preserving Original Field Names

The original field name is preserved in node metadata for reference:

```csharp
var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("user:john");

var fieldMap = new FieldMap { { "user", "data.user.identity" } };
var context = new QueryVisitorContext();

await FieldResolverQueryVisitor.RunAsync(result, fieldMap, context);

// Access original field name
var groupNode = result as GroupNode;
var termNode = groupNode?.Left as TermNode;
string original = termNode.GetOriginalField(); // "user"
string resolved = termNode.Field;               // "data.user.identity"
```

## Validation with Field Aliases

When using validation, specify allowed fields using the alias names (not resolved names):

```csharp
var parser = new ElasticQueryParser(c => c
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "data.user.identity" },
        { "status", "workflow.status" }
    })
    .SetValidationOptions(new QueryValidationOptions {
        // Use alias names in allowed fields
        AllowedFields = { "user", "status", "created" }
    }));

// Valid - uses allowed alias
var result = await parser.ValidateQueryAsync("user:john");
// result.IsValid == true

// Invalid - uses resolved name directly
result = await parser.ValidateQueryAsync("data.user.identity:john");
// result.IsValid == false (unless also in AllowedFields)
```

## Best Practices

### 1. Use Descriptive Alias Names

```csharp
// Good - clear, domain-specific names
var fieldMap = new Dictionary<string, string> {
    { "author", "document.metadata.author.name" },
    { "published", "document.metadata.publishedDate" }
};

// Avoid - cryptic abbreviations
var fieldMap = new Dictionary<string, string> {
    { "a", "document.metadata.author.name" },
    { "pd", "document.metadata.publishedDate" }
};
```

### 2. Document Your Aliases

Maintain documentation of available aliases for API consumers:

```csharp
/// <summary>
/// Available field aliases for the search API:
/// - user: Maps to data.user.identity
/// - status: Maps to workflow.currentStatus
/// - created: Maps to metadata.createdAt
/// </summary>
```

### 3. Handle Unknown Fields Gracefully

```csharp
var parser = new ElasticQueryParser(c => c
    .UseFieldResolver(async (field, context) => {
        if (knownAliases.TryGetValue(field, out var mapped))
            return mapped;
        
        // Option 1: Return null to keep original (permissive)
        return null;
        
        // Option 2: Use validation to reject unknown fields (strict)
        // Configure AllowedFields in validation options
    })
    .SetValidationOptions(new QueryValidationOptions {
        AllowUnresolvedFields = false // Reject unknown fields
    }));
```

### 4. Consider Case Sensitivity

```csharp
var parser = new ElasticQueryParser(c => c
    .UseFieldResolver(async (field, context) => {
        // Case-insensitive lookup
        var key = fieldMap.Keys.FirstOrDefault(k => 
            k.Equals(field, StringComparison.OrdinalIgnoreCase));
        
        return key != null ? fieldMap[key] : null;
    }));
```

## Next Steps

- [Query Includes](./query-includes) - Reusable query macros
- [Validation](./validation) - Validate and restrict queries
- [Elasticsearch Integration](./elastic-query-parser) - Full parser configuration
