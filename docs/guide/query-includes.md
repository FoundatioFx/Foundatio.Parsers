# Query Includes

Query includes (also called macros) allow you to define reusable query fragments that expand inline. This is useful for:

- Storing commonly used filters
- Creating user-defined saved searches
- Building complex queries from simple building blocks
- Providing shortcuts for frequently used conditions

## Basic Usage

### Syntax

Include a stored query using the `@include:` prefix:

```
@include:name
```

### Simple Example

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("@include:active");

// Define includes
var includes = new Dictionary<string, string> {
    { "active", "status:active AND deleted:false" }
};

// Expand includes
var expanded = await IncludeVisitor.RunAsync(result, includes);

// Result: (status:active AND deleted:false)
Console.WriteLine(expanded.ToString());
```

## With ElasticQueryParser

```csharp
using Foundatio.Parsers.ElasticQueries;

var includes = new Dictionary<string, string> {
    { "active", "status:active AND deleted:false" },
    { "recent", "created:[now-7d TO now]" },
    { "highvalue", "amount:>=1000" }
};

var parser = new ElasticQueryParser(c => c
    .UseIncludes(includes));

// Single include
var query = await parser.BuildQueryAsync("@include:active");

// Multiple includes
query = await parser.BuildQueryAsync("@include:active AND @include:recent");

// Combine with other conditions
query = await parser.BuildQueryAsync("@include:active AND category:electronics");
```

## Dynamic Include Resolution

For includes stored in a database or external service:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(async (name) => {
        // Load from database
        var savedSearch = await db.SavedSearches
            .FirstOrDefaultAsync(s => s.Name == name);
        
        return savedSearch?.Query;
    }));

// Resolves include from database
var query = await parser.BuildQueryAsync("@include:my-saved-search");
```

### Async Include Resolver

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(async (name) => {
        // Call external API
        var response = await httpClient.GetAsync($"/api/includes/{name}");
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadAsStringAsync();
        }
        return null; // Include not found
    }));
```

## Nested Includes

Includes can reference other includes:

```csharp
var includes = new Dictionary<string, string> {
    { "active", "status:active AND deleted:false" },
    { "recent", "created:[now-7d TO now]" },
    { "active-recent", "@include:active AND @include:recent" }
};

var parser = new ElasticQueryParser(c => c.UseIncludes(includes));

// Expands to: ((status:active AND deleted:false) AND (created:[now-7d TO now]))
var query = await parser.BuildQueryAsync("@include:active-recent");
```

## Recursive Include Detection

The parser automatically detects and prevents infinite recursion:

```csharp
var includes = new Dictionary<string, string> {
    { "a", "@include:b" },
    { "b", "@include:a" }  // Circular reference!
};

var parser = new LuceneQueryParser();
var result = await parser.ParseAsync("@include:a");

var context = new QueryVisitorContext();
var expanded = await IncludeVisitor.RunAsync(result, includes, context);

// Check for errors
var validation = context.GetValidationResult();
if (!validation.IsValid)
{
    Console.WriteLine(validation.Message);
    // Output: "Recursive include detected: a"
}
```

## Skipping Includes

You can conditionally skip include expansion:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(
        includeResolver: name => includes.GetValueOrDefault(name),
        shouldSkipInclude: (node, context) => {
            // Skip includes starting with "admin_" for non-admin users
            if (node.Term.StartsWith("admin_") && !context.GetValue<bool>("IsAdmin"))
                return true;
            
            return false;
        }));

// Set context
var context = new ElasticQueryVisitorContext();
context.SetValue("IsAdmin", false);

// admin_reports include will be skipped
var query = await parser.BuildQueryAsync("@include:admin_reports", context);
```

## Tracking Referenced Includes

The validation result tracks which includes were referenced:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(includes)
    .SetValidationOptions(new QueryValidationOptions()));

var context = new ElasticQueryVisitorContext();
var query = await parser.BuildQueryAsync("@include:active AND @include:recent", context);

var validation = context.GetValidationResult();

// Get referenced includes
foreach (var include in validation.ReferencedIncludes)
{
    Console.WriteLine($"Used include: {include}");
}

// Get unresolved includes (not found)
foreach (var include in validation.UnresolvedIncludes)
{
    Console.WriteLine($"Missing include: {include}");
}
```

## Validation Options

Control include behavior with validation options:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(includes)
    .SetValidationOptions(new QueryValidationOptions {
        // Fail if an include cannot be resolved
        AllowUnresolvedIncludes = false
    }));

// Throws QueryValidationException if include not found
try
{
    var query = await parser.BuildQueryAsync("@include:nonexistent");
}
catch (QueryValidationException ex)
{
    Console.WriteLine(ex.Message);
    // Output: "Include 'nonexistent' could not be resolved"
}
```

## Combining with Field Aliases

Includes work with field aliases - the alias resolution happens after include expansion:

```csharp
var includes = new Dictionary<string, string> {
    { "active", "status:active" }  // Uses alias "status"
};

var fieldMap = new Dictionary<string, string> {
    { "status", "workflow.currentStatus" }
};

var parser = new ElasticQueryParser(c => c
    .UseIncludes(includes)
    .UseFieldMap(fieldMap));

// 1. Expands @include:active to (status:active)
// 2. Resolves status to workflow.currentStatus
var query = await parser.BuildQueryAsync("@include:active");
// Final: workflow.currentStatus:active
```

## Use Cases

### Saved Searches

Allow users to save and reuse searches:

```csharp
public class SavedSearchService
{
    private readonly ElasticQueryParser _parser;
    private readonly IRepository<SavedSearch> _repository;
    
    public SavedSearchService(IRepository<SavedSearch> repository)
    {
        _repository = repository;
        _parser = new ElasticQueryParser(c => c
            .UseIncludes(ResolveSavedSearch));
    }
    
    private async Task<string> ResolveSavedSearch(string name)
    {
        var search = await _repository.GetByNameAsync(name);
        return search?.Query;
    }
    
    public async Task<QueryContainer> BuildQuery(string userQuery)
    {
        // User can reference saved searches: @include:my-filter AND category:books
        return await _parser.BuildQueryAsync(userQuery);
    }
}
```

### Role-Based Filters

Automatically apply filters based on user role:

```csharp
var roleFilters = new Dictionary<string, string> {
    { "user-filter", "organization_id:{userId}" },
    { "admin-filter", "*" },  // No filter for admins
    { "manager-filter", "department_id:{departmentId}" }
};

var parser = new ElasticQueryParser(c => c
    .UseIncludes(async name => {
        var template = roleFilters.GetValueOrDefault(name);
        if (template == null) return null;
        
        // Replace placeholders with actual values
        return template
            .Replace("{userId}", currentUser.Id)
            .Replace("{departmentId}", currentUser.DepartmentId);
    }));
```

### Query Templates

Create parameterized query templates:

```csharp
var templates = new Dictionary<string, string> {
    { "date-range", "created:[{start} TO {end}]" },
    { "price-range", "price:[{min} TO {max}]" }
};

// Note: This requires custom expansion logic
var parser = new ElasticQueryParser(c => c
    .UseIncludes(name => {
        // Parse template name and parameters
        // e.g., "date-range:2024-01-01:2024-12-31"
        var parts = name.Split(':');
        if (parts.Length < 2) return templates.GetValueOrDefault(parts[0]);
        
        var template = templates.GetValueOrDefault(parts[0]);
        if (template == null) return null;
        
        // Simple parameter substitution
        for (int i = 1; i < parts.Length; i++)
        {
            template = template.Replace($"{{{i-1}}}", parts[i]);
        }
        return template;
    }));
```

## Best Practices

### 1. Use Descriptive Names

```csharp
// Good
var includes = new Dictionary<string, string> {
    { "active-users", "status:active AND type:user" },
    { "last-30-days", "created:[now-30d TO now]" }
};

// Avoid
var includes = new Dictionary<string, string> {
    { "q1", "status:active AND type:user" },
    { "d", "created:[now-30d TO now]" }
};
```

### 2. Document Available Includes

```csharp
/// <summary>
/// Available query includes:
/// - active: Active, non-deleted records
/// - recent: Created in the last 7 days
/// - high-priority: Priority >= 8
/// </summary>
```

### 3. Validate Include Content

```csharp
public async Task<bool> SaveInclude(string name, string query)
{
    // Validate the query before saving
    var parser = new LuceneQueryParser();
    try
    {
        var result = await parser.ParseAsync(query);
        // Additional validation...
        return true;
    }
    catch (FormatException)
    {
        return false;
    }
}
```

### 4. Handle Missing Includes Gracefully

```csharp
var parser = new ElasticQueryParser(c => c
    .UseIncludes(name => {
        var query = includes.GetValueOrDefault(name);
        if (query == null)
        {
            logger.LogWarning("Include not found: {Name}", name);
        }
        return query;
    }));
```

## Next Steps

- [Validation](./validation) - Validate queries and includes
- [Field Aliases](./field-aliases) - Map field names
- [Custom Visitors](./custom-visitors) - Create custom query transformations
