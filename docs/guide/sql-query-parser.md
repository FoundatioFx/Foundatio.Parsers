# SQL Integration

The `SqlQueryParser` extends the base Lucene parser to generate Dynamic LINQ expressions for Entity Framework Core. This enables powerful query capabilities for SQL databases.

## Installation

```bash
dotnet add package Foundatio.Parsers.SqlQueries
```

## Basic Usage

```csharp
using Foundatio.Parsers.SqlQueries;
using Microsoft.EntityFrameworkCore;

var parser = new SqlQueryParser(c => c
    .SetDefaultFields(new[] { "Name", "Description" }));

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

## Configuration

### Basic Configuration

```csharp
var parser = new SqlQueryParser(c => c
    // Logging
    .SetLoggerFactory(loggerFactory)
    
    // Default fields for unqualified search terms (operator is second param)
    .SetDefaultFields(new[] { "Name", "Description", "Tags" }, SqlSearchOperator.Contains)
    
    // Full-text search fields
    .SetFullTextFields(new[] { "Name", "Description" })
    
    // Field aliases
    .UseFieldMap(new Dictionary<string, string> {
        { "user", "CreatedBy.Name" }
    })
    
    // Query includes
    .UseIncludes(new Dictionary<string, string> {
        { "active", "Status == \"Active\"" }
    })
    
    // Validation
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "Name", "Status", "Price", "Created" }
    })
    
    // Navigation depth limit
    .SetFieldDepth(3));
```

### Configuration Methods

| Method | Description |
|--------|-------------|
| `SetLoggerFactory(factory)` | Set logging factory |
| `SetDefaultFields(fields, op)` | Default fields for unqualified terms; optional `SqlSearchOperator` (Equals, Contains, StartsWith) |
| `SetFullTextFields(fields)` | Fields using SQL full-text search |
| `SetSearchTokenizer(tokenizer)` | Custom search term tokenization |
| `SetDateTimeParser(parser)` | Custom DateTime parsing |
| `SetDateOnlyParser(parser)` | Custom DateOnly parsing |
| `SetFieldDepth(depth)` | Maximum navigation property depth |
| `UseFieldMap(map)` | Static field alias map |
| `UseFieldResolver(resolver)` | Dynamic field resolver |
| `UseIncludes(includes)` | Query include definitions |
| `SetValidationOptions(options)` | Validation configuration |

## Query Context

The `SqlQueryVisitorContext` provides entity metadata for query generation:

```csharp
// Get context from entity type
var context = parser.GetContext(db.Products.EntityType);

// Context contains:
// - Fields: List of EntityFieldInfo with type information
// - ValidationOptions: Automatically populated from entity properties
// - SearchTokenizer, DateTimeParser, etc. from configuration
```

### EntityFieldInfo

Each field has metadata:

```csharp
public class EntityFieldInfo
{
    public string Name { get; }           // Property name
    public string FullName { get; }       // Full path (e.g., "Category.Name")
    public bool IsNumber { get; }         // Numeric type
    public bool IsDate { get; }           // DateTime type
    public bool IsDateOnly { get; }       // DateOnly type
    public bool IsBoolean { get; }        // Boolean type
    public bool IsCollection { get; }     // Collection navigation
    public bool IsNavigation { get; }     // Navigation property
    public EntityFieldInfo Parent { get; } // Parent for nested fields
}
```

## Query Translation

### Term Queries

```csharp
// Exact match
"status:active"
// Generates: Status == "active"

// Quoted phrase
"name:\"John Smith\""
// Generates: Name == "John Smith"

// Wildcard (suffix)
"name:john*"
// Generates: Name.StartsWith("john")

// Wildcard (contains)
"name:*john*"
// Generates: Name.Contains("john")
```

### Range Queries

```csharp
// Inclusive range
"price:[100 TO 500]"
// Generates: (Price >= 100 AND Price <= 500)

// Exclusive range
"price:{100 TO 500}"
// Generates: (Price > 100 AND Price < 500)

// Comparison operators
"price:>100"
// Generates: Price > 100

"price:>=100"
// Generates: Price >= 100
```

### Boolean Operators

```csharp
// AND
"status:active AND price:>100"
// Generates: Status == "active" AND Price > 100

// OR
"status:active OR status:pending"
// Generates: Status == "active" OR Status == "pending"

// NOT
"NOT status:deleted"
// Generates: NOT (Status == "deleted")
```

### Existence Queries

```csharp
// Field exists (not null)
"_exists_:description"
// Generates: Description != null

// Field missing (null)
"_missing_:description"
// Generates: Description == null
```

## Default Field Search

When no field is specified, the query searches default fields:

```csharp
var parser = new SqlQueryParser(c => c
    .SetDefaultFields(new[] { "Name", "Description" }, SqlSearchOperator.Contains));

var context = parser.GetContext(db.Products.EntityType);

// Search term without field
string sql = await parser.ToDynamicLinqAsync("laptop", context);
// Generates: Name.Contains("laptop") OR Description.Contains("laptop")
```

### Search Operators

| Operator | Description | Generated SQL |
|----------|-------------|---------------|
| `Equals` | Exact match | `Field == "value"` |
| `Contains` | Contains substring | `Field.Contains("value")` |
| `StartsWith` | Starts with | `Field.StartsWith("value")` |

## Full-Text Search

For SQL Server full-text search:

```csharp
var parser = new SqlQueryParser(c => c
    .SetDefaultFields(new[] { "Name", "Description" })
    .SetFullTextFields(new[] { "Name", "Description" }));

var context = parser.GetContext(db.Products.EntityType);

string sql = await parser.ToDynamicLinqAsync("laptop", context);
// Generates: FTS.Contains(Name, "\"laptop*\"") OR FTS.Contains(Description, "\"laptop*\"")
```

### FTS Helper Class

The `FTS` class wraps `EF.Functions.Contains`:

```csharp
// In your queries
db.Products.Where(p => FTS.Contains(p.Name, "search term"));

// Equivalent to
db.Products.Where(p => EF.Functions.Contains(p.Name, "search term"));
```

## Navigation Properties

The parser automatically handles EF Core navigation properties:

### Non-Collection Navigation

```csharp
// Query on related entity
"category.name:electronics"
// Generates: Category.Name == "electronics"
```

### Collection Navigation

```csharp
// Query on collection items
"tags.name:sale"
// Generates: Tags.Any(t => t.Name == "sale")
```

### Depth Limiting

```csharp
var parser = new SqlQueryParser(c => c
    .SetFieldDepth(2));  // Limit navigation depth

// "category.parent.grandparent.name" would be limited
```

## Date Handling

### Date Math

```csharp
// Relative dates
"created:>now-7d"
// Generates: Created > DateTime.Parse("2024-01-08") // 7 days ago

// Date ranges
"created:[now-30d TO now]"
// Generates: (Created >= ... AND Created <= ...)
```

### Custom Date Parsing

```csharp
var parser = new SqlQueryParser(c => c
    .SetDateTimeParser(value => {
        if (value.StartsWith("now"))
            return ParseDateMath(value).ToString("O");
        return DateTime.Parse(value).ToString("O");
    })
    .SetDateOnlyParser(value => {
        if (value.StartsWith("now"))
            return DateOnly.FromDateTime(ParseDateMath(value)).ToString("O");
        return DateOnly.Parse(value).ToString("O");
    }));
```

## Validation

### Automatic Field Validation

The parser automatically populates allowed fields from the entity type:

```csharp
var context = parser.GetContext(db.Products.EntityType);

// context.ValidationOptions.AllowedFields contains all entity properties
```

### Custom Validation

```csharp
var parser = new SqlQueryParser(c => c
    .SetValidationOptions(new QueryValidationOptions {
        AllowedFields = { "Name", "Status", "Price" },
        RestrictedFields = { "InternalNotes" },
        AllowLeadingWildcards = false
    }));

var result = await parser.ValidateAsync(query, context);
if (!result.IsValid)
{
    Console.WriteLine($"Invalid: {result.Message}");
}
```

## Entity Type Filtering

Control which properties and navigations are exposed:

```csharp
var parser = new SqlQueryParser(c => c
    // Filter properties
    .UseEntityTypePropertyFilter(property => {
        // Exclude internal properties
        return !property.Name.StartsWith("Internal");
    })
    
    // Filter navigation properties
    .UseEntityTypeNavigationFilter(navigation => {
        // Exclude audit navigations
        return navigation.Name != "AuditLogs";
    })
    
    // Filter skip navigations (many-to-many)
    .UseEntityTypeSkipNavigationFilter(skipNav => {
        return true; // Include all
    }));
```

## Complete Example

```csharp
using Foundatio.Parsers.SqlQueries;
using Microsoft.EntityFrameworkCore;

public class ProductSearchService
{
    private readonly MyDbContext _db;
    private readonly SqlQueryParser _parser;

    public ProductSearchService(MyDbContext db, ILoggerFactory loggerFactory)
    {
        _db = db;
        _parser = new SqlQueryParser(c => c
            .SetLoggerFactory(loggerFactory)
            .SetDefaultFields(new[] { "Name", "Description" })
            .SetFullTextFields(new[] { "Name", "Description" })
            .UseFieldMap(new Dictionary<string, string> {
                { "category", "Category.Name" },
                { "brand", "Brand.Name" }
            })
            .UseIncludes(new Dictionary<string, string> {
                { "available", "Status == \"Active\" AND Inventory > 0" },
                { "sale", "DiscountPercent > 0" }
            })
            .SetFieldDepth(2)
            .SetValidationOptions(new QueryValidationOptions {
                AllowLeadingWildcards = false,
                AllowedMaxNodeDepth = 5
            }));
    }

    public async Task<List<Product>> SearchAsync(string query)
    {
        var context = _parser.GetContext(_db.Products.EntityType);

        // Validate
        var validation = await _parser.ValidateAsync(query, context);
        if (!validation.IsValid)
            throw new ArgumentException(validation.Message);

        // Convert to Dynamic LINQ
        string dynamicLinq = await _parser.ToDynamicLinqAsync(query, context);

        // Execute query
        return await _db.Products
            .Include(p => p.Category)
            .Include(p => p.Brand)
            .Where(_parser.ParsingConfig, dynamicLinq)
            .ToListAsync();
    }

    public async Task<PagedResult<Product>> SearchPagedAsync(
        string query,
        string sort = null,
        int page = 1,
        int pageSize = 20)
    {
        var context = _parser.GetContext(_db.Products.EntityType);
        string dynamicLinq = await _parser.ToDynamicLinqAsync(query, context);

        var baseQuery = _db.Products
            .Include(p => p.Category)
            .Where(_parser.ParsingConfig, dynamicLinq);

        // Get total count
        int total = await baseQuery.CountAsync();

        // Apply sorting
        if (!string.IsNullOrEmpty(sort))
        {
            // Parse sort: "-created +name" -> "Created desc, Name asc"
            var sortParts = sort.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.StartsWith("-") 
                    ? $"{s.Substring(1)} desc" 
                    : $"{s.TrimStart('+')} asc");
            
            baseQuery = baseQuery.OrderBy(_parser.ParsingConfig, 
                string.Join(", ", sortParts));
        }

        // Apply paging
        var items = await baseQuery
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

        return new PagedResult<Product>
        {
            Items = items,
            Total = total,
            Page = page,
            PageSize = pageSize
        };
    }
}
```

## Troubleshooting

### Dynamic LINQ Errors

```csharp
try
{
    var results = await db.Products
        .Where(parser.ParsingConfig, dynamicLinq)
        .ToListAsync();
}
catch (ParseException ex)
{
    // Dynamic LINQ parsing error
    Console.WriteLine($"Parse error: {ex.Message}");
}
```

### Field Not Found

```csharp
var context = parser.GetContext(db.Products.EntityType);

// Check available fields
foreach (var field in context.Fields)
{
    Console.WriteLine($"{field.FullName}: {field.GetType().Name}");
}
```

### Navigation Depth Issues

```csharp
// If queries on deep navigations fail, increase depth
var parser = new SqlQueryParser(c => c
    .SetFieldDepth(4));  // Allow deeper navigation
```

## Next Steps

- [Query Syntax](./query-syntax) - Query syntax reference
- [Validation](./validation) - Query validation
- [Field Aliases](./field-aliases) - Field mapping
