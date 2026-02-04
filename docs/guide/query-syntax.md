# Query Syntax

The query syntax is based on [Lucene query syntax](https://lucene.apache.org/core/2_9_4/queryparsersyntax.html) and is compatible with [Elasticsearch query_string](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html).

## Basic Queries

### Term Queries

Match documents where a field contains a specific value:

| Syntax | Description | Example |
|--------|-------------|---------|
| `field:value` | Exact match | `status:active` |
| `field:"quoted value"` | Exact phrase match | `name:"John Smith"` |
| `value` | Search default fields | `error` |

```csharp
var parser = new LuceneQueryParser();

// Simple term
var result = parser.Parse("status:active");

// Quoted phrase
result = parser.Parse("name:\"John Smith\"");

// Default field search (when configured)
result = parser.Parse("error");
```

### Existence Queries

Check if a field has any value or is missing:

| Syntax | Description |
|--------|-------------|
| `_exists_:field` | Field has any value |
| `_missing_:field` | Field has no value (null or missing) |

```csharp
// Find documents with a title
var result = parser.Parse("_exists_:title");

// Find documents without a description
result = parser.Parse("_missing_:description");
```

### Wildcard Queries

Use wildcards for partial matching:

| Wildcard | Description | Example |
|----------|-------------|---------|
| `*` | Matches zero or more characters | `name:john*` |
| `?` | Matches exactly one character | `name:jo?n` |

```csharp
// Prefix match
var result = parser.Parse("name:john*");

// Single character wildcard
result = parser.Parse("code:A?123");
```

::: warning Leading Wildcards
Leading wildcards (`*value`) can be expensive. Use [validation options](./validation) to disable them if needed.
:::

### Regex Queries

Use regular expressions enclosed in forward slashes:

```
field:/regex/
```

Example:

```csharp
// Match email patterns
var result = parser.Parse("email:/.*@example\\.com/");
```

## Range Queries

Range queries filter numeric or date fields within bounds.

### Bracket Syntax

| Syntax | Description |
|--------|-------------|
| `[min TO max]` | Inclusive on both ends |
| `{min TO max}` | Exclusive on both ends |
| `[min TO max}` | Inclusive min, exclusive max |
| `{min TO max]` | Exclusive min, inclusive max |

Examples:

```csharp
// Inclusive range: 1 <= value <= 5
var result = parser.Parse("field:[1 TO 5]");

// Exclusive range: 1 < value < 5
result = parser.Parse("field:{1 TO 5}");

// Mixed: 1 <= value < 5
result = parser.Parse("field:[1 TO 5}");
```

### Shorthand Syntax

Use `..` as shorthand for inclusive ranges:

```csharp
// Equivalent to field:[1 TO 5]
var result = parser.Parse("field:1..5");
```

### Unbounded Ranges

Use `*` for unbounded sides:

```csharp
// All values before 2024
var result = parser.Parse("date:{* TO 2024-01-01}");

// All values 10 and above
result = parser.Parse("count:[10 TO *]");
```

### Comparison Operators

Use comparison operators for single-sided ranges:

| Operator | Description | Equivalent |
|----------|-------------|------------|
| `>` | Greater than | `{value TO *}` |
| `>=` | Greater than or equal | `[value TO *]` |
| `<` | Less than | `{* TO value}` |
| `<=` | Less than or equal | `{* TO value]` |

```csharp
// Greater than 10
var result = parser.Parse("age:>10");

// Greater than or equal to 10
result = parser.Parse("age:>=10");

// Less than 100
result = parser.Parse("price:<100");

// Less than or equal to 100
result = parser.Parse("price:<=100");
```

## Boolean Operators

Combine queries with boolean logic:

| Operator | Description | Alternative |
|----------|-------------|-------------|
| `AND` | Both conditions must match | `&&` |
| `OR` | Either condition must match | `\|\|` |
| `NOT` | Negate the following condition | `!` |

### Examples

```csharp
// AND - both must match
var result = parser.Parse("status:active AND type:user");

// OR - either must match
result = parser.Parse("status:active OR status:pending");

// NOT - exclude matches
result = parser.Parse("status:active AND NOT deleted:true");

// Complex boolean
result = parser.Parse("((status:active AND type:user) OR type:admin) AND NOT deleted:true");
```

### Prefix Operators

Use prefix operators for required/excluded terms:

| Prefix | Description |
|--------|-------------|
| `+` | Term must be present (required) |
| `-` | Term must not be present (excluded) |

```csharp
// Required term
var result = parser.Parse("+status:active");

// Excluded term
result = parser.Parse("-deleted:true");

// Combined
result = parser.Parse("+status:active -deleted:true type:user");
```

## Grouping

Use parentheses to group clauses and control precedence:

```csharp
// Group OR conditions
var result = parser.Parse("(status:active OR status:pending) AND type:user");

// Nested groups
result = parser.Parse("((a:1 OR b:2) AND c:3) OR d:4");
```

### Field Grouping

Apply a field to multiple values:

```csharp
// Field applies to all terms in group
var result = parser.Parse("status:(active OR pending OR review)");
```

## Date Math

Date fields support date math expressions for relative dates.

### Anchor Date

Expressions start with an anchor:
- `now` - Current date/time
- `2024-01-01||` - Specific date followed by `||`

### Math Operations

| Operation | Description |
|-----------|-------------|
| `+1d` | Add 1 day |
| `-1d` | Subtract 1 day |
| `/d` | Round down to day |

### Supported Units

| Unit | Description |
|------|-------------|
| `y` | Years |
| `M` | Months |
| `w` | Weeks |
| `d` | Days |
| `h` or `H` | Hours |
| `m` | Minutes |
| `s` | Seconds |

### Examples

Assuming current time is `2024-06-15 12:00:00`:

| Expression | Result |
|------------|--------|
| `now` | 2024-06-15 12:00:00 |
| `now+1h` | 2024-06-15 13:00:00 |
| `now-1d` | 2024-06-14 12:00:00 |
| `now-7d` | 2024-06-08 12:00:00 |
| `now/d` | 2024-06-15 00:00:00 |
| `now-1M/M` | 2024-05-01 00:00:00 |

```csharp
// Last 7 days
var result = parser.Parse("created:[now-7d TO now]");

// Last month
result = parser.Parse("created:[now-1M/M TO now/M]");

// Future dates
result = parser.Parse("expires:[now TO now+30d]");
```

## Geo Proximity Queries

Filter documents by geographic distance from a point.

### Syntax

```
geofield:location~distance
```

Where:
- `location` can be a geohash, coordinates, or resolvable location (zip code, city)
- `distance` is a number followed by a unit (`mi`, `km`, `m`)

### Examples

```csharp
// Within 75 miles of a geohash
var result = parser.Parse("location:abc123~75mi");

// Within 75 miles of a zip code (requires geo resolver)
result = parser.Parse("location:75044~75mi");

// Within 10 kilometers
result = parser.Parse("location:51.5,-0.1~10km");
```

### Configuration

Geo queries require a location resolver:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseGeo(location => {
        // Resolve location string to coordinates
        if (location == "75044")
            return "32.9,-96.8";
        return location;
    }));
```

## Geo Range Queries

Filter documents within a geographic bounding box.

### Syntax

```
geofield:[topLeft TO bottomRight]
```

### Examples

```csharp
// Bounding box with geohashes
var result = parser.Parse("location:[u4pruydqqv TO u4pruydr2n]");

// Bounding box with coordinates
result = parser.Parse("location:[51.5,-0.2 TO 51.4,-0.1]");
```

## Nested Document Queries

When using Elasticsearch, queries on nested document fields work automatically with the `ElasticQueryParser`:

```csharp
var parser = new ElasticQueryParser(c => c
    .UseMappings(client, "my-index")
    .UseNested());

// Query nested field - automatically wrapped in nested query
var query = await parser.BuildQueryAsync("comments.author:john");
```

::: info Elasticsearch Limitation
Standard Elasticsearch `query_string` does not support nested documents. Foundatio.Parsers automatically detects nested fields and wraps queries appropriately.
:::

## Boosting

Boost the relevance of specific terms:

```csharp
// Boost a term
var result = parser.Parse("title:important^2");

// Boost a phrase
result = parser.Parse("title:\"very important\"^3");
```

## Fuzzy Queries

Use `~` for fuzzy matching (edit distance):

```csharp
// Fuzzy match with default edit distance
var result = parser.Parse("name:john~");

// Fuzzy match with specific edit distance
result = parser.Parse("name:john~2");
```

## Escaping Special Characters

Escape special characters with backslash:

```
+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
```

```csharp
// Escape colon in value
var result = parser.Parse("url:https\\://example.com");

// Escape parentheses
result = parser.Parse("name:John\\ \\(Jr\\)");
```

## Complete Example

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();

// Complex query
string query = @"
    (status:active OR status:pending)
    AND created:[now-30d TO now]
    AND NOT deleted:true
    AND (name:john* OR email:*@example.com)
";

var result = parser.Parse(query);

// Debug the AST
Console.WriteLine(DebugQueryVisitor.Run(result));

// Regenerate (normalized)
string normalized = GenerateQueryVisitor.Run(result);
```

## Next Steps

- [Aggregation Syntax](./aggregation-syntax) - Dynamic aggregation expressions
- [Field Aliases](./field-aliases) - Map field names
- [Validation](./validation) - Validate and restrict queries
