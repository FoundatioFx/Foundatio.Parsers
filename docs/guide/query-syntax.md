# Query Syntax

The query syntax is inspired by [Lucene classic QueryParser](https://lucene.apache.org/core/10_3_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html) and [Elasticsearch query_string](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), but it is not a drop-in implementation of either. Examples using `LuceneQueryParser.Parse` demonstrate AST parsing, not backend execution. See [Syntax Compatibility](./syntax-compatibility) for extensions, query-generation limitations, and migration guidance.

## Basic Queries

### Term Queries

Match documents where a field contains a specific value:

| Syntax | Description | Example |
|--------|-------------|---------|
| `field:value` | Field term; matching depends on mapping and analyzer | `status:active` |
| `field:"quoted value"` | Quoted value; a phrase on analyzed text fields | `name:"John Smith"` |
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

With the Elasticsearch query builder, check whether a field has an indexed value:

| Syntax | Description |
|--------|-------------|
| `_exists_:field` | Field has an indexed value |
| `_missing_:field` | Field has no indexed value; implemented as negated `exists` |

This is not simply a check for a non-null property in `_source`: mappings can affect which values are indexed. `_missing_` is a Foundatio extension, while `_exists_` is supported by Elasticsearch but is not existence syntax in bare Lucene classic.

```csharp
// Find documents with an indexed title
var result = parser.Parse("_exists_:title");

// Find documents without an indexed description
result = parser.Parse("_missing_:description");
```

### Wildcard Queries

The parser accepts wildcard characters in terms, but backend support is more limited than the Lucene syntax suggests:

| Pattern | Default Elasticsearch query builder |
|---------|-------------------------------------|
| `name:john*` | Prefix query on keyword fields; `query_string` on analyzed fields |
| `name:jo?n` | Ordinary `match` or `term`, not a single-character wildcard |
| `name:jo*n`, `name:*john` | Ordinary `match` or `term`, not wildcard queries |

```csharp
// Parse a trailing-star prefix expression
var result = parser.Parse("name:john*");
```

::: warning Wildcard translation limitations
Only an unquoted term ending in `*` takes the default builder's special trailing-star path. On analyzed fields that path sets `analyze_wildcard: true` and `allow_leading_wildcard: false`, unlike Elasticsearch's defaults. On keyword fields, embedded wildcard characters are literal parts of the prefix. Escaping a trailing `*` also loses its literal distinction after unescaping. Fieldless queries depend on configured default fields. See [Wildcard Compatibility](./syntax-compatibility#wildcards-depend-on-the-generated-query-path).
:::

[Validation options](./validation) can reject leading wildcard input; enabling such input does not add missing wildcard translation support.

### Regex Queries

The parser recognizes regular expressions enclosed in forward slashes:

```
field:/regex/
```

```csharp
// Parse a regex expression into the AST; this does not execute it
var result = parser.Parse("email:/.*@example\\.com/");
```

::: warning Regex query generation is not implemented
The AST stores `IsRegexTerm`, but the default `ElasticQueryParser` query builder does not emit a `regexp` query. A pattern such as `/foo.bar/` becomes an analyzed `match` on a text field or a literal `term` on a keyword field. A pattern ending in `*` instead takes the trailing-star path: `query_string` on analyzed fields or `prefix` on keyword fields. For example, `/val.*/` becomes the literal prefix `val.` on a keyword field. See [Syntax Compatibility](./syntax-compatibility#term-modifiers-this-library-parses-but-does-not-translate).
:::

## Range Queries

Range queries express bounds; field mappings determine how the backend interprets the values.

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

Inside brackets or braces, use `..` instead of `TO`. The brackets still determine inclusivity:

```csharp
// Equivalent to field:[1 TO 5]
var result = parser.Parse("field:[1 .. 5]");

// Equivalent to field:{1 TO 5}
result = parser.Parse("field:{1 .. 5}");
```

::: warning Brackets are required
Bare `field:1..5` is an ordinary term in Foundatio, not a range. Only the bracketed `..` delimiter is a Foundatio extension. Use `field:[1 TO 5]` for an inclusive range that also uses Lucene classic and Elasticsearch `query_string` syntax. See [Syntax Compatibility](./syntax-compatibility#bare-dots-do-not-make-a-range).
:::

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

These comparison forms are supported by Elasticsearch `query_string`, but are not classic Lucene range syntax. Use the bracketed equivalents for a bare Lucene classic consumer.

### Date-range Time Zones

On a mapped Elasticsearch `date` or `date_nanos` field, a caret suffix supplies a range time zone:

```csharp
var result = parser.Parse("created:[2024-01-01 TO *]^\"America/Chicago\"");
```

The default Elasticsearch range builder assigns that value to `time_zone`, not `boost`. Do not use `^2` to boost a date range: it is interpreted as a time-zone value instead. The caret time-zone form is a Foundatio extension; for an external `query_string` request, configure its `time_zone` option separately. See [Date-range Compatibility](./syntax-compatibility#date-range-caret-values-are-time-zones-not-boosts).

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
| `!` | Term must not be present (excluded) |

```csharp
// Required term
var result = parser.Parse("+status:active");

// Excluded term
result = parser.Parse("-deleted:true");

// Combined
result = parser.Parse("+status:active -deleted:true type:user");
```

`-`, `!`, and `NOT` all negate a clause, but the parser stores them on different node properties: `NOT` sets `IsNegated` while `-` and `!` set `Prefix`. In query contexts, use the `IsExcluded()` extension method rather than checking either property directly. See [Negation and Prefix Operators](./visitors#negation-and-prefix-operators).

Write clause operators before the field name, with symbolic prefixes attached: `-field:value`, `!field:value`, or `NOT field:value`. Legacy post-colon forms such as `field:-value` and `field:-(value)` are currently accepted for terms and groups, but not ranges: `field:-[1 TO 2]` and `field:NOT [1 TO 2]` throw `FormatException`. The decision in [#272](https://github.com/FoundatioFx/Foundatio.Parsers/issues/272#issuecomment-5701649902) is to reject post-colon operators consistently, not to extend them. Migrate to leading operators; `field:(-value)` remains a distinct, field-scoped clause form.

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
var result = parser.Parse("location:u4pruydqqv~75mi");

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

// Grouped nested query
query = await parser.BuildQueryAsync("comments:(comments.author:john comments.text:hello)");

// Negated nested query
query = await parser.BuildQueryAsync("NOT comments:(comments.author:spammer)");

// Exists/missing on nested fields
query = await parser.BuildQueryAsync("_exists_:comments.author");
query = await parser.BuildQueryAsync("_missing_:comments");
```

::: info Elasticsearch Limitation
Standard Elasticsearch `query_string` does not support nested documents. With the mappings and nested visitor configured, Foundatio.Parsers can detect nested fields and wrap queries appropriately. This does not add support for the missing term modifiers or wildcard forms described above.
:::

For a full explanation of how the AST is structured and traversed for nested queries, see [Nested Queries and Visitor Traversal](./nested-queries).

## Boosting

The parser recognizes relevance-boost syntax:

```csharp
// Parse a term boost into the AST
var result = parser.Parse("title:important^2");

// Parse a phrase boost into the AST
result = parser.Parse("title:\"very important\"^3");
```

::: warning Term and phrase boosts are not applied
The boost is available on the AST (`TermNode.Boost`), but the default `ElasticQueryParser` query builder does not apply it to term or phrase queries. Mapped date ranges are a different case: their caret suffix supplies a time zone, not a boost. See [Syntax Compatibility](./syntax-compatibility#term-modifiers-this-library-parses-but-does-not-translate) and [Date-range Time Zones](#date-range-time-zones).
:::

## Fuzzy Queries

The parser recognizes `~` as fuzzy matching syntax (edit distance):

```csharp
// Parse fuzzy syntax with the distance omitted
var result = parser.Parse("name:john~");

// Parse fuzzy syntax with a specific edit distance
result = parser.Parse("name:john~2");
```

::: warning Fuzziness and phrase proximity are not applied
The edit distance is available on the AST (`TermNode.Proximity`), but the default `ElasticQueryParser` query builder does not set `fuzziness`. On an analyzed text field it emits an ordinary `match`, not an exact keyword match. Similarly, `"a b"~5` becomes `match_phrase` without `slop`. See [Syntax Compatibility](./syntax-compatibility#term-modifiers-this-library-parses-but-does-not-translate).
:::

## Escaping Special Characters

The ordinary term/field escape rule accepts a literal space and these characters after a backslash:

```
+ - ! ( ) { } [ ] ^ " ~ * ? : \ /
```

Do not copy Elasticsearch's entire reserved-character list into an escaping function for this parser. For example, `field\.with\.dots:value` fails because `.` is not an allowed escape; write `field.with.dots:value`. A supported grammar escape also does not guarantee that a query builder preserves literal wildcard semantics; see [Wildcard Queries](#wildcard-queries).

```csharp
// Escape colon in value (the C# string needs a second backslash)
var result = parser.Parse("url:https\\://example.com");

// Escape parentheses and a space
result = parser.Parse("name:John\\ \\(Jr\\)");
```

## Complete Example

```csharp
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

var parser = new LuceneQueryParser();

// Parse and inspect the AST; backend behavior depends on configuration
string query = @"
    (status:active OR status:pending)
    AND created:[now-30d TO now]
    AND NOT deleted:true
    AND (name:john* OR email:john*)
";

var result = parser.Parse(query);

// Debug the AST
Console.WriteLine(DebugQueryVisitor.Run(result));

// Regenerate (normalized)
string normalized = GenerateQueryVisitor.Run(result);
```

## Next Steps

- [Syntax Compatibility](./syntax-compatibility) - Known syntax and query-generation differences
- [Aggregation Syntax](./aggregation-syntax) - Dynamic aggregation expressions
- [Field Aliases](./field-aliases) - Map field names
- [Validation](./validation) - Validate and restrict queries
