# Query Syntax

The query syntax is inspired by [Lucene classic QueryParser](https://lucene.apache.org/core/10_3_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html) and [Elasticsearch query_string](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), but it is not a drop-in implementation of either. Examples using `LuceneQueryParser.Parse` demonstrate AST parsing, not backend execution. See [Syntax Compatibility](./syntax-compatibility) for extensions, query-generation limitations, and migration guidance.

## Basic Queries

The parsing snippets below use `Foundatio.Parsers.LuceneQueries` and reuse a `LuceneQueryParser` named `parser`. The geo configuration, nested-query, and complete examples create their own parsers.

### Term Queries

Match documents where a field contains a specific value:

| Syntax | Description | Example |
|--------|-------------|---------|
| `field:value` | Field term; matching depends on mapping and analyzer | `status:active` |
| `field:"quoted value"` | Quoted value; a phrase on analyzed text fields | `name:"John Smith"` |
| `value` | Search default fields | `error` |

```csharp
using Foundatio.Parsers.LuceneQueries;

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

For adjacent terms, Foundatio defaults to AND, while Elasticsearch `query_string` and bare Lucene classic default to OR. Set the default operator explicitly when moving queries between consumers.

::: warning Boolean syntax is not a cross-engine equivalence guarantee
Live comparison tests show different document sets for `A OR NOT B` and unparenthesized `A OR B AND C`. Use explicit parentheses for mixed positive operators, and translate the intended Boolean structure rather than forwarding negated disjunctions unchanged. Pure-negative queries also return the complement in Foundatio/Elasticsearch but no documents in bare Lucene classic. See [Boolean Compatibility](./syntax-compatibility#boolean-defaults-and-clause-semantics).
:::

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

The AST records required/excluded clause markers:

| Prefix | Parsed meaning |
|--------|----------------|
| `+` | Required marker; see the query-builder limitation below |
| `-` | Excluded marker |
| `!` | Excluded marker |

```csharp
// Parse a required marker; this is not a query-execution assertion
var result = parser.Parse("+status:active");

// Parse an excluded marker
result = parser.Parse("-deleted:true");

// Parse combined markers
result = parser.Parse("+status:active -deleted:true type:user");
```

::: warning Required clauses are not reliably enforced
Under an OR default, the default Elasticsearch builder treats `+text:alpha text:gamma` as optional alternatives and can return documents without `alpha`. This changes which documents match. [Issue #288](https://github.com/FoundatioFx/Foundatio.Parsers/issues/288) tracks the fix. Until fixed, do not rely on `+` for mandatory conditions; explicitly construct the required backend clauses or reject unsupported input. A successful parse or validation does not enforce the marker.
:::

`-`, `!`, and `NOT` all negate a clause, but the parser stores them on different node properties: `NOT` sets `IsNegated` while `-` and `!` set `Prefix`. In query contexts, use the `IsExcluded()` extension method rather than checking either property directly. See [Negation and Prefix Operators](./visitors#negation-and-prefix-operators).

Write clause operators before the field name, with symbolic prefixes attached: `+field:value`, `-field:value`, `!field:value`, or `NOT field:value`. Operators immediately after the colon are rejected consistently for terms, groups, and ranges. Operators inside a field-scoped group, such as `field:(-value)` or `field:(NOT value)`, remain valid and apply to the inner clause.

### Breaking syntax correction: operator placement

Following [the decision in issue #272](https://github.com/FoundatioFx/Foundatio.Parsers/issues/272#issuecomment-5701649902), previously accepted post-colon terms and groups now produce a parse error. Update saved queries and query generators before upgrading; this correction requires a breaking-change release.

| Rejected input | Replacement |
|---|---|
| `field:-value` | `-field:value` |
| `field:+value` | `+field:value` |
| `field:!value` | `!field:value` |
| `field:NOT value` | `NOT field:value` |
| `field:-(a OR b)` | `-field:(a OR b)` |
| `field:NOT [1 TO 2]` | `NOT field:[1 TO 2]` |

For literal values, preserve the value by quoting or escaping it: `field:"-value"`, `field:\-value`, or `field:"NOT value"`. Do not move a literal sign before the field. Signed range endpoints remain valid, for example `field:[-5 TO -1]` and `field:>=-5`. Signed aggregation option values also need quotes, for example `@offset:"-6h"`.

The grammar is shared by queries, sorts, aggregations, and parsed include expressions. Audit stored expressions and custom query generators in each context; the number of affected consumer expressions cannot be inferred from this repository. This does not require grouping every value: `-field:value` remains valid. Use `field:(-value)` only when the operator should apply to an inner clause. For numeric equality with a literal negative value, quote it (`price:"-5"`); moving the sign to `-price:5` instead means exclusion, not negative five.

`LuceneQueryParser.Parse` throws `FormatException` with a cursor and a message directing callers to put the operator before the field name. Elasticsearch and SQL `ParseAsync` return `null` and record that diagnostic in the supplied context. `ElasticQueryParser.BuildQueryAsync` throws `QueryValidationException`; `SqlQueryParser.ToDynamicLinqAsync` throws `ValidationException`. These are syntax errors, not empty result sets.

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

// A quoted multiword value remains one clause in the group
result = parser.Parse("city:(\"New York\" OR Madison)");
```

On an analyzed text field, `"New York"` is a phrase; on a keyword field it is a single literal value. This ordinary field grouping does not perform geographic resolution.

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

// Previous calendar month: inclusive start, exclusive end
result = parser.Parse("created:[now-1M/M TO now/M}");

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

Quote a location containing spaces so it remains one value:

```text
location:"New York, NY"~75mi
location:10001~10mi
```

The first query searches within 75 miles of the point resolved for `New York, NY`; the second searches within 10 miles of the point resolved for ZIP code `10001`. Both need an application-provided lookup. They do not search city or ZIP-code boundaries or geocode the input automatically. Keep postal codes as strings to preserve leading zeros.

```csharp
// Parse a quoted city name; the quotes are escaped inside a C# string
var result = parser.Parse("location:\"New York, NY\"~75mi");

// A ZIP code is one value and does not need quotes
result = parser.Parse("location:10001~10mi");

// Coordinates can be supplied directly (latitude, longitude)
result = parser.Parse("location:40.7128,-74.0060~10km");
```

### Configuration

To generate an Elasticsearch [geo-distance query](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-geo-distance-query) with Foundatio, map the field as [`geo_point`](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/geo-point) and enable the geo visitor through `UseGeo`. Place names and postal codes also need an application-provided resolver. This example supplies an in-memory mapping and two illustrative lookup points:

```csharp
using System;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries;

using var mappings = new ElasticMappingResolver(() => new TypeMapping
{
    Properties = new Properties
    {
        { "location", new GeoPointProperty() }
    }
});

var parser = new ElasticQueryParser(c => c
    .UseMappings(mappings)
    .UseGeo(ResolveLocation));

var query = await parser.BuildQueryAsync("location:\"New York, NY\"~75mi");
query = await parser.BuildQueryAsync("location:10001~10mi");

static string ResolveLocation(string location)
{
    if (String.Equals(location, "New York, NY", StringComparison.OrdinalIgnoreCase))
        return "40.7128,-74.0060";

    if (String.Equals(location, "10001", StringComparison.Ordinal))
        return "40.7506,-73.9972";

    throw new ArgumentException("This example only resolves New York, NY and ZIP code 10001.", nameof(location));
}
```

The resolver receives `New York, NY` without the surrounding quotes, or the string `10001`. In an application, use the actual index mappings and a resolver that handles the inputs you support, including coordinates or geohashes if offered. The example's in-memory mapping informs query generation; it does not create an Elasticsearch index.

## Geo Range Queries

Filter documents within a geographic bounding box using the top-left and bottom-right coordinates:

```text
geofield:[topLeft TO bottomRight]
```

```csharp
// Approximate rectangle around New York City, in latitude,longitude order
var result = parser.Parse("location:[40.92,-74.26 TO 40.49,-73.70]");
```

The field must be mapped as `geo_point` and the geo visitor enabled to generate an Elasticsearch [bounding-box query](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-geo-bounding-box-query). Bounds accept coordinates or geohashes directly; the range visitor does not resolve city names. The rectangle is illustrative, not an exact city boundary.

## Nested Document Queries

Enable nested-query handling with `UseNested()`. This example uses a configured `ElasticsearchClient` named `client` and an existing `my-index` index with `comments` mapped as [`nested`](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/nested):

```csharp
using Foundatio.Parsers.ElasticQueries;

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

::: warning Term, phrase, and group boosts are not applied
The boost is available on the AST, but the default `ElasticQueryParser` query builder does not apply it to term, phrase, or group queries. Live score/ranking controls verify this difference; identical document membership alone cannot detect it. Mapped date ranges are a different case: their caret suffix supplies a time zone, not a boost. See [Matching and Scoring](./syntax-compatibility#matching-and-scoring-are-separate-contracts) and [Date-range Time Zones](#date-range-time-zones).
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

In unquoted terms and field names, a backslash can escape a literal space and these characters:

```
+ - ! ( ) { } [ ] ^ " ~ * ? : \ /
```

Dots do not need escaping: use `field.with.dots:value`. Writing `field\.with\.dots:value` throws `FormatException` from `LuceneQueryParser.Parse` because `\.` is not an allowed escape in an unquoted field name. The same restriction applies to unquoted term values. Backslashes before `&`, `|`, `=`, `<`, and `>` are also rejected there; those characters can appear unescaped within a term, although some have operator meanings in other positions.

Quoted values and regex bodies follow different rules. They accept these backslashes and preserve them in the raw AST `Term`; the `UnescapedTerm` property removes them:

| Query text | Parsing result |
|------------|----------------|
| `field:a.b` | Term value `a.b` |
| `field:a\.b` | Parse error |
| `field:a&b` | Term value `a&b` |
| `field:a\&b` | Parse error |
| `field:"a\.b"` | Quoted term; raw value `a\.b`, unescaped value `a.b` |
| `field:/a\.b/` | Regex term; raw value `a\.b`, unescaped value `a.b` |

These rules come from the [Foundatio grammar](https://github.com/FoundatioFx/Foundatio.Parsers/blob/main/src/Foundatio.Parsers.LuceneQueries/LuceneQueryParser.peg); they differ from [Elasticsearch's reserved-character rules](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query). Successful parsing still does not guarantee literal wildcard matching or regex execution in the generated query. See [Wildcard Queries](#wildcard-queries) and [Regex Queries](#regex-queries).

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
