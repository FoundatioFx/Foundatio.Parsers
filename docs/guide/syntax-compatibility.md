# Syntax Compatibility

Foundatio.Parsers accepts a Lucene-style language, but it is not a drop-in implementation of either Lucene's classic `QueryParser` or Elasticsearch's `query_string`. Some constructs are extensions; others are parsed into the abstract syntax tree (AST) but are not implemented by the default Elasticsearch query builder.

This page describes known differences in the default `ElasticQueryParser` query pipeline. The examples assume explicit `text`, `keyword`, and `date` mappings and no custom query-building visitors. SQL translation, aggregation modifiers, custom visitors, and other configurations need separate verification. Successful parsing does not establish equivalent matching or scoring.

## Tier 1 — Accepted syntax with different behavior

### Library extensions that query_string interprets as fields

| Syntax | Foundatio behavior | Elasticsearch `query_string` behavior |
|--------|--------------------|---------------------------------------|
| `_missing_:name` | Builds a negated `exists` query for `name` | Treats `_missing_` as a field name, not a missing-field operator |
| `@include:active` | Expands a query include when `UseIncludes` is configured | Treats `@include` as a field name, not a macro |

If those external field names are unmapped, the query may simply match no documents; if they exist, it searches them. Neither outcome implements the intended extension. For Elasticsearch, express missing indexed values as `NOT _exists_:name`, and expand includes before sending a query to another parser. `_exists_` is itself an Elasticsearch extension, not an existence operator in bare Lucene classic syntax.

Existence concerns an **indexed value**, not merely whether a property occurs in `_source`. Mapping options such as `null_value`, `index`, and `ignore_above` can affect the result. See the [Elasticsearch exists query reference](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-exists-query).

### Term modifiers this library parses but does not translate

The parser preserves modifier metadata, but the default Elasticsearch query builder does not apply term fuzziness, phrase slop, or term/phrase boosts. Regex metadata is also ignored. These limitations are tracked in [#278](https://github.com/FoundatioFx/Foundatio.Parsers/issues/278).

| Input | Generated Elasticsearch query |
|-------|-------------------------------|
| `text:value~2` | `match` for `value`, without `fuzziness` |
| `text:"a b"~5` | `match_phrase` for `a b`, without `slop` |
| `text:value^2` | `match` for `value`, without `boost` |
| `text:"a b"^2` | `match_phrase` for `a b`, without `boost` |
| `text:/foo.bar/` | `match` for `foo.bar`, not `regexp` |
| `keyword:/[0-9]+/` | `term` for the literal value `[0-9]+`, not `regexp` |
| `text:/val.*/` | `query_string` for `val.*`, not a regex query |
| `keyword:/val.*/` | `prefix` for the literal prefix `val.`, not a regex query |

A `match` query still analyzes its input; absence of fuzziness does **not** make it an exact keyword match. Regex delimiters are removed, and normal term unescaping is applied to the payload. The default builder never selects `regexp` from `IsRegexTerm`: a payload ending in `*` takes the trailing-star path described below, while other payloads take ordinary term paths.

These are query-generation limitations, not recommendations to remove support from the AST. Use an explicitly constructed Elasticsearch query or a tested custom visitor when the application requires these features. Do not assume that successful validation means the modifiers were applied.

### Wildcards depend on the generated query path

The default builder tests whether the **unescaped, unquoted term ends in `*`**. It does not implement a general wildcard translator.

| Input | Mapped text field | Mapped keyword field |
|-------|-------------------|----------------------|
| `field:jo?n` | `match` for `jo?n` | `term` for `jo?n` |
| `field:jo*n` | `match` for `jo*n` | `term` for `jo*n` |
| `field:*john` | `match` for `*john` | `term` for `*john` |
| `field:john*` | `query_string` for `john*` | `prefix` for `john` |
| `field:jo?n*` | `query_string` for `jo?n*` | `prefix` for the literal prefix `jo?n` |
| `field:john\*` | Also takes the trailing-star path after unescaping | Also builds a `prefix` for `john` |

Thus `?` alone is not a single-character wildcard in generated queries. Embedded or leading wildcards without a final `*` are not wildcard queries either. Escaping a trailing star does not reliably preserve literal-star semantics through this builder.

For the analyzed trailing-star path, Foundatio sets `analyze_wildcard: true` and `allow_leading_wildcard: false`. Elasticsearch `query_string` defaults are `false` and `true`, respectively. Analyzer tokenization can therefore change results even for a trailing-star input. Align those options, mappings, analyzers, and default fields before comparing results; matching the spelling alone is insufficient. See [query_string wildcard options](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query#query-string-wildcard).

The fieldless case also depends on configuration: with configured analyzed default fields, `john*` takes the analyzed `query_string` path; with no default fields configured, it falls back to `multi_match`, not that wildcard path.

### Date-range caret values are time zones, not boosts

On a mapped `date` or `date_nanos` field, the default range builder uses the range's caret value as `time_zone`:

```text
date:[2024-01-01 TO *]^"America/Chicago"
```

This sets `time_zone` to `America/Chicago`; it does not boost the range. By contrast, Elasticsearch `query_string` expects a numeric boost after `^`, so the quoted time-zone suffix is not portable. Set the external query's `time_zone` option separately instead.

A numeric caret value such as `^2` is also assigned to `time_zone` on this path, rather than applied as a boost, and may be rejected by Elasticsearch as an invalid time zone. This exception is why a blanket statement that all boosts are simply ignored is incorrect. On other query paths, a parsed caret value does not establish that a scoring boost is generated.

## Tier 2 — Syntax rejected by one of the parsers

| Input | Foundatio behavior | External syntax / migration |
|-------|--------------------|-----------------------------|
| `field:[1 .. 5]` or `field:[1..5]` | A range; brackets determine inclusivity | Use `field:[1 TO 5]` for Lucene classic or Elasticsearch `query_string` |
| `field:-value`, `field:NOT value`, `field:-(a OR b)` | Legacy post-colon operator forms accepted by this grammar | Write `-field:value`, `NOT field:value`, or `-field:(a OR b)` instead |
| `field:-[1 TO 5]`, `field:NOT [1 TO 5]` | `FormatException` | Put the operator before the field: `-field:[1 TO 5]` |
| `field\.with\.dots:value` | `FormatException`; dot is not an allowed backslash escape | Write `field.with.dots:value` |
| `location:75044~75mi` | Geographic distance syntax when the geo visitor and resolver are configured | Use an explicit Elasticsearch geo query, not a fuzzy `query_string` expression |

The post-colon inconsistency is not a syntax pattern to adopt. [#272](https://github.com/FoundatioFx/Foundatio.Parsers/issues/272#issuecomment-5701649902) records the decision to reject post-colon operators consistently rather than extend them to ranges. Use leading operators now. An operator inside a field-scoped group, such as `field:(-value)`, is a different case.

### Bare dots do not make a range

`field:1..5` and `1..5` are ordinary terms in Foundatio, **not ranges**. The grammar only recognizes `..` as a range delimiter inside brackets or braces. Elasticsearch also treats the bare form as a term; backend conversion can then fail on a numeric mapping. That is not evidence that Foundatio interprets it as a numeric interval.

Use `field:[1 TO 5]` when an inclusive interval is intended. The local shorthand is `field:[1 .. 5]`; `{1 .. 5}`, `[1 .. 5}`, and `{1 .. 5]` retain their respective endpoint inclusivity.

## Tier 3 — Shared syntax, not an equivalence guarantee

| Form | Scope and caveats |
|------|-------------------|
| `field:value`, `field:"a b"` | Shared syntax; field mappings and analyzers determine matching |
| `NOT field:value`, `-field:value`, `!field:value` | Use leading operators; parenthesize combinations explicitly |
| `field:(a OR b)` | Shared field-group syntax; do not assume identical required/optional clause or scoring behavior |
| `field:[1 TO 5]`, `field:{1 TO 5}`, `field:[1 TO *]` | Shared range forms; value interpretation depends on field type |
| `field.with.dots:value`, `first\ name:Alice` | Dotted paths remain unescaped; a literal field-name space is escaped |
| `_exists_:field`, `NOT _exists_:field` | Supported by Foundatio and Elasticsearch; not existence syntax in bare Lucene classic |
| `field:>10`, `field:>=10`, `field:<10`, `field:<=10` | Foundatio and Elasticsearch comparison syntax; use bracketed ranges for bare Lucene classic |

`!` is a Boolean NOT token in Lucene's classic grammar and is documented as a NOT alias by Elasticsearch. It should not be classified as a Foundatio-only extension merely because the separate `+`/`-` modifier production does not list it. In Foundatio, attach symbolic prefixes to their clause; use `NOT field:value` when whitespace is desired.

Escaping is not interchangeable either: Foundatio's ordinary escape rule accepts a literal space and `+ - ! ( ) { } [ ] ^ " ~ * ? : \ /`. It does not accept backslash-escaped dots or the full reserved-character list from Elasticsearch. See [Escaping Special Characters](./query-syntax#escaping-special-characters).

## Choosing a portable query

Start with explicit fields, explicit Boolean operators and parentheses, leading negation, and `TO` ranges. For a bare Lucene classic consumer, do not send Elasticsearch-specific existence or comparison syntax. For an Elasticsearch consumer, replace `_missing_` with `NOT _exists_` and expand configured includes first.

Do not forward fuzzy, proximity, regex, boost, or general wildcard expressions under an assumption of equivalent query generation. Prefix searches still require aligned wildcard options and field configuration. Date-range time zones must be configured for each consumer rather than forwarded as caret suffixes.

Finally, compare **both the generated query and returned documents** under the application's actual mappings. Include positive and negative fixtures, analyzed and keyword fields, and scoring assertions when ranking matters. Default fields, default operators, filter/scoring context, nested queries, aliases, and visitors can all change behavior without changing whether the input parses.

## Verification and maintenance

`SyntaxCompatibilityTests` in `tests/Foundatio.Parsers.ElasticQueries.Tests` characterizes the AST and serialized query shapes described here using in-memory mappings; these tests do not contact Elasticsearch. They cover bare versus bracketed ranges, escaping, modifier metadata, wildcard branches, regex fallbacks, date-range time zones, and default-field behavior. The tests intentionally record existing limitations and must be updated together with these docs when the underlying behavior changes.

That coverage is not a live-server compatibility certification or an exhaustive catalogue of every accepted expression. External semantics should be checked against the target engine version and realistic indexed documents, not inferred solely from `_validate/query` success.

Primary references: [Elasticsearch query_string](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), [Lucene classic QueryParser](https://lucene.apache.org/core/10_3_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html), and [Lucene's classic grammar](https://github.com/apache/lucene/blob/main/lucene/queryparser/src/java/org/apache/lucene/queryparser/classic/QueryParser.jj). The local implementation is in `LuceneQueryParser.peg` and `Extensions/DefaultQueryNodeExtensions.cs`.

Related work: [#271](https://github.com/FoundatioFx/Foundatio.Parsers/issues/271) tracks this documentation, [#278](https://github.com/FoundatioFx/Foundatio.Parsers/issues/278) tracks missing query modifiers, and [#272](https://github.com/FoundatioFx/Foundatio.Parsers/issues/272) tracks post-colon syntax. Sorting and aggregation operator behavior is a separate concern tracked in [#273](https://github.com/FoundatioFx/Foundatio.Parsers/issues/273).
