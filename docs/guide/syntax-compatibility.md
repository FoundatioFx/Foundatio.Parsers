# Syntax Compatibility

Foundatio.Parsers accepts a Lucene-style language, but it is not a drop-in implementation of either Lucene's classic `QueryParser` or Elasticsearch's `query_string`. Some constructs are extensions; others are parsed into the abstract syntax tree (AST) but are not implemented by the default Elasticsearch query builder.

This page describes known differences in the default `ElasticQueryParser` query pipeline. The examples assume explicit `text`, `keyword`, and `date` mappings and no custom query-building visitors. SQL translation, aggregation modifiers, custom visitors, and other configurations need separate verification. Successful parsing does not establish equivalent matching or scoring.

## Tier 1 — Accepted syntax with different behavior

### Boolean defaults and clause semantics

Foundatio's default query operator is **AND**; Elasticsearch `query_string` and bare Lucene classic default to **OR**. Configure the operator explicitly when migrating fieldless or adjacent terms. Matching that setting does not remove the other differences below.

Consider these documents with a standard-analyzed `text` field:

| ID | `text` |
|----|--------|
| `a` | `alpha beta` |
| `b` | `alpha gamma beta` |
| `c` | `beta gamma` |
| `l` | `alpha` |

With an explicit OR default, the following queries illustrate matching behavior and remaining differences. The IDs in this table refer only to these four documents:

| Query | Foundatio | Elasticsearch `query_string` |
|-------|-----------|----------------------------------------------|
| `+text:alpha text:gamma` | `a,b,l` | `a,b,l` |
| `text:alpha OR NOT text:beta` | `a,b,l` | `l` |
| `text:alpha OR text:beta AND text:gamma` | `a,b,c,l` | `b,c` |
| `(text:alpha OR text:beta) AND text:gamma` | `b,c` | `b,c` |
| `text:alpha OR (text:beta AND text:gamma)` | `a,b,c,l` | `a,b,c,l` |

In the first row, `+text:alpha` means “the document must contain alpha.” The letters `a`, `b`, `c`, and `l` are document IDs, not query operators. Document `c` contains only `beta gamma`, so it must not appear in the results. Earlier versions incorrectly returned it; [#288](https://github.com/FoundatioFx/Foundatio.Parsers/issues/288) records that defect. The corrected query enforces `alpha` in both consumers; `gamma` remains optional for matching and contributes to scoring. Required groups preserve their internal Boolean operator: `+(text:alpha OR text:beta)` requires either term. These guarantees apply to the default Elasticsearch query pipeline.

The next two rows expose Boolean interpretation differences, not analyzer or scoring differences. Foundatio's `OR NOT` combines a positive condition with a complement; the classic prohibited-clause interpretation excludes `beta` from the whole query at that level. Mixed `AND`/`OR` syntax also needs explicit grouping. Parentheses make the two positive-group examples unambiguous, but are not a universal conversion recipe for required clauses or negated disjunctions. Translate the intended Boolean structure explicitly when moving between consumers, and assert returned document IDs.

Pure-negative queries are another separate case. `NOT text:alpha`, `-text:alpha`, and `!text:alpha` select the complement in Foundatio and Elasticsearch, but return no documents in bare Lucene classic: a prohibited clause alone supplies no positive match set. A custom Lucene consumer must explicitly implement complement semantics when required. The accepted `!` token does not establish equivalent pure-negative results.

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

### Matching and scoring are separate contracts

The default builder does not apply `^8` to terms, phrases, or parenthesized groups. Elasticsearch `query_string` applies the boost, so the same expression can rank matching documents differently. Group boosts need the same caution as term and phrase boosts.

`UseScoring = false` intentionally builds filter-context queries with zero scores. Optional conditions can affect ranking only when scoring is enabled. Scores also depend on mappings, analyzers, indexed statistics, and query rewriting; matching document sets do not imply identical scores across consumers or indexes.

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

Thus `?` alone is not a single-character wildcard in generated queries. Embedded or leading wildcards without a final `*` are not wildcard queries either. Escaping a trailing star does not reliably preserve literal-star semantics through this builder. [Issue #289](https://github.com/FoundatioFx/Foundatio.Parsers/issues/289) tracks wildcard and escaped-literal translation.

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
| `field:-value`, `field:NOT value`, `field:-(a OR b)` | Parse rejection (breaking correction of legacy terms/groups) | Write `-field:value`, `NOT field:value`, or `-field:(a OR b)` instead |
| `field:-[1 TO 5]`, `field:NOT [1 TO 5]` | Parse rejection | Put the operator before the field: `-field:[1 TO 5]` |
| `field\.with\.dots:value` | Parse rejection; dot is not an allowed backslash escape | Write `field.with.dots:value` |
| `location:"New York, NY"~75mi` | Geographic distance syntax with a `geo_point` mapping, geo visitor, and city resolver | Use an explicit Elasticsearch geo query, not a fuzzy `query_string` expression |

`LuceneQueryParser.Parse` reports the rejected grammar examples as `FormatException`; the public `ElasticQueryParser.BuildQueryAsync` API reports query validation failures as `QueryValidationException`. A backend request rejection is a third, separate outcome. Do not conflate these with transport or service failures.

Operators must precede the field name for terms, groups, and ranges. This implements [the decision in issue #272](https://github.com/FoundatioFx/Foundatio.Parsers/issues/272#issuecomment-5701649902). See [breaking syntax correction and migration examples](./query-syntax.md#breaking-syntax-correction-operator-placement) before upgrading. Inner clauses such as `field:(-value)` and quoted or escaped literal values remain supported.

### Bare dots do not make a range

`field:1..5` and `1..5` are ordinary terms in Foundatio, **not ranges**. The grammar only recognizes `..` as a range delimiter inside brackets or braces. Elasticsearch also treats the bare form as a term; backend conversion can then fail on a numeric mapping. That is not evidence that Foundatio interprets it as a numeric interval.

Use `field:[1 TO 5]` when an inclusive interval is intended. The local shorthand is `field:[1 .. 5]`; `{1 .. 5}`, `[1 .. 5}`, and `{1 .. 5]` retain their respective endpoint inclusivity.

## Tier 3 — Shared syntax, not an equivalence guarantee

| Form | Scope and caveats |
|------|-------------------|
| `field:value`, `field:"a b"` | Shared syntax; field mappings and analyzers determine matching |
| `NOT field:value`, `-field:value`, `!field:value` | Use leading operators; pure-negative and `OR NOT` behavior differs as described above |
| `field:(a OR b)` | Shared field-group syntax; do not assume identical required/optional clause or scoring behavior |
| `field:[1 TO 5]`, `field:{1 TO 5}`, `field:[1 TO *]` | Shared range forms; value interpretation depends on field type |
| `field.with.dots:value`, `first\ name:Alice` | Dotted paths remain unescaped; a literal field-name space is escaped |
| `_exists_:field`, `NOT _exists_:field` | Supported by Foundatio and Elasticsearch; not existence syntax in bare Lucene classic |
| `field:>10`, `field:>=10`, `field:<10`, `field:<=10` | Foundatio and Elasticsearch comparison syntax; use bracketed ranges for bare Lucene classic |

`!` is a Boolean NOT token in Lucene's classic grammar and is documented as a NOT alias by Elasticsearch. It should not be classified as a Foundatio-only extension merely because the separate `+`/`-` modifier production does not list it. In Foundatio, attach symbolic prefixes to their clause; use `NOT field:value` when whitespace is desired.

Escaping is not interchangeable either: Foundatio's ordinary escape rule accepts a literal space and `+ - ! ( ) { } [ ] ^ " ~ * ? : \ /`. It does not accept backslash-escaped dots or the full reserved-character list from Elasticsearch. See [Escaping Special Characters](./query-syntax#escaping-special-characters).

### Sort and aggregation ordering

Ordering has a separate contract from search queries: `price`, `+price`, and `-price` are valid sort expressions; `+max:price` and `-max:price` select aggregation order. Boolean `!` and `NOT` are rejected in ordering expressions, including `NOT +price`, rather than interpreted as descending or silently ignored. Their meaning in search queries is unchanged. This follows the [ordering decision in #273](https://github.com/FoundatioFx/Foundatio.Parsers/issues/273#issuecomment-5687993474).

This is a behavioral breaking change for previously accepted ordering input. Choose an explicit direction when migrating; do not automatically turn ignored aggregation negation into descending order. See [Ordering Operators](./validation#ordering-operators) for validation errors, migration examples, and the limits of raw AST build overloads.

## Upgrading queries that use required clauses or includes

The required-clause fix changes search results without changing public method signatures. Treat it as a behavioral compatibility change when upgrading applications with saved queries:

| Query or condition | Previous behavior | Corrected behavior |
|---|---|---|
| `+status:active category:premium` with an OR default | Could return premium records that were not active | Only active records match; premium can increase their score |
| `+(status:active OR status:pending)` | Could require both alternatives | Either status satisfies the required group |
| `+@include:active` or `NOT @include:active` | The outer include operator was discarded | The expanded fragment is required or negated as requested |
| A required clause that produces no Elasticsearch query | Could be silently omitted | Query building reports a validation error |

Replay affected saved queries and compare document IDs and ranking before upgrading. Ordinary queries without these markers retain their existing behavior; the default operator remains AND. Include expansion is shared with other query consumers, including SQL, so review prefixed includes there too. Custom visitors see an extra outer group carrying the include operator; review assumptions about the exact AST shape and configured depth limits. This fix does not establish general equivalence with Lucene or implement the unsupported modifiers listed above.

The required/optional distinction follows [Elasticsearch's Boolean operators](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query) and [bool query](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-bool-query): mandatory conditions control which documents match, while optional conditions can improve the score of documents that already qualify.

## Choosing a portable query

Start with explicit fields, explicit Boolean operators and parentheses, leading negation, and `TO` ranges. For a bare Lucene classic consumer, do not send Elasticsearch-specific existence or comparison syntax, and explicitly account for pure-negative queries. For an Elasticsearch consumer, replace `_missing_` with `NOT _exists_` and expand configured includes first.

Do not forward negated-disjunction, fuzzy, proximity, regex, boost, or general wildcard expressions under an assumption of equivalent query generation. Prefix searches still require aligned wildcard options and field configuration. Date-range time zones must be configured for each consumer rather than forwarded as caret suffixes.

Finally, compare **both the generated query and returned documents** under the application's actual mappings. Include positive and negative fixtures, analyzed and keyword fields, and scoring assertions when ranking matters. Default fields, default operators, filter/scoring context, nested queries, aliases, and visitors can all change behavior without changing whether the input parses. For production inputs, enforce the supported subset explicitly; documenting an ignored modifier does not make it safe to accept when the application requires its semantics.

## References

See [Elasticsearch query_string](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), [Elasticsearch exists](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-exists-query), [Lucene classic QueryParser](https://lucene.apache.org/core/10_3_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html), and [Lucene's classic grammar](https://github.com/apache/lucene/blob/main/lucene/queryparser/src/java/org/apache/lucene/queryparser/classic/QueryParser.jj).
