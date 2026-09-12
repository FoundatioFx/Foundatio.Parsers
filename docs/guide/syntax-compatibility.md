# Syntax Compatibility with Lucene and Elasticsearch

Foundatio.Parsers is a deliberate **superset** of the [Lucene classic query syntax](https://lucene.apache.org/core/10_5_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html) and [Elasticsearch's `query_string`](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), not a strict clone. Most syntax round-trips cleanly between the two. This page catalogues where it does not, so you can tell which constructs are safe to hand to a plain Lucene/`query_string` consumer and which are Foundatio.Parsers extensions.

Every row below was verified against a live **Elasticsearch 8.19** instance using the read-only `_validate/query` endpoint, which reports whether `query_string` accepts a given query without executing a search. Behavior can shift between Elasticsearch major versions; treat this as a snapshot, not a guarantee.

The deviations are grouped by consequence, not by feature area, because the risk they carry is wildly different:

- **Tier 1** -- the query is accepted, but means something else entirely. This is the dangerous tier: nothing errors, so a query that looks correct can silently return the wrong set. Two separate causes land here: syntax `query_string` reinterprets, and syntax *this library* parses but then drops.
- **Tier 2** -- the query throws a parse error in `query_string`. Annoying, but safe: it fails loudly instead of returning wrong data.
- **Tier 3** -- behaves identically in both. Listed for completeness so you know what's safe to rely on.

## Tier 1: Silently means something different

These are the ones to actually worry about. There are two distinct causes, and neither produces an error.

### Special prefixes that `query_string` reads as field names

`query_string` has no concept of these prefixes, so it parses them as an ordinary field name and then reports that field as unmapped.

| Syntax | Our meaning | `query_string` behavior |
|--------|-------------|--------------------------|
| `_missing_:field` | Field is null or absent | Looks for a field literally named `_missing_`. Returns `MatchNoDocsQuery("unmapped fields [_missing_]")` -- matches **nothing**, no error. |
| `@include:name` | Expand a stored query fragment | Looks for a field literally named `@include`. Same `MatchNoDocsQuery`, no error. |

Both were confirmed with `_validate/query`: the response has `"valid": true`, and the `explanation` shows the unmapped-field query rather than an error. If you pass user-authored queries through to a system that also runs `query_string` directly, these two will look like they work and then quietly return an empty set.

Note the asymmetry: `_exists_:field` is standard Elasticsearch syntax (see Tier 3), so `_exists_` and `_missing_` are **not** a matched pair even though they read like one here.

### Term modifiers this library parses but does not translate

These are accepted by the parser and recorded on the AST, but `ElasticQueryParser` does not carry them into the generated Elasticsearch query. The modifier is silently ignored, so the query still runs and still returns results -- just not the results the syntax asked for. This is a gap in this library rather than a disagreement with Elasticsearch, which supports all of these.

| Syntax | What the syntax implies | What `ElasticQueryParser` actually emits |
|--------|-------------------------|-------------------------------------------|
| `field:value~2` | Fuzzy match, edit distance 2 | `match` with **no `fuzziness`** -- an exact match |
| `field:"a b"~5` | Phrase proximity, slop 5 | `match_phrase` with **no `slop`** -- adjacent terms only |
| `field:value^2` | Boosted relevance score | `match`/`term` with **no `boost`** -- unweighted |
| `field:/val.*/` (analyzed field) | Regular expression match | `query_string` with `analyze_wildcard`, treating `val.*` as a **wildcard** |
| `field:/val.*/` (keyword field) | Regular expression match | `prefix` query for the literal string `val.` -- the `.` matches literally |

Verified by building each query through `ElasticQueryParser` against a mapped index and inspecting the request JSON actually sent to Elasticsearch. The AST does record the modifier -- `Proximity`, `Boost`, and `IsRegexTerm` are all populated, and `GenerateQueryVisitor` round-trips the query text correctly -- so the information is lost specifically during Elasticsearch query generation, not during parsing.

The two regex rows are the most dangerous here, because a wildcard or prefix interpretation of a regex pattern still returns plausible-looking results rather than obviously wrong ones. If you need any of these semantics today, construct that part of the query with the Elasticsearch client directly. Tracked as a bug in [#278](https://github.com/FoundatioFx/Foundatio.Parsers/issues/278).

## Tier 2: Rejected outright by `query_string`

These fail with a parse error in Elasticsearch, which is a much safer failure mode than Tier 1 -- you find out immediately rather than shipping wrong results.

| Syntax | Our meaning | `query_string` error |
|--------|-------------|------------------------|
| `field:-value`, `field:!value`, `field:+value` | Same as `-field:value` / `!field:value` / `+field:value` | `org.apache.lucene.queryparser.classic.ParseException: Cannot parse 'field:-value': Encountered "-" ...` |
| `field:-(value)` | Same as `-field:(value)` | Same `ParseException` family |
| `field:1..5` or `field:[1 .. 5]` | Same as `field:[1 TO 5]` | `ParseException: Encountered " <RANGE_GOOP> ".. ""` |
| `field:location~distance` (geo proximity, e.g. `location:abc123~75mi`) | Geo-distance filter | `QueryShardException: failed to create query: fuzziness cannot be [75mi]`. Elasticsearch's `~` is always fuzzy-search edit distance, so `~75mi` is parsed as an (invalid) fuzziness value, not a geo radius. |

The post-colon and `..` cases are pre-existing behavior, not something introduced recently; the [Lucene grammar](https://lucene.apache.org/core/10_5_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html) explains why they are structurally unreachable there:

```
Query  ::= ( Clause )*
Clause ::= ["+", "-"] [<TERM> ":"] ( <TERM> | "(" Query ")" )
```

The `+`/`-` modifier is positioned **before** the optional field in the grammar, so there is no production that reaches a modifier immediately after `field:`. Whether Foundatio.Parsers should extend this further (e.g. to ranges, where `field:-[1 TO 2]` currently throws `FormatException` here too) or hold the line is an open design question -- see the linked issues below.

The geo-proximity collision with fuzzy search is a namespacing consequence of reusing `~`, not a bug in either system; it just means a geo query can never be expressed through a bare `query_string`.

## Tier 3: Compatible

Verified to behave identically when built through `ElasticQueryParser` and when sent to `query_string`:

| Syntax | Notes |
|--------|-------|
| `_exists_:field` | Standard `query_string` syntax; not a Foundatio.Parsers addition. |
| `field:>10`, `>=`, `<`, `<=` | Documented directly in the [Elasticsearch ranges reference](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query). |
| `field:[10 TO *]`, `field:{* TO 10}` | Unbounded range bounds. |
| `field:val*` | Trailing wildcard. |
| `field:(+term1 +term2)` | Field grouping with per-term modifiers. |
| `field:"exact phrase"` | Quoted phrase (without a `~slop` modifier -- see Tier 1). |
| `escaped\ field\ name:value` | Backslash-escaped spaces in field names. |
| `field.with.dots:value` | Dotted (sub-object) field paths, written **unescaped**. |

Note on escaping: only the characters this parser's `escape_sequence` rule recognizes can be backslash-escaped. A dot is not one of them, so `field\.with\.dots:value` throws `FormatException` here even though `query_string` accepts it. Write dotted paths unescaped.

Fuzzy (`~2`), proximity (`~5`), boost (`^2`), and regex (`/.../ `) are **not** in this tier -- Elasticsearch supports them, but `ElasticQueryParser` drops them. See Tier 1.

Foundatio.Parsers' aggregation expression language (`terms:`, `min:`, `date:`, and so on) and sort expressions have no `query_string` equivalent at all -- they are a separate API surface built on top of the query grammar, not a deviation within it, so they are out of scope for this page.

## Writing portable queries

If a query needs to also work against a plain Elasticsearch `query_string` (or a bare Lucene `QueryParser`), stick to:

- `+`/`-`/`NOT`/`!` and `AND`/`OR` immediately **before** the field name, never after the colon
- `[min TO max]` / `{min TO max}` for ranges, not the `..` shorthand
- `>`, `>=`, `<`, `<=` single-sided ranges
- `_exists_:field` (but not `_missing_:field`, which has no portable equivalent -- express it as `NOT _exists_:field` instead)
- Trailing wildcards, quoted phrases, and dotted field paths written unescaped

Avoid `@include:` macros, `_missing_:field`, geo proximity (`~distance`), post-colon operator placement, and the `..` range shorthand if the query needs to travel outside this parser.

Fuzzy, proximity, boost, and regex are portable *as syntax* -- `query_string` handles all four -- but this parser drops them during Elasticsearch query generation (Tier 1), so a query relying on them behaves differently here than elsewhere. That is the opposite direction from the rest of this page: the query is portable, but the local behavior is not.

## Related

- [Query Syntax](./query-syntax) -- the full syntax reference for what this parser accepts
- [Negation and Prefix Operators](./visitors#negation-and-prefix-operators) -- how `NOT`, `-`, and `!` are represented internally
- Open design questions on extending or restricting the Tier 2 behaviors are tracked in the linked GitHub issues on post-colon range operators and aggregation `!` handling
