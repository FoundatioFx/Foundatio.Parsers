# Syntax Compatibility with Lucene and Elasticsearch

Foundatio.Parsers is a deliberate **superset** of the [Lucene classic query syntax](https://lucene.apache.org/core/10_5_1/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html) and [Elasticsearch's `query_string`](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), not a strict clone. Most syntax round-trips cleanly between the two. This page catalogues where it does not, so you can tell which constructs are safe to hand to a plain Lucene/`query_string` consumer and which are Foundatio.Parsers extensions.

Rows were verified against live Elasticsearch instances (**8.19** and **9.5**) using the read-only `_validate/query` endpoint, which reports whether `query_string` accepts a given query without executing a search, and by indexing documents and asserting which ones come back where accepting-versus-rejecting was not the whole story. Rows describing what this library generates were verified by building the query through `ElasticQueryParser` and inspecting the request JSON. Behavior can shift between Elasticsearch major versions; treat this as a snapshot, not a guarantee.

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

### Range shorthand that `query_string` reads as a term

| Syntax | Our meaning | `query_string` behavior |
|--------|-------------|--------------------------|
| `field:1..5` on a **text or keyword** field | Range, same as `field:[1 TO 5]` | Parses as a search for the **literal term `1..5`** |

With no opening bracket, `..` never reaches Lucene's range lexer, and dots are ordinary term characters -- so the whole thing is just a term. Confirmed by indexing a document whose keyword value is literally `1..5` alongside numeric documents: `kw:1..5` returned only the literal-valued document and ignored the intended range entirely.

This is field-type dependent, which is what makes it easy to miss. The *same* query on a numeric field is rejected outright (`failed to create query: multiple points`) and so appears in Tier 2. The bracketed `field:[1 .. 5]` form is rejected on every field type, also Tier 2.

### Term modifiers this library parses but does not translate

These are accepted by the parser and recorded on the AST, but `ElasticQueryParser` does not carry them into the generated Elasticsearch query. The modifier is silently ignored, so the query still runs and still returns results -- just not the results the syntax asked for. This is a gap in this library rather than a disagreement with Elasticsearch, which supports all of these.

| Syntax | What the syntax implies | What `ElasticQueryParser` actually emits |
|--------|-------------------------|-------------------------------------------|
| `field:value~2` | Fuzzy match, edit distance 2 | `match` with **no `fuzziness`** -- an exact match |
| `field:"a b"~5` | Phrase proximity, slop 5 | `match_phrase` with **no `slop`** -- adjacent terms only |
| `field:value^2` | Boosted relevance score | `match`/`term` with **no `boost`** -- unweighted |
| `field:/foo.bar/` (analyzed) | Regular expression match | `match` on the **literal pattern text** `foo.bar` |
| `field:/[0-9]+/` (keyword) | Regular expression match | `term` for the **literal string** `[0-9]+` |
| `field:/val.*/` (analyzed) | Regular expression match | `query_string` with `analyze_wildcard`, treating `val.*` as a **wildcard** |
| `field:/val.*/` (keyword) | Regular expression match | `prefix` query for the literal string `val.` -- the `.` matches literally |

Verified by building each query through `ElasticQueryParser` against a mapped index and inspecting the request JSON actually sent to Elasticsearch. The AST does record the modifier -- `Proximity`, `Boost`, and `IsRegexTerm` are all populated, and `GenerateQueryVisitor` round-trips the query text correctly -- so the information is lost specifically during Elasticsearch query generation, not during parsing.

No `regexp` query is ever emitted. Which wrong query you get depends on whether the pattern happens to end in `*`: only then does the wildcard/prefix path apply, because `GetSingleFieldQuery` selects it on `term.EndsWith("*")` alone and has no notion of regex syntax. Every other pattern is passed through as an ordinary term, so `/[0-9]+/` searches for the literal characters `[0-9]+`.

The regex rows are the most dangerous of the group. A wildcard or prefix interpretation still returns plausible-looking results, and a literal-text match usually returns nothing at all while looking like a legitimately empty result set. If you need any of these semantics today, construct that part of the query with the Elasticsearch client directly. Tracked as a bug in [#278](https://github.com/FoundatioFx/Foundatio.Parsers/issues/278).

## Tier 2: Rejected outright by `query_string`

These fail with a parse error in Elasticsearch, which is a much safer failure mode than Tier 1 -- you find out immediately rather than shipping wrong results.

| Syntax | Our meaning | `query_string` error |
|--------|-------------|------------------------|
| `field:-value`, `field:!value`, `field:+value` | Same as `-field:value` / `!field:value` / `+field:value` | `org.apache.lucene.queryparser.classic.ParseException: Cannot parse 'field:-value': Encountered "-" ...` |
| `field:-(value)` | Same as `-field:(value)` | Same `ParseException` family |
| `field:[1 .. 5]` | Same as `field:[1 TO 5]` | `ParseException: Encountered " <RANGE_GOOP> ".. ""`. Rejected on every field type, because the brackets put `..` inside Lucene's range lexer. |
| `field:1..5` on a **numeric** field | Same as `field:[1 TO 5]` | `query_shard_exception: failed to create query: multiple points`. See Tier 1 for the text/keyword case, which is **not** rejected. |
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
| `_exists_:field` | Standard `query_string` syntax; not a Foundatio.Parsers addition. Note it is an *Elasticsearch* extension, so it is not portable to a bare Lucene `QueryParser` -- see "Writing portable queries" below. |
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

Portability depends on *which* consumer you mean, and the two are not the same. Elasticsearch `query_string` is itself a superset of Lucene: it subclasses Lucene's `QueryParser` and adds mapping-aware extensions. So a query can be portable to `query_string` and still be wrong in bare Lucene.

Safe for **both** a bare Lucene `QueryParser` and Elasticsearch `query_string`:

- `+`/`-`/`NOT`/`!` and `AND`/`OR` immediately **before** the field name, never after the colon
- `[min TO max]` / `{min TO max}` for ranges, not the `..` shorthand
- Trailing wildcards, quoted phrases, and dotted field paths written unescaped

Safe for Elasticsearch `query_string` **only** -- not for bare Lucene:

- `_exists_:field`, and `NOT _exists_:field` for the missing-field case. These are Elasticsearch [syntax extensions](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), implemented by Elasticsearch's own query parser rather than Lucene's. Bare Lucene parses `_exists_` as an ordinary **field name** and `field` as its term, so it silently queries a field called `_exists_` instead of testing existence. Verified against Elasticsearch: `_exists_:name` lowers to `ConstantScore(FieldExistsQuery [field=name])`, which is an Elasticsearch construct with no Lucene classic-syntax equivalent.
- `>`, `>=`, `<`, `<=` single-sided ranges, which are likewise a `query_string` addition rather than classic Lucene syntax.

Avoid everywhere if the query needs to travel: `@include:` macros, `_missing_:field`, geo proximity (`~distance`), post-colon operator placement, and the `..` range shorthand.

Fuzzy, proximity, boost, and regex are portable *as syntax* -- `query_string` handles all four -- but this parser drops them during Elasticsearch query generation (Tier 1), so a query relying on them behaves differently here than elsewhere. That is the opposite direction from the rest of this page: the query is portable, but the local behavior is not.

## Related

- [Query Syntax](./query-syntax) -- the full syntax reference for what this parser accepts
- [Negation and Prefix Operators](./visitors#negation-and-prefix-operators) -- how `NOT`, `-`, and `!` are represented internally
- Open design questions on extending or restricting the Tier 2 behaviors are tracked in the linked GitHub issues on post-colon range operators and aggregation `!` handling
