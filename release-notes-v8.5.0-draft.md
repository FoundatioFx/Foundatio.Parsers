# Draft migration notes for the next release

Tentative version: **v8.5.0**. These notes describe changes since `v8.0.2`; they are not a release announcement. Confirm the final version and test evidence before publishing.

## Query behavior to review

- Move Boolean operators before a field name. For example, replace `status:-closed` with `-status:closed` and `status:NOT closed` with `NOT status:closed`. Post-colon operators that were previously accepted now fail parsing.
- Required clauses now restrict matches even when the default operator is OR. `+status:active category:premium` must match `status:active`; the optional category can affect ranking. Required or negated include operators now apply to the expanded include.
- Elasticsearch term translation now honors unescaped `*` and `?`, regex, fuzzy, phrase proximity, and boost syntax. Escaped and quoted wildcard characters stay literal. Recheck both result sets and ranking for saved queries, especially on analyzed fields and queries using `UseScoring`.
- SQL now translates unescaped `?` and leading or embedded `*` on mapped string fields to escaped `LIKE` patterns. Trailing `*` and `*term*` keep their existing prefix and contains behavior. SQL validation rejects regex, fuzzy, proximity, and boost modifiers, plus advanced wildcard patterns on non-string and full-text fields. Replace those queries with application-specific filters or an Elasticsearch query where the operation is required.
- Sort and aggregation expressions reject Boolean `NOT` and `!` rather than treating them as direction. Use `+field` or `-field` for sort order.

## Upgrade checks

1. Search saved queries and query builders for the forms above; update invalid forms before deploying.
2. Replay representative queries against the application's mappings and SQL schema. Compare generated predicates, result IDs, and ranking where scoring is enabled. Include escaped wildcards, aliases, includes, nested fields, and null values.
3. Confirm custom visitors still preserve node parent links and run at the intended priority after include expansion and field resolution.

See the [syntax compatibility guide](docs/guide/syntax-compatibility.md) for detailed query examples and compatibility limits.
