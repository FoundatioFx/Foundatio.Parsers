# Production Query Contract

## Status and scope

This is the **version 1 acceptance contract for the #270 → #290 → #291 → #292 stack**, not a declaration that an unreleased draft is production-ready. All prerequisite reviews, the full solution tests, documentation checks, and both current-head production-contract jobs must pass before release approval. A red contract assertion is a blocker; it must not be changed to preserve a known implementation defect.

The executable profile is `tests/compatibility/production-profile.json`. It contains selected real application configuration and clearly labeled supplemental controls. **It is not the complete Exceptionless EventIndex configuration and does not certify a deployed application.** Neither copying these tests nor having another mapping with similarly named fields establishes compatibility.

The source baseline is pinned to:

- [Exceptionless EventIndex at deffcf1](https://github.com/exceptionless/Exceptionless/blob/deffcf18550f38813823e0ed9be4e83d04ad3f66/src/Exceptionless.Core/Repositories/Configuration/Indexes/EventIndex.cs): selected scalar mappings, mapping aliases and analyzer definitions.
- [Foundatio.Repositories Index at a61d541](https://github.com/FoundatioFx/Foundatio.Repositories/blob/a61d541613528428cc00bc2236602db72948a924/src/Foundatio.Repositories.Elasticsearch/Configuration/Index.cs): live mapping resolution, field resolution and nested-visitor composition.

The source commits are provenance, not NuGet version requirements or a claim that these are the deployed commits. The profile must be reviewed when the consuming configuration changes.

## Configuration covered

| Dimension | Version 1 profile |
| --- | --- |
| Application-derived fields | Exact keyword identifiers; standard-analyzed `message`; `source` with `standardplus` indexing and `whitespace_lower` search analysis; `tags` with `lowerkeyword`; integer `count`, double `value`, Boolean `is_first_occurrence`, and `date` |
| Application-derived mapping aliases | `organization`, `project`, `reference`, `tag`, `first` resolve to their mapped canonical fields |
| Default search fields | A **six-field projection**: `id`, `reference`, `reference_id`, `source`, `message`, `tags`. The other EventIndex defaults are not reproduced here |
| Supplemental configuration | `msg`/`who` field-map aliases, a `vip` include, correlated nested children, lowercase/ASCII-folding keyword normalization, `ignore_above`, `null_value`, and `date_nanos` |
| Parser configuration | A disposable resolver reads the actual test index mappings; the configured field map and includes are applied; `UseNested()` is enabled |
| Operators and execution context | Unset library default, explicit AND and explicit OR; filter and scoring modes |
| Input policy exercised | Leading wildcard operators disabled through `QueryValidationOptions`; literal escaped/quoted characters remain allowed |
| Engine matrix | Elasticsearch 8.19.0 and 9.5.0; exact server/Lucene versions are recorded in each artifact |
| Index topology | Isolated indexes, one primary shard, zero replicas; deterministic documents, explicit refresh and complete-result assertions |

The application uses distinct indexing and search analysis for `source`. Replacing both with `standard` would invalidate this profile. Analyzer tests assert the actual token sets, including punctuation-preserving source tokens and an unsplit, lowercased `VIP Member` tag. A separate keyword-normalizer control verifies that case/diacritic behavior follows the mapping rather than being implemented by a blanket lowercase operation in the parser.

Unknown fields, arbitrary mappings and custom visitors are **not rejected merely because they are outside this profile**. This PR adds executable acceptance tests, not a general-purpose allowlist or a new public validation API. Applications must separately enforce their field, operation and cost policy.

## Behavior classification

| Classification | Contract and evidence |
| --- | --- |
| Required correctness | Required `+` clauses cannot admit documents lacking the required clause. Under OR, optional clauses may raise scores without broadening the required set. AND behavior and group scope remain explicit |
| Required correctness | Escaped `reference:john\*` matches the literal-star document, not `john` or `johnny`. Prefix, single-character wildcard, regex, fuzzy and phrase-slop controls have independent expected IDs |
| Mapping-dependent support | Text analysis, exact keyword matching, configured normalization and aliases determine membership. `tag:/vip/` operates on indexed terms; it does not analyze an uppercase regex into lowercase |
| Required correctness | A grouped nested query must match within one child record. `alice` in one child plus `approved` in a different child must not satisfy the same-child conjunction |
| Intentional nested semantics | Inner NOT excludes matching children; outer NOT excludes parents having a matching child. These are different predicates, including for empty/missing child arrays |
| Intentional default difference | Foundatio's unset operator is AND; a bare reference parser's default must not be assumed. Explicitly select the operator when importing saved queries |
| Intentional Boolean difference | The contract retains Foundatio's Boolean-complement meaning of `A OR NOT B` and pure-negative complement behavior. It does not silently replace them with Lucene's prohibited-clause semantics |
| Intentional extension | Configured includes, `_missing_`, bracketed `..` ranges, and date-range time-zone carets are library constructs with explicit migration paths |
| Intentional date semantics | A mapped date/date_nanos range caret supplies `time_zone`, not a numeric scoring boost. Use an explicit native date-range boost when one is needed; a numeric date caret is not supported boost syntax |
| Intentional scoring boundary | Filter-mode hits have zero scores. Scoring controls compare native and reference queries on the same index; mixed-field/nested aggregation of scores is not promised to equal query_string or a separately constructed Lucene index |
| Rejected input exercised | Leading wildcard operators under the configured policy, escaped dots, post-colon ranges, unsupported edit distances, fractional phrase slop and negative boosts must fail before search |
| Outside portable contract | Unparenthesized mixed Boolean expressions, arbitrary analyzers, arbitrary visitor chains, query_string nested behavior, and cross-version/distributed absolute-score identity |

These classifications are chosen contract requirements, not evidence that all syntax differences are desirable. New disagreements require a minimal reproduction, root-cause classification, an explicit decision and a regression test. Do not turn a correctness defect into an "intentional difference" merely to pass CI.

Elasticsearch [query_string](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query), [nested queries](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-nested-query), [Boolean queries](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-bool-query), and [range queries](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-range-query) define the external reference behavior. The separate [Syntax Compatibility](./syntax-compatibility) guide and Java corpus retain the bare-Lucene distinction. A query_string request is not a valid reference implementation for nested document traversal.

## Date boundaries and time-zone migration

The boundary corpus uses separate records immediately before, at, within and at the exclusive end of each interval. Both date mappings are exercised. Chicago's March 10, 2024 day maps to `[2024-03-10T06:00:00Z, 2024-03-11T05:00:00Z)`; November 3 maps to `[2024-11-03T05:00:00Z, 2024-11-04T06:00:00Z)`. The fall corpus includes both occurrences of local 01:30.

The native reference queries contain explicit UTC bounds, independently of the parser's caret extension. Inclusive date-only upper-bound rounding is checked separately from half-open timestamp ranges. Leap day, year rollover, fixed-anchor date math, and adjacent one-nanosecond records have independent assertions. Timestamp strings are retained without converting through a millisecond- or tick-limited application type.

For an external query_string consumer, remove the caret time-zone suffix and set `query_string.time_zone`. For a native DSL consumer, supply the time zone on the date range or use the explicit UTC bounds. Do not replace a local calendar day with a fixed 24-hour interval. Wall-clock `now`, arbitrary historical time zones, invalid local times and application-specific date preprocessing need additional fixtures; the current corpus does not certify them.

## Migration contract

`production-migrations.tsv` executes both sides and asserts an independent expected document set, rather than accepting two equally wrong answers.

| Existing form or configuration | Migration |
| --- | --- |
| `count:[1 .. 5]` | `count:[1 TO 5]`; retain bracket/brace inclusivity |
| `_missing_:tags` | `NOT _exists_:tags` for Elasticsearch; this is not bare-Lucene existence syntax |
| `@include:vip` | Expand its configured expression with scope/operators preserved before forwarding to another consumer |
| Mapping/field-map aliases | Resolve the configured alias to the canonical mapped field; nested paths must retain nested scope |
| Legacy `message:-alpha` | `-message:alpha`; the corpus records migration while the legacy term form remains accepted. The future #272 grammar decision must update legacy rejection tests and migration guidance together |
| Implicit operators | Choose AND/OR explicitly. Rewriting every implicit expression to one operator changes intent |
| Mixed Boolean expressions | Require the caller to specify grouping. `A OR (B AND C)` and `(A OR B) AND C` are not interchangeable; there is no automatic semantics-preserving blanket rewrite |
| Date-range time-zone caret | Use the consumer's time-zone option or equivalent explicit UTC bounds; do not interpret it as a boost |

The #290/#291 runtime fixes change results for previously ignored required clauses and modifiers, and change scores where boosts were ignored. **Do not preserve those incorrect outputs for backward compatibility without a deliberate product decision.** Before deploying, replay the application's saved-query corpus, identify affected users/configurations, publish the behavior changes, and choose the release/versioning strategy. These tests do not invent a legacy-mode flag or migrate stored queries automatically.

## Application-enforced filters and resource limits

The suite composes organization/project constraints as caller-owned outer DSL filters, outside the parsed user expression. Adversarial OR, NOT and required-clause examples must not escape those filters. This checks query composition, **not the application's authorization system**: the test does not verify tenant resolution, request authentication, permissions, alternate endpoints or custom visitor implementations.

Leading-wildcard rejection is one tested policy, not a complete search-cost budget. Regex/fuzzy expansion, request length, clause/depth limits, execution timeouts and application cancellation must be bounded according to the deployed workload. Do not enable every accepted operation because a small single-shard fixture passes. Performance, relevance quality under real corpus statistics, shard distribution and ingestion behavior remain separate release gates.

## Reproduction and evidence

From the repository root, with the selected Elasticsearch server available at `ELASTICSEARCH_URL` (default `http://localhost:9200`):

```bash
dotnet build Foundatio.Parsers.slnx --configuration Release
mkdir -p contract-results
dotnet tests/Foundatio.Parsers.ElasticQueries.Tests/bin/Release/net10.0/Foundatio.Parsers.ElasticQueries.Tests.dll \
  -class '*ProductionQueryContractTests' -failSkips \
  -xml contract-results/contract.xml
python3 tests/compatibility/verify_production_contract.py \
  contract-results/contract.xml tests/compatibility
python3 -m unittest discover -s tests/compatibility \
  -p test_verify_production_contract.py -v
```

The dedicated **Production query contract** workflow runs both pinned servers, preserves exit status through pipelines, and uploads inputs, their hashes, revision/tree, server versions, XML, query diagnostics and formatting results. The report verifier requires every matching/migration case in all three operator configurations and both scoring modes, and every date case in both scoring modes. It rejects missing/duplicate case configurations, invalid expected document IDs, wrong test classes, skips, assembly errors and inconsistent counts. Its own mutation tests include equal-count substitution, so a passing total alone is insufficient.

The current inventory is 45 matching rows × 6 configurations, 8 migration rows × 6, 11 date rows × 2, and 36 additional test cases: **376 xUnit executions per server**. Some additional cases internally exercise both contexts. Counts are execution inventory, not 376 distinct syntax features and not an exhaustive compatibility claim.

## Release and maintenance gate

Merge in order: **#270, #290, #291, #292**. After each merge, retarget/synchronize descendants and rerun on the resulting head. A previous green head is not evidence for a new tree. #277/#282 are independent sibling changes; their eventual combined integration and overlapping documentation need review, but they are not silently merged by this contract PR.

For application deployment, replace or extend the projected profile with the exact deployed index settings, complete default fields, aliases, global/query-specific visitors and representative indexed documents. This profile omits EventFieldsQueryVisitor, stack filtering, the complete data dictionary/error/user mappings, dynamic templates, multifields, geo queries, ingestion pipelines and application saved queries. Their absence is an explicit qualification gap, not a claim that they are unsupported by the library.

When behavior intentionally changes, update the implementation, independently justified expectations, classification and migration instructions together. Keep the profile version and pinned sources reviewable. Preserve failing evidence and track runtime defects in the appropriate prerequisite; never skip or relabel a broken contract case to make a release appear ready.
