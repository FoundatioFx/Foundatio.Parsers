# Live syntax compatibility corpus

This suite distinguishes AST acceptance, emitted Elasticsearch queries, document membership, and scoring. It records both agreement and known differences; a passing characterization test is not proof that the engines are interchangeable.

## Inputs and configuration

`documents.tsv` contains twelve fixed documents. `cases.tsv` contains 59 cases with seven tab-separated columns: case ID, query, default-operator profile, expected Foundatio IDs, expected Elasticsearch `query_string` IDs, expected bare-Lucene IDs, and rationale. IDs are sorted ordinally. `-` means no matches, `ERROR` means a query rejection, and `N/A` identifies mapping-specific cases that bare Lucene classic cannot meaningfully execute.

The C# fixture uses a unique, explicitly scoped index, one primary shard, zero replicas, strict mappings, the standard analyzer for text fields, exact keyword fields, an integer field, and date/date_nanos fields. A refresh makes all documents visible before tests start. Search assertions reject timeouts, partial results, truncation, and unexpected transport/service failures. Grammar errors from the public builder are recognized as `QueryValidationException`, not confused with Elasticsearch HTTP query rejection.

`DEFAULT` leaves the C# and Elasticsearch default operator unset; `AND` and `OR` set it explicitly. The Java classic parser uses its OR profile unless AND is specified. All consumers use `text` as the default field. The Elasticsearch reference explicitly enables wildcard analysis and leading wildcards, and the Java reference allows leading wildcards. This makes leading-wildcard cases intentional comparisons, not claims that Foundatio uses the same options. Foundatio's default analyzed trailing-star query still disables leading wildcards. The include resolver expands `@include:active` to `keyword:john` only on the Foundatio side.

## Checks

- `SyntaxCompatibilityTests`: 47 in-memory AST/serialized-query cases; no live server needed.
- `SyntaxCompatibilityIntegrationTests`: 59 document-set cases in each of filter and scoring modes, plus eleven controls for same-index score equivalence, term/phrase/group boost multipliers, ranking changes, zero filter scores, and date/date_nanos time-zone migration boundaries. Total: 129 live C# cases per Elasticsearch version.
- `LuceneCompatibility.java`: 46 applicable document-set cases plus three boost multiplier controls and one ranking control, using each tested server's actual bundled Lucene jars. Thirteen numeric/date rows are explicitly excluded from bare-classic execution because that parser has no Elasticsearch mapping layer. Numeric fields are not replaced with misleading lexical fields.

The expected document columns are independent oracles. Some columns deliberately differ: required clauses (#288), Boolean composition, pure-negative queries, regex/fuzziness/slop/boosts (#278), wildcard forms, and library extensions. These rows must remain visible. Do not copy a failing engine's output into the expected column merely to make CI pass.

The C# score-equivalence controls compare Foundatio with `query_string` on the same Elasticsearch index. The Java runner checks corresponding boost/ranking invariants in its own index and logs scores; it does not assert absolute score equality with Elasticsearch. The corpus controls mappings and statistics but does not pin similarity implementations or all possible rewrite paths.

## Reproduce

Start a disposable Elasticsearch instance on port 9200, then from the repository root:

```sh
dotnet build Foundatio.Parsers.slnx --configuration Release
mkdir -p compatibility-results
dotnet tests/Foundatio.Parsers.ElasticQueries.Tests/bin/Release/net10.0/Foundatio.Parsers.ElasticQueries.Tests.dll \
  -class Foundatio.Parsers.ElasticQueries.Tests.SyntaxCompatibilityIntegrationTests \
  -failSkips -xml "$PWD/compatibility-results/foundatio.xml"
```

The test assembly uses the native xUnit command-line mode when launched directly, so these are single-hyphen xUnit options. The solution build is intentional: repository build properties depend on the solution context. Do not add sibling Foundatio project references or suppress build warnings to work around a project-only invocation.

For bare Lucene, set `ES_CONTAINER` to that disposable container's ID and run:

```sh
docker cp tests/compatibility "$ES_CONTAINER:/tmp/compatibility"
docker exec "$ES_CONTAINER" /usr/share/elasticsearch/jdk/bin/java \
  --class-path '/usr/share/elasticsearch/lib/*:/usr/share/elasticsearch/modules/analysis-common/*' \
  /tmp/compatibility/LuceneCompatibility.java /tmp/compatibility
```

`.github/workflows/syntax-compatibility.yml` runs the same tests against pinned Elasticsearch 8.19.0 and 9.5.0 images. It records server/Lucene versions and uploads the Lucene log, native xUnit XML/output, and scoped formatter diagnostics. Shell pipeline failures propagate. A report-completeness step verifies that all expected live C# cases ran and passed, preventing an empty or partial filtered run from appearing green. The workflow has read-only repository permissions and does not publish packages or deploy documentation.

## Maintenance and release decisions

When adding a case, choose documents that distinguish the intended behavior from a plausible wrong result. Keep positive and negative controls. For ranking changes, assert score relationships or ordering rather than only IDs. When adding a separate C# control, update the workflow's expected control count as well. Keep both language readers and their column contract aligned.

A runtime fix must update the affected expected results, the guides, and any migration/release notes together. Preserving a known-bad expectation forever is not the goal. Required-clause and ignored-modifier defects remain blockers for applications relying on those features until fixed or explicitly rejected at the application input boundary.

This finite corpus does not cover every expression, analyzer, mapping, alias, nested visitor, distributed-scoring configuration, date-math/DST transition, engine version, or performance/security boundary. Production acceptance requires a defined supported input/configuration contract and application-specific regression cases. Do not describe this workflow as exhaustive universal compatibility.
