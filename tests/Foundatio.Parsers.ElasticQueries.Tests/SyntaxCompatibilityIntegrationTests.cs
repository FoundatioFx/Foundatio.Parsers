using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class SyntaxCompatibilityIntegrationTests : ElasticsearchTestBase<SyntaxCompatibilityFixture>
{
    private readonly SyntaxCompatibilityFixture _fixture;

    public SyntaxCompatibilityIntegrationTests(ITestOutputHelper output, SyntaxCompatibilityFixture fixture) : base(output, fixture)
    {
        _fixture = fixture;
    }

    public static IEnumerable<TheoryDataRow<string, string, GroupOperator?, string?, string?, bool>> MatchingCases()
    {
        // Null means query rejection; an empty string means a successful search with no matches.
        (string Id, string Query, GroupOperator? Operator, string? Native, string? External)[] cases =
        [
            ("text-term", "text:alpha", GroupOperator.Or, "a,b,l", "a,b,l"),
            ("text-case", "text:ALPHA", GroupOperator.Or, "a,b,l", "a,b,l"),
            ("keyword-term", "keyword:john", GroupOperator.Or, "a", "a"),
            ("keyword-case", "keyword:JOHN", GroupOperator.Or, "", ""),
            ("keyword-uppercase", "keyword:ALPHA", GroupOperator.Or, "l", "l"),
            ("phrase", "text:\"alpha beta\"", GroupOperator.Or, "a", "a"),
            ("and", "text:alpha AND text:beta", GroupOperator.Or, "a,b", "a,b"),
            ("or", "text:alpha OR text:beta", GroupOperator.Or, "a,b,c,l", "a,b,c,l"),
            ("implicit-default", "alpha beta", null, "a,b", "a,b,c,l"),
            ("implicit-and", "alpha beta", GroupOperator.And, "a,b", "a,b"),
            ("implicit-or", "alpha beta", GroupOperator.Or, "a,b,c,l", "a,b,c,l"),
            ("required-term", "+text:alpha text:gamma", GroupOperator.Or, "a,b,c,l", "a,b,l"),
            ("negative-only", "NOT text:alpha", GroupOperator.Or, "c,d,e,f,g,h,i,j,k", "c,d,e,f,g,h,i,j,k"),
            ("negative-minus", "-text:alpha", GroupOperator.Or, "c,d,e,f,g,h,i,j,k", "c,d,e,f,g,h,i,j,k"),
            ("negative-bang", "!text:alpha", GroupOperator.Or, "c,d,e,f,g,h,i,j,k", "c,d,e,f,g,h,i,j,k"),
            ("spaced-bang", "! text:alpha", GroupOperator.Or, "a,b,l", "a,b,l"),
            ("and-not", "text:beta AND NOT text:alpha", GroupOperator.Or, "c", "c"),
            ("or-not", "text:alpha OR NOT text:beta", GroupOperator.Or, "a,b,d,e,f,g,h,i,j,k,l", "l"),
            ("mixed-operators", "text:alpha OR text:beta AND text:gamma", GroupOperator.Or, "a,b,c,l", "b,c"),
            ("parenthesized-and", "(text:alpha OR text:beta) AND text:gamma", GroupOperator.Or, "b,c", "b,c"),
            ("parenthesized-or", "text:alpha OR (text:beta AND text:gamma)", GroupOperator.Or, "a,b,c,l", "a,b,c,l"),
            ("field-group", "text:(alpha OR beta)", GroupOperator.Or, "a,b,c,l", "a,b,c,l"),
            ("negative-group", "-text:(alpha OR beta)", GroupOperator.Or, "d,e,f,g,h,i,j,k", "d,e,f,g,h,i,j,k"),
            ("post-colon-minus", "text:-alpha", GroupOperator.Or, "c,d,e,f,g,h,i,j,k", null),
            ("post-colon-not", "text:NOT alpha", GroupOperator.Or, "c,d,e,f,g,h,i,j,k", null),
            ("post-colon-range", "number:-[1 TO 5]", GroupOperator.Or, null, null),
            ("bare-dots", "keyword:1..5", GroupOperator.Or, "e", "e"),
            ("dot-range", "number:[1 .. 5]", GroupOperator.Or, "a,b,c,f,g", null),
            ("dot-range-compact", "number:[1..5]", GroupOperator.Or, "a,b,c,f,g", null),
            ("escaped-dot", "field\\.with\\.dots:value", GroupOperator.Or, null, ""),
            ("wildcard-question", "keyword:jo?n", GroupOperator.Or, "c", "a,b,c"),
            ("wildcard-middle", "keyword:jo*n", GroupOperator.Or, "", "a,b,c"),
            ("wildcard-leading", "keyword:*john", GroupOperator.Or, "", "a"),
            ("wildcard-prefix", "keyword:john*", GroupOperator.Or, "a,d,j", "a,d,j"),
            ("wildcard-mixed", "keyword:jo?n*", GroupOperator.Or, "c,k", "a,b,c,d,j,k"),
            ("wildcard-escaped", "keyword:john\\*", GroupOperator.Or, "a,d,j", "j"),
            ("wildcard-text", "text:alp*", GroupOperator.Or, "a,b,d,l", "a,b,d,l"),
            ("regex-prefix", "keyword:/val.*/", GroupOperator.Or, "f", "f,g"),
            ("regex-nonprefix", "keyword:/[0-9]+/", GroupOperator.Or, "h", ""),
            ("fuzzy", "text:alphx~1", GroupOperator.Or, "", "a,b,l"),
            ("phrase-slop", "text:\"alpha beta\"~1", GroupOperator.Or, "a", "a,b"),
            ("boost-membership", "text:alpha^8", GroupOperator.Or, "a,b,l", "a,b,l"),
            ("exists", "_exists_:keyword", GroupOperator.Or, "a,b,c,d,e,f,g,h,j,k,l", "a,b,c,d,e,f,g,h,j,k,l"),
            ("missing", "_missing_:keyword", GroupOperator.Or, "i", ""),
            ("missing-migration", "NOT _exists_:keyword", GroupOperator.Or, "i", "i"),
            ("include", "@include:active", GroupOperator.Or, "a", ""),
            ("range-inclusive", "number:[1 TO 5]", GroupOperator.Or, "a,b,c,f,g", "a,b,c,f,g"),
            ("range-exclusive", "number:{1 TO 5}", GroupOperator.Or, "b,f,g", "b,f,g"),
            ("range-open-upper", "number:[1 TO 5}", GroupOperator.Or, "a,b,f,g", "a,b,f,g"),
            ("range-open-lower", "number:{1 TO 5]", GroupOperator.Or, "b,c,f,g", "b,c,f,g"),
            ("range-unbounded", "number:[1 TO *]", GroupOperator.Or, "a,b,c,d,e,f,g,h,i,j", "a,b,c,d,e,f,g,h,i,j"),
            ("comparison", "number:>5", GroupOperator.Or, "d,e,h,i,j", "d,e,h,i,j"),
            ("comparison-inclusive", "number:>=5", GroupOperator.Or, "c,d,e,h,i,j", "c,d,e,h,i,j"),
            ("comparison-less", "number:<1", GroupOperator.Or, "k,l", "k,l"),
            ("comparison-less-inclusive", "number:<=1", GroupOperator.Or, "a,k,l", "a,k,l"),
            ("date-utc", "date:[2024-01-01 TO 2024-01-01]", GroupOperator.Or, "a,b,c", "a,b,c"),
            ("date-timezone", "date:[2024-01-01 TO *]^\"America/Chicago\"", GroupOperator.Or, "c,d,g,h,j,k,l", null),
            ("date-nanos-timezone", "dateNanos:[2024-01-01 TO *]^\"America/Chicago\"", GroupOperator.Or, "c,d,g,h,j,k,l", null),
            ("date-numeric-caret", "date:[2024-01-01 TO *]^2", GroupOperator.Or, null, "a,b,c,d,g,h,j,k,l"),
        ];

        foreach (var testCase in cases)
            foreach (bool scoring in new[] { false, true })
                yield return new(testCase.Id, testCase.Query, testCase.Operator, testCase.Native, testCase.External, scoring);
    }

    [Theory]
    [MemberData(nameof(MatchingCases))]
    public async Task BuildQueryAsync_WithCorpusCase_MatchesIndependentDocumentExpectations(
        string id, string text, GroupOperator? defaultOperator, string? expectedNative, string? expectedExternal, bool scoring)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        var context = new ElasticQueryVisitorContext { UseScoring = scoring };
        if (defaultOperator.HasValue)
            context.DefaultOperator = defaultOperator.Value;

        var reference = new QueryStringQuery
        {
            Query = text,
            DefaultField = "text",
            AnalyzeWildcard = true,
            AllowLeadingWildcard = true
        };
        if (defaultOperator.HasValue)
            reference.DefaultOperator = defaultOperator == GroupOperator.And ? Operator.And : Operator.Or;

        Query referenceQuery = scoring ? reference : new BoolQuery { Filter = [reference] };

        // Act
        Query? query = null;
        try
        {
            query = await parser.BuildQueryAsync(text, context);
            Assert.NotNull(query);
        }
        catch (QueryValidationException exception) when (expectedNative is null)
        {
            // BuildQueryAsync reports grammar failures through its public validation API.
            // Do not turn transport or result-deserialization exceptions into expected rejection.
            _logger.LogInformation("Foundatio query rejection: {Message}", exception.Message);
        }

        string? native = query is null ? null : await GetMatchesAsync(query);
        string? external = await GetMatchesAsync(referenceQuery);

        _logger.LogInformation("{Case}: {Query}; scoring={Scoring}; Foundatio={Native}; query_string={External}",
            id, text, scoring, native, external);

        // Assert
        Assert.Equal(expectedNative, native);
        Assert.Equal(expectedExternal, external);
    }

    [Theory]
    [InlineData("text:alpha")]
    [InlineData("text:\"alpha beta\"")]
    [InlineData("text:alpha AND text:beta")]
    [InlineData("text:alpha OR text:gamma")]
    public async Task BuildQueryAsync_WithEquivalentScoringQuery_PreservesScores(string text)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);

        // Act
        var native = await GetScoresAsync(await parser.BuildQueryAsync(text, ScoringContext()));
        var external = await GetScoresAsync(ReferenceQuery(text));

        // Assert
        Assert.Equal(external.Keys.Order(StringComparer.Ordinal), native.Keys.Order(StringComparer.Ordinal));
        Assert.NotEmpty(native);

        foreach (string id in native.Keys)
            AssertClose(external[id], native[id]);
    }

    [Theory]
    [InlineData("text:alpha", "text:alpha^8")]
    [InlineData("text:\"alpha beta\"", "text:\"alpha beta\"^8")]
    [InlineData("(text:alpha OR text:gamma)", "(text:alpha OR text:gamma)^8")]
    public async Task BuildQueryAsync_WithBoost_CharacterizesMissingBoostAndReferenceMultiplier(string baseline, string boosted)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);

        // Act
        var native = await GetScoresAsync(await parser.BuildQueryAsync(baseline, ScoringContext()));
        var nativeBoosted = await GetScoresAsync(await parser.BuildQueryAsync(boosted, ScoringContext()));
        var external = await GetScoresAsync(ReferenceQuery(baseline));
        var externalBoosted = await GetScoresAsync(ReferenceQuery(boosted));

        // Assert
        Assert.NotEmpty(native);
        Assert.Equal(native.Keys.Order(StringComparer.Ordinal), external.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(native.Keys.Order(StringComparer.Ordinal), nativeBoosted.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(external.Keys.Order(StringComparer.Ordinal), externalBoosted.Keys.Order(StringComparer.Ordinal));

        foreach (string id in native.Keys)
            AssertClose(native[id], nativeBoosted[id]);

        foreach (string id in external.Keys)
            AssertClose(external[id] * 8, externalBoosted[id]);
    }

    [Fact]
    public async Task BuildQueryAsync_WithBoostedDisjunction_ChangesReferenceRankingButNotNativeRanking()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        const string query = "text:alpha^8 OR text:gamma";

        // Act
        var native = await GetScoresAsync(await parser.BuildQueryAsync(query, ScoringContext()));
        var external = await GetScoresAsync(ReferenceQuery(query));

        // Assert
        Assert.True(native["c"] > native["a"], "Unboosted gamma should outrank alpha in equal-length documents.");
        Assert.True(external["a"] > external["c"], "The reference boost must reverse that document ranking.");
    }

    [Fact]
    public async Task BuildQueryAsync_WithFilterContext_PreservesMatchesAndHasZeroScores()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        const string text = "text:alpha OR text:gamma";

        // Act
        var query = await parser.BuildQueryAsync(text, new ElasticQueryVisitorContext { DefaultOperator = GroupOperator.Or });
        var native = await GetScoresAsync(query);
        var external = await GetScoresAsync(new BoolQuery { Filter = [ReferenceQuery(text)] });

        // Assert
        Assert.Equal(new[] { "a", "b", "c", "l" }, native.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(external.Keys.Order(StringComparer.Ordinal), native.Keys.Order(StringComparer.Ordinal));
        Assert.All(native.Values, score => Assert.Equal(0, score));
        Assert.All(external.Values, score => Assert.Equal(0, score));
    }

    [Theory]
    [InlineData("date")]
    [InlineData("dateNanos")]
    public async Task BuildQueryAsync_WithTimeZoneMigration_PreservesBoundaryDocuments(string field)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        string range = $"{field}:[2024-01-01 TO *]";

        // Act
        var native = await GetMatchesAsync(await parser.BuildQueryAsync(range + "^\"America/Chicago\"", ScoringContext()));
        var external = await GetMatchesAsync(new QueryStringQuery { Query = range, TimeZone = "America/Chicago" });

        // Assert
        Assert.Equal("c,d,g,h,j,k,l", native);
        Assert.Equal(native, external);
    }

    private ElasticQueryParser CreateParser(ElasticMappingResolver resolver) => new(configuration => configuration
        .SetLoggerFactory(Log)
        .UseMappings(resolver)
        .SetDefaultFields(["text"])
        .UseIncludes(new Dictionary<string, string> { { "active", "keyword:john" } }));

    private static ElasticQueryVisitorContext ScoringContext() => new() { UseScoring = true, DefaultOperator = GroupOperator.Or };

    private static Query ReferenceQuery(string text) => new QueryStringQuery
    {
        Query = text,
        DefaultField = "text",
        DefaultOperator = Operator.Or,
        AnalyzeWildcard = true,
        AllowLeadingWildcard = true
    };

    private async Task<string?> GetMatchesAsync(Query query)
    {
        var response = await SearchAsync(query);
        if (!response.IsValidResponse)
        {
            Assert.Equal(400, response.ApiCallDetails.HttpStatusCode);

            _logger.LogInformation("Elasticsearch rejected query: {DebugInformation}", response.DebugInformation);
            return null;
        }

        AssertComplete(response);

        return String.Join(',', response.Documents.Select(document => document.Id).Order(StringComparer.Ordinal));
    }

    private async Task<Dictionary<string, double>> GetScoresAsync(Query query)
    {
        var response = await SearchAsync(query);
        Assert.True(response.IsValidResponse, response.DebugInformation);
        AssertComplete(response);

        var scores = response.Hits.ToDictionary(hit => hit.Source!.Id, hit => hit.Score ?? Double.NaN, StringComparer.Ordinal);
        Assert.All(scores.Values, score => Assert.True(Double.IsFinite(score) && score >= 0));

        _logger.LogInformation("Scores: {Scores}", String.Join(", ", scores.OrderByDescending(pair => pair.Value).Select(pair => $"{pair.Key}={pair.Value:R}")));
        return scores;
    }

    private Task<SearchResponse<SyntaxCompatibilityFixture.CompatibilityDocument>> SearchAsync(Query query) => Client.SearchAsync<SyntaxCompatibilityFixture.CompatibilityDocument>(descriptor => descriptor
        .Indices(_fixture.Index)
        .Query(query)
        .Size(100)
        .TrackTotalHits(true)
        .AllowPartialSearchResults(false), TestCancellationToken);

    private static void AssertComplete(SearchResponse<SyntaxCompatibilityFixture.CompatibilityDocument> response)
    {
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        Assert.All(response.Hits, hit => Assert.NotNull(hit.Source));
    }

    private static void AssertClose(double expected, double actual)
    {
        double tolerance = 0.00001 * Math.Max(1, Math.Abs(expected));
        Assert.InRange(actual, expected - tolerance, expected + tolerance);
    }
}
