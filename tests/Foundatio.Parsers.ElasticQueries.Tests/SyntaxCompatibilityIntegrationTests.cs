using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class SyntaxCompatibilityIntegrationTests : ElasticsearchTestBase<SyntaxCompatibilityFixture>
{
    private readonly SyntaxCompatibilityFixture _fixture;
    private readonly ITestOutputHelper _output;

    public SyntaxCompatibilityIntegrationTests(ITestOutputHelper output, SyntaxCompatibilityFixture fixture) : base(output, fixture)
    {
        _fixture = fixture;
        _output = output;
    }

    public static IEnumerable<object[]> MatchingCases()
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (string line in File.ReadLines(SyntaxCompatibilityFixture.CorpusPath("cases.tsv")))
        {
            if (line.StartsWith('#') || String.IsNullOrWhiteSpace(line))
                continue;

            var values = line.Split('\t');
            if (values.Length != 7 || values.Any(String.IsNullOrWhiteSpace) || !ids.Add(values[0])
                || values[2] is not ("OR" or "AND" or "DEFAULT"))
                throw new InvalidDataException($"Invalid or duplicate compatibility case: {line}");

            foreach (bool scoring in new[] { false, true })
                yield return [values[0], values[1], values[2], values[3], values[4], scoring];
        }

        if (ids.Count == 0)
            throw new InvalidDataException("The compatibility corpus must not be empty.");
    }

    [Theory]
    [MemberData(nameof(MatchingCases))]
    public async Task BuildQueryAsync_WithCorpusCase_MatchesIndependentDocumentExpectations(
        string id, string text, string defaultOperator, string expectedNative, string expectedExternal, bool scoring)
    {
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        var context = new ElasticQueryVisitorContext { UseScoring = scoring };
        if (defaultOperator != "DEFAULT")
            context.DefaultOperator = defaultOperator == "OR" ? GroupOperator.Or : GroupOperator.And;

        Query? query = null;
        try
        {
            query = await parser.BuildQueryAsync(text, context);
            Assert.NotNull(query);
        }
        catch (QueryValidationException exception)
        {
            // BuildQueryAsync reports grammar failures through its public validation API.
            // Do not turn transport or result-deserialization exceptions into expected rejection.
            _output.WriteLine($"Foundatio query rejection: {exception.Message}");
        }

        string native = query is null ? "ERROR" : await GetMatchesAsync(query);
        var reference = new QueryStringQuery
        {
            Query = text,
            DefaultField = "text",
            AnalyzeWildcard = true,
            AllowLeadingWildcard = true
        };
        if (defaultOperator != "DEFAULT")
            reference.DefaultOperator = defaultOperator == "AND" ? Operator.And : Operator.Or;
        string external = await GetMatchesAsync(reference);

        _output.WriteLine($"{id}: {text}; scoring={scoring}; Foundatio={native}; query_string={external}");
        Assert.True(native == expectedNative && external == expectedExternal,
            $"{id}: {text}; scoring={scoring}; expected Foundatio={expectedNative}, query_string={expectedExternal}; actual Foundatio={native}, query_string={external}");
    }

    [Theory]
    [InlineData("text:alpha")]
    [InlineData("text:\"alpha beta\"")]
    [InlineData("text:alpha AND text:beta")]
    [InlineData("text:alpha OR text:gamma")]
    public async Task BuildQueryAsync_WithEquivalentScoringQuery_PreservesScores(string text)
    {
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        var native = await GetScoresAsync(await parser.BuildQueryAsync(text, ScoringContext()));
        var external = await GetScoresAsync(ReferenceQuery(text));

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
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        var native = await GetScoresAsync(await parser.BuildQueryAsync(baseline, ScoringContext()));
        var nativeBoosted = await GetScoresAsync(await parser.BuildQueryAsync(boosted, ScoringContext()));
        var external = await GetScoresAsync(ReferenceQuery(baseline));
        var externalBoosted = await GetScoresAsync(ReferenceQuery(boosted));

        Assert.NotEmpty(native);
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
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        const string query = "text:alpha^8 OR text:gamma";
        var native = await GetScoresAsync(await parser.BuildQueryAsync(query, ScoringContext()));
        var external = await GetScoresAsync(ReferenceQuery(query));

        Assert.True(native["c"] > native["a"], "Unboosted gamma should outrank alpha in equal-length documents.");
        Assert.True(external["a"] > external["c"], "The reference boost must reverse that document ranking.");
    }

    [Fact]
    public async Task BuildQueryAsync_WithFilterContext_PreservesMatchesAndHasZeroScores()
    {
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        const string text = "text:alpha OR text:gamma";
        var query = await parser.BuildQueryAsync(text, new ElasticQueryVisitorContext { DefaultOperator = GroupOperator.Or });
        var native = await GetScoresAsync(query);
        var external = await GetScoresAsync(new BoolQuery { Filter = [ReferenceQuery(text)] });

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
        using var resolver = new ElasticMappingResolver(() => SyntaxCompatibilityFixture.Mapping);
        var parser = CreateParser(resolver);
        string range = $"{field}:[2024-01-01 TO *]";
        var native = await GetMatchesAsync(await parser.BuildQueryAsync(range + "^\"America/Chicago\"", ScoringContext()));
        var external = await GetMatchesAsync(new QueryStringQuery { Query = range, TimeZone = "America/Chicago" });

        Assert.Equal("c,d,g,h,j,k,l", native);
        Assert.Equal(native, external);
    }

    private static ElasticQueryParser CreateParser(ElasticMappingResolver resolver) => new(configuration => configuration
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

    private async Task<string> GetMatchesAsync(Query query)
    {
        var response = await SearchAsync(query);
        if (!response.IsValidResponse)
        {
            Assert.Equal(400, response.ApiCallDetails.HttpStatusCode);
            _output.WriteLine($"Elasticsearch rejected query: {response.DebugInformation}");
            return "ERROR";
        }

        AssertComplete(response);
        string matches = String.Join(',', response.Documents.Select(document => document.Id).Order(StringComparer.Ordinal));
        return matches.Length == 0 ? "-" : matches;
    }

    private async Task<Dictionary<string, double>> GetScoresAsync(Query query)
    {
        var response = await SearchAsync(query);
        Assert.True(response.IsValidResponse, response.DebugInformation);
        AssertComplete(response);
        var scores = response.Hits.ToDictionary(hit => hit.Source!.Id, hit => hit.Score ?? Double.NaN, StringComparer.Ordinal);
        Assert.All(scores.Values, score => Assert.True(Double.IsFinite(score) && score >= 0));
        _output.WriteLine(String.Join(", ", scores.OrderByDescending(pair => pair.Value).Select(pair => $"{pair.Key}={pair.Value:R}")));
        return scores;
    }

    private Task<SearchResponse<SyntaxCompatibilityFixture.Document>> SearchAsync(Query query) => Client.SearchAsync<SyntaxCompatibilityFixture.Document>(descriptor => descriptor
        .Indices(_fixture.Index)
        .Query(query)
        .Size(100)
        .TrackTotalHits(true)
        .AllowPartialSearchResults(false), TestCancellationToken);

    private static void AssertComplete(SearchResponse<SyntaxCompatibilityFixture.Document> response)
    {
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        Assert.All(response.Hits, hit => Assert.NotNull(hit.Source));
    }

    private static void AssertClose(double expected, double actual) => Assert.True(
        Math.Abs(expected - actual) <= 0.00001 * Math.Max(1, Math.Abs(expected)),
        $"Expected score {expected:R}, actual {actual:R}");
}
