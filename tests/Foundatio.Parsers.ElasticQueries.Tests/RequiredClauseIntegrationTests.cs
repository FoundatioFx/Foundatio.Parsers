using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class RequiredClauseIntegrationTests : ElasticsearchTestBase<RequiredClauseFixture>
{
    private readonly RequiredClauseFixture _fixture;
    private readonly ITestOutputHelper _output;

    public RequiredClauseIntegrationTests(ITestOutputHelper output, RequiredClauseFixture fixture) : base(output, fixture)
    {
        _fixture = fixture;
        _output = output;
    }

    public static IEnumerable<object[]> Cases()
    {
        (string Query, string Or, string And)[] cases =
        [
            ("+tags:a tags:b", "1,3,5,7", "3,7"),
            ("tags:a +tags:b", "2,3,6,7", "3,7"),
            ("tags:a tags:b +tags:c", "4,5,6,7", "7"),
            ("+tags:a +tags:b tags:c", "3,7", "7"),
            ("+tags:a tags:b tags:c", "1,3,5,7", "7"),
            ("+tags:a -tags:b tags:c", "1,5", "5"),
            ("-tags:b tags:c +tags:a", "1,5", "5"),
            ("+tags:a NOT tags:b", "1,5", "1,5"),
            ("+tags:a !tags:b", "1,5", "1,5"),
            ("+tags:a NOT +tags:b", "1,5", "1,5"),
            ("+(tags:a OR tags:b)", "1,2,3,5,6,7", "1,2,3,5,6,7"),
            ("+(tags:a OR tags:b) tags:c", "1,2,3,5,6,7", "5,6,7"),
            ("+(tags:a tags:b) tags:c", "1,2,3,5,6,7", "7"),
            ("+tags:a (tags:b OR tags:c)", "1,3,5,7", "3,5,7"),
            ("(+tags:a tags:b) OR tags:c", "1,3,4,5,6,7", "3,4,5,6,7"),
            ("+tags:a OR tags:b", "1,3,5,7", "1,3,5,7"),
            ("tags:a OR +tags:b OR tags:c", "2,3,6,7", "2,3,6,7"),
            ("+a b", "1,3,5,7", "3,7"),
            ("+alias:a tags:b", "1,3,5,7", "3,7"),
            ("+@include:a tags:b", "1,3,5,7", "3,7"),
            ("NOT @include:a", "0,2,4,6", "0,2,4,6"),
            ("-@include:a", "0,2,4,6", "0,2,4,6"),
            ("NOT @include:not-a", "1,3,5,7", "1,3,5,7"),
            ("+_exists_:tags tags:a", "1,2,3,4,5,6,7", "1,3,5,7"),
            ("+children.name:a children.value:y", "1,3,5,7", "3,7"),
            ("children.value:y +children.name:a", "1,3,5,7", "3,7"),
            ("+children.name:a tags:c", "1,3,5,7", "5,7"),
            ("+tags:c children.name:a", "4,5,6,7", "5,7"),
            ("+children.name:a -children.value:y", "5", "5"),
            ("+children:(children.name:a OR children.name:b) tags:c", "1,2,3,5,6,7", "5,6,7"),
            ("children:(+children.name:a children.value:y)", "1,3,5,7", "3,7")
        ];

        foreach (var (query, or, and) in cases)
            foreach (bool scoring in new[] { false, true })
            {
                yield return [query, GroupOperator.Or, scoring, or];
                yield return [query, GroupOperator.And, scoring, and];
            }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task BuildQueryAsync_WithRequiredClauses_PreservesDocumentContract(
        string text, GroupOperator defaultOperator, bool scoring, string expected)
    {
        using var resolver = new ElasticMappingResolver(() => RequiredClauseFixture.Mapping);
        var parser = CreateParser(resolver);
        var query = await parser.BuildQueryAsync(text, new ElasticQueryVisitorContext
        {
            DefaultOperator = defaultOperator,
            UseScoring = scoring
        });
        var response = await SearchAsync(query);
        AssertComplete(response);
        string actual = String.Join(',', response.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
        Assert.True(expected == actual, $"{text}; default={defaultOperator}; scoring={scoring}; expected={expected}; actual={actual}\n{response.DebugInformation}");
        if (!scoring)
            Assert.All(response.Hits, hit => Assert.Equal(0, hit.Score));
    }

    [Fact]
    public async Task BuildQueryAsync_WithOptionalClause_ContributesScoreWithoutAdmittingOtherDocuments()
    {
        using var resolver = new ElasticMappingResolver(() => RequiredClauseFixture.Mapping);
        var parser = CreateParser(resolver);
        var context = new ElasticQueryVisitorContext { DefaultOperator = GroupOperator.Or, UseScoring = true };
        var native = await SearchAsync(await parser.BuildQueryAsync("+tags:a tags:b", context));
        var reference = await SearchAsync(new QueryStringQuery("+tags:a tags:b") { DefaultOperator = Operator.Or });
        AssertComplete(native);
        AssertComplete(reference);
        Assert.Equal(new[] { "1", "3", "5", "7" }, native.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
        var nativeScores = native.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        var referenceScores = reference.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        Assert.Equal(nativeScores.Keys.Order(StringComparer.Ordinal), referenceScores.Keys.Order(StringComparer.Ordinal));
        foreach (string id in nativeScores.Keys)
            Assert.True(Math.Abs(nativeScores[id] - referenceScores[id]) <= 0.00001 * Math.Max(1, Math.Abs(referenceScores[id])));
        Assert.True(nativeScores["3"] > nativeScores["1"]);
    }

    private static ElasticQueryParser CreateParser(ElasticMappingResolver resolver) => new(configuration => configuration
        .UseMappings(resolver)
        .SetDefaultFields(["tags"])
        .UseFieldResolver((field, _) => Task.FromResult<string?>(field == "alias" ? "tags" : null))
        .UseIncludes(new Dictionary<string, string> { { "a", "tags:a" }, { "not-a", "NOT tags:a" } })
        .UseNested());

    private Task<SearchResponse<RequiredClauseFixture.Document>> SearchAsync(Query query) => Client.SearchAsync<RequiredClauseFixture.Document>(descriptor => descriptor
        .Indices(_fixture.Index)
        .Query(query)
        .Size(100)
        .TrackTotalHits(true)
        .AllowPartialSearchResults(false), TestCancellationToken);

    private void AssertComplete(SearchResponse<RequiredClauseFixture.Document> response)
    {
        Assert.True(response.IsValidResponse, response.DebugInformation);
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        Assert.All(response.Hits, hit => Assert.True(hit.Score.HasValue && Double.IsFinite(hit.Score.Value)));
        _output.WriteLine(response.DebugInformation);
    }
}
