using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class RequiredClauseTruthTableTests : ElasticsearchTestBase<RequiredClauseFixture>
{
    private readonly RequiredClauseFixture _fixture;

    public RequiredClauseTruthTableTests(ITestOutputHelper output, RequiredClauseFixture fixture) : base(output, fixture)
    {
        _fixture = fixture;
    }

    public static IEnumerable<object[]> Cases()
    {
        int[][] orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
        string[] prefixes = ["", "+", "-", "NOT "];
        for (int combination = 0; combination < 64; combination++)
        {
            int[] modifiers = [combination % 4, combination / 4 % 4, combination / 16];
            if (!modifiers.Contains(1))
                continue;

            foreach (var order in orders)
            {
                string query = String.Join(' ', order.Select(field => prefixes[modifiers[field]] + "tags:" + (char)('a' + field)));
                foreach (var op in new[] { GroupOperator.And, GroupOperator.Or })
                {
                    // This oracle evaluates the eight documents as sets, independently of
                    // parser nodes, generated DSL, Elasticsearch and clause construction.
                    var ids = Enumerable.Range(0, 8).Where(document => Enumerable.Range(0, 3).All(field =>
                    {
                        bool present = (document & (1 << field)) != 0;
                        return modifiers[field] switch
                        {
                            1 => present,
                            2 or 3 => !present,
                            _ => op != GroupOperator.And || present
                        };
                    })).Select(id => id.ToString(CultureInfo.InvariantCulture));
                    string expected = String.Join(',', ids);
                    foreach (bool scoring in new[] { false, true })
                        yield return [query, op, scoring, expected];
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task BuildQueryAsync_WithEveryThreeTermRequiredCombination_PreservesTruthTable(
        string text, GroupOperator op, bool scoring, string expected)
    {
        using var resolver = new ElasticMappingResolver(() => RequiredClauseFixture.Mapping);
        var parser = new ElasticQueryParser(configuration => configuration.UseMappings(resolver));
        var native = await SearchAsync(await parser.BuildQueryAsync(text, new ElasticQueryVisitorContext { DefaultOperator = op, UseScoring = scoring }));
        var reference = await SearchAsync(new QueryStringQuery(text) { DefaultOperator = op == GroupOperator.And ? Operator.And : Operator.Or });
        string actual = String.Join(',', native.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
        string external = String.Join(',', reference.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
        Assert.True(actual == expected && external == expected,
            $"{text}; default={op}; scoring={scoring}; expected={expected}; native={actual}; query_string={external}");
        if (!scoring)
            Assert.All(native.Hits, hit => Assert.Equal(0, hit.Score));
    }

    [Theory]
    [InlineData(GroupOperator.And)]
    [InlineData(GroupOperator.Or)]
    public async Task BuildQueryAsync_WhenRequiredClauseCannotBeBuilt_RejectsRatherThanBroadening(GroupOperator op)
    {
        using var resolver = new ElasticMappingResolver(() => RequiredClauseFixture.Mapping);
        var parser = new ElasticQueryParser(configuration => configuration.UseMappings(resolver));
        await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync("+[1 TO 5] tags:a", new ElasticQueryVisitorContext { DefaultOperator = op }));
    }

    private async Task<SearchResponse<RequiredClauseFixture.Document>> SearchAsync(Query query)
    {
        var response = await Client.SearchAsync<RequiredClauseFixture.Document>(descriptor => descriptor
            .Indices(_fixture.Index).Query(query).Size(100).TrackTotalHits(true).AllowPartialSearchResults(false), TestCancellationToken);
        Assert.True(response.IsValidResponse, response.DebugInformation);
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        return response;
    }
}
