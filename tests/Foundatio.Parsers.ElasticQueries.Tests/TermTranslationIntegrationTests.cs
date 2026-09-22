using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class TermTranslationIntegrationTests : ElasticsearchTestBase<TermTranslationFixture>
{
    private readonly TermTranslationFixture _fixture;

    public TermTranslationIntegrationTests(ITestOutputHelper output, TermTranslationFixture fixture) : base(output, fixture)
    {
        _fixture = fixture;
    }

    public static IEnumerable<object[]> Cases()
    {
        (string Query, string Expected)[] cases =
        [
            ("keyword:jo?n", "a,b,c"),
            ("keyword:jo*n", "a,b,c"),
            ("keyword:*john", "a"),
            ("keyword:john*", "a,d,e"),
            ("keyword:jo?n*", "a,b,c,d,e,f"),
            ("keyword:john\\*", "e"),
            ("keyword:jo\\?n", "c"),
            ("keyword:jo\\?n*", "c,f"),
            ("keyword:john\\**", "e"),
            ("keyword:JOHN*", "g"),
            ("keyword:foo\\:bar*", "h,i,j"),
            ("keyword:foo\\:bar\\*", "j"),
            ("keyword:path\\\\name", "m"),
            ("keyword:path\\\\*", "m"),
            ("keyword:path\\/name", "n"),
            ("keyword:path\\/*", "n"),
            ("keyword:\"john*\"", "e"),
            ("keyword:*", "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o"),
            ("keyword:?", ""),
            ("text:JOHN*", "a,d,e,g"),
            ("text:jo?n", "a,b,e,g"),
            ("text:john\\*", "a,e,g"),
            ("keyword:/jo.n/", "a,b,c"),
            ("keyword:/john.*/", "a,d,e"),
            ("keyword:/jo\\?n/", "c"),
            ("keyword:/foo:bar\\*/", "j"),
            ("keyword:/path\\\\name/", "m"),
            ("keyword:/path\\/name/", "n"),
            ("text:/john/", "a,e,g"),
            ("keyword:john~0", "a"),
            ("keyword:john~1", "a,b,c,e"),
            ("keyword:john~", "a,b,c,d,e"),
            ("text:john~1", "a,b,e,g"),
            ("text:\"alpha beta\"~1", "k,l"),
            ("children.keyword:jo?n", "a,b,c"),
            ("children.keyword:john\\*", "e"),
            ("children.keyword:/jo.n/", "a,b,c"),
            ("children.keyword:john~1", "a,b,c,e"),
            ("children.text:\"alpha beta\"~1", "k,l"),
            ("NOT children.keyword:jo?n", "d,e,f,g,h,i,j,k,l,m,n,o,p")
        ];

        foreach (var (query, expected) in cases)
            foreach (bool scoring in new[] { false, true })
                yield return [query, expected, scoring];
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task BuildQueryAsync_WithTermSyntax_PreservesIndependentDocumentExpectations(string text, string expected, bool scoring)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        var native = await SearchAsync(await parser.BuildQueryAsync(text, Context(scoring)));
        Assert.Equal(expected, Ids(native));
        if (!scoring)
            Assert.All(native.Hits, hit => Assert.Equal(0, hit.Score));

        if (!text.Contains("children.", StringComparison.Ordinal))
        {
            var reference = await SearchAsync(new QueryStringQuery(text) { DefaultOperator = Operator.Or, AnalyzeWildcard = true, AllowLeadingWildcard = true });
            Assert.Equal(expected, Ids(reference));
        }
    }

    [Theory]
    [InlineData("jo?n", "keyword", "a,b,c")]
    [InlineData("john\\*", "keyword", "e")]
    [InlineData("/jo.n/", "keyword", "a,b,c")]
    [InlineData("john~1", "keyword", "a,b,c,e")]
    [InlineData("jo?n", "text", "a,b,e,g")]
    [InlineData("john~1", "text", "a,b,e,g")]
    [InlineData("jo?n", "children.keyword", "a,b,c")]
    [InlineData("john\\*", "children.keyword", "e")]
    public async Task BuildQueryAsync_WithConfiguredDefaultField_PreservesTermSyntax(string text, string field, string expected)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, [field]);
        Assert.Equal(expected, Ids(await SearchAsync(await parser.BuildQueryAsync(text, Context(true)))));
    }

    [Theory]
    [InlineData("jo?n", "a,b,e,g,p")]
    [InlineData("john~1", "a,b,e,g,p")]
    [InlineData("/john/", "a,e,g,p")]
    public async Task BuildQueryAsync_WithMultipleAnalyzedFields_PreservesTermSyntax(string text, string expected)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, ["text", "otherText"]);
        Assert.Equal(expected, Ids(await SearchAsync(await parser.BuildQueryAsync(text, Context(true)))));
    }

    [Theory]
    [InlineData("jo?n", "a,b,c,e,g")]
    [InlineData("/john/", "a,e,g")]
    public async Task BuildQueryAsync_WithMixedDefaultFields_PreservesTermSyntax(string text, string expected)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, ["text", "keyword", "children.keyword"]);
        Assert.Equal(expected, Ids(await SearchAsync(await parser.BuildQueryAsync(text, Context(true)))));
    }

    [Theory]
    [InlineData("keyword:john", "keyword:john^8")]
    [InlineData("keyword:john~1", "keyword:john~1^8")]
    [InlineData("keyword:jo?n", "keyword:jo?n^8")]
    [InlineData("keyword:/jo.n/", "keyword:/jo.n/^8")]
    [InlineData("text:john", "text:john^8")]
    [InlineData("text:john~1", "text:john~1^8")]
    [InlineData("text:\"alpha beta\"~1", "text:\"alpha beta\"~1^8")]
    [InlineData("children.keyword:john~1", "children.keyword:john~1^8")]
    [InlineData("(text:john OR text:alpha)", "(text:john OR text:alpha)^8")]
    [InlineData("(+text:john text:alpha)", "(+text:john text:alpha)^8")]
    [InlineData("@include:john", "@include:john^8")]
    [InlineData("john", "john^8")]
    public async Task BuildQueryAsync_WithBoost_MultipliesScoresWithoutChangingMembership(string baseline, string boosted)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, ["text", "keyword", "children.keyword"]);
        var before = await SearchAsync(await parser.BuildQueryAsync(baseline, Context(true)));
        var after = await SearchAsync(await parser.BuildQueryAsync(boosted, Context(true)));
        Assert.NotEmpty(before.Hits);
        Assert.Equal(Ids(before), Ids(after));
        var scores = before.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        foreach (var hit in after.Hits)
            Assert.True(Math.Abs(hit.Score!.Value - scores[hit.Id] * 8) <= 0.00001 * Math.Max(1, scores[hit.Id] * 8));
    }

    [Theory]
    [InlineData("keyword:*john")]
    [InlineData("text:?ohn*")]
    [InlineData("children.keyword:*john")]
    public async Task BuildQueryAsync_WithDisallowedLeadingWildcard_RejectsBeforeSearch(string text)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        var options = new QueryValidationOptions { AllowLeadingWildcards = false };
        Assert.False((await parser.ValidateQueryAsync(text, options)).IsValid);
        var context = Context(true);
        context.SetValidationOptions(options);
        await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(text, context));
    }

    [Theory]
    [InlineData("keyword:\"*john\"")]
    [InlineData("keyword:\\*john")]
    [InlineData("keyword:\"?ohn\"")]
    public async Task BuildQueryAsync_WithLiteralLeadingWildcard_DoesNotTreatItAsAnOperator(string text)
    {
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        var context = Context(true);
        context.SetValidationOptions(new QueryValidationOptions { AllowLeadingWildcards = false });
        Assert.Empty((await SearchAsync(await parser.BuildQueryAsync(text, context))).Hits);
    }

    private static ElasticQueryParser CreateParser(ElasticMappingResolver resolver, string[]? fields = null) => new(configuration => configuration
        .UseMappings(resolver)
        .SetDefaultFields(fields ?? ["text"])
        .UseIncludes(new Dictionary<string, string> { { "john", "text:john" } })
        .UseNested());

    private static ElasticQueryVisitorContext Context(bool scoring) => new() { UseScoring = scoring, DefaultOperator = GroupOperator.Or };

    private async Task<SearchResponse<TermTranslationFixture.Document>> SearchAsync(Query query)
    {
        var response = await Client.SearchAsync<TermTranslationFixture.Document>(descriptor => descriptor
            .Indices(_fixture.Index).Query(query).Size(100).TrackTotalHits(true).AllowPartialSearchResults(false), TestCancellationToken);
        Assert.True(response.IsValidResponse, response.DebugInformation);
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        Assert.All(response.Hits, hit => Assert.True(hit.Score.HasValue && Double.IsFinite(hit.Score.Value)));
        return response;
    }

    private static string Ids(SearchResponse<TermTranslationFixture.Document> response) => String.Join(',', response.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
}
