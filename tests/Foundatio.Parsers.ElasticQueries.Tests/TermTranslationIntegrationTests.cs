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

    public static IEnumerable<TheoryDataRow<string, string, bool>> TermMatchingCases()
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
                yield return new(query, expected, scoring);
    }

    [Theory]
    [MemberData(nameof(TermMatchingCases))]
    public async Task BuildQueryAsync_WithTermSyntax_ReturnsExpectedDocuments(string text, string expected, bool scoring)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);

        // Native bare fuzziness is distance 2; query_string otherwise defaults to AUTO.
        string referenceText = text.EndsWith("~", StringComparison.Ordinal) ? text + "2" : text;
        var referenceQuery = new QueryStringQuery(referenceText)
        {
            DefaultOperator = Operator.Or,
            AnalyzeWildcard = true,
            AllowLeadingWildcard = true
        };

        // Act
        var query = await parser.BuildQueryAsync(text, CreateQueryContext(scoring));
        var native = await SearchAsync(query);
        var reference = text.Contains("children.", StringComparison.Ordinal) ? null : await SearchAsync(referenceQuery);

        // Assert
        Assert.Equal(expected, GetDocumentIds(native));

        if (!scoring)
            Assert.All(native.Hits, hit => Assert.Equal(0, hit.Score));

        if (reference is not null)
            Assert.Equal(expected, GetDocumentIds(reference));
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
    public async Task BuildQueryAsync_WithConfiguredDefaultField_ReturnsExpectedDocuments(string text, string field, string expected)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, [field]);

        // Act
        var query = await parser.BuildQueryAsync(text, CreateQueryContext(true));
        var result = await SearchAsync(query);

        // Assert
        Assert.Equal(expected, GetDocumentIds(result));
    }

    [Theory]
    [InlineData("jo?n", "a,b,e,g,p")]
    [InlineData("john~1", "a,b,e,g,p")]
    [InlineData("john~AUTO", "a,b,e,g,p")]
    [InlineData("/john/", "a,e,g,p")]
    public async Task BuildQueryAsync_WithMultipleAnalyzedFields_ReturnsExpectedDocuments(string text, string expected)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, ["text", "otherText"]);

        // Act
        var query = await parser.BuildQueryAsync(text, CreateQueryContext(true));
        var result = await SearchAsync(query);

        // Assert
        Assert.Equal(expected, GetDocumentIds(result));
    }

    [Theory]
    [InlineData("jo?n", "a,b,c,e,g")]
    [InlineData("/john/", "a,e,g")]
    [InlineData("john~AUTO", "a,b,c,e,g")]
    public async Task BuildQueryAsync_WithMixedDefaultFields_ReturnsExpectedDocuments(string text, string expected)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, ["text", "keyword", "children.keyword"]);

        // Act
        var query = await parser.BuildQueryAsync(text, CreateQueryContext(true));
        var result = await SearchAsync(query);

        // Assert
        Assert.Equal(expected, GetDocumentIds(result));
    }

    [Theory]
    [InlineData("keyword:john", "keyword:john^8")]
    [InlineData("keyword:john~1", "keyword:john~1^8")]
    [InlineData("keyword:john~AUTO", "keyword:john~AUTO^8")]
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
    [InlineData("children:(children.text:john OR children.text:alpha)", "children:(children.text:john OR children.text:alpha)^8")]
    [InlineData("+(children.text:john OR children.text:alpha)", "+(children.text:john OR children.text:alpha)^8")]
    [InlineData("(text:john OR text:alpha)", @"(text:john OR text:alpha)^\+8")]
    [InlineData("@include:john", @"@include:john^\+8")]
    [InlineData("+(children.text:john OR children.text:alpha)", @"+(children.text:john OR children.text:alpha)^\+8")]
    public async Task BuildQueryAsync_WithBoost_MultipliesScoresWithoutChangingMembership(string baseline, string boosted)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver, ["text", "keyword", "children.keyword"]);

        // Act
        var baselineQuery = await parser.BuildQueryAsync(baseline, CreateQueryContext(true));
        var boostedQuery = await parser.BuildQueryAsync(boosted, CreateQueryContext(true));
        var before = await SearchAsync(baselineQuery);
        var after = await SearchAsync(boostedQuery);

        // Assert
        Assert.NotEmpty(before.Hits);
        Assert.Equal(GetDocumentIds(before), GetDocumentIds(after));

        var scores = before.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        foreach (var hit in after.Hits)
        {
            double expectedScore = scores[hit.Id] * 8;
            Assert.True(Math.Abs(hit.Score!.Value - expectedScore) <= 0.00001 * Math.Max(1, expectedScore));
        }
    }

    [Theory]
    [InlineData("john*", "a,d,e,g,p")]
    [InlineData("/john/", "a,e,g,p")]
    [InlineData("john~1", "a,b,c,e,g,p")]
    [InlineData("john~AUTO", "a,b,c,e,g,p")]
    [InlineData("\"alpha beta\"~1", "k,l")]
    public async Task BuildQueryAsync_WithoutDefaultFields_UsesServerDefaultFields(string text, string expected)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = new ElasticQueryParser(configuration => configuration.SetLoggerFactory(Log).UseMappings(resolver));

        // Act
        var query = await parser.BuildQueryAsync(text, CreateQueryContext(true));
        var result = await SearchAsync(query);

        // Assert
        Assert.Equal(expected, GetDocumentIds(result));
    }

    [Theory]
    [InlineData("keyword:*john")]
    [InlineData("text:?ohn*")]
    [InlineData("children.keyword:*john")]
    public async Task BuildQueryAsync_WithDisallowedLeadingWildcard_RejectsBeforeSearch(string text)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        var options = new QueryValidationOptions { AllowLeadingWildcards = false };
        var context = CreateQueryContext(true);
        context.SetValidationOptions(options);

        // Act
        var validation = await parser.ValidateQueryAsync(text, options);
        var error = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(text, context));

        // Assert
        Assert.False(validation.IsValid);
        Assert.NotNull(error.Result);
    }

    [Theory]
    [InlineData("keyword:\"*john\"")]
    [InlineData("keyword:\\*john")]
    [InlineData("keyword:\"?ohn\"")]
    public async Task BuildQueryAsync_WithLiteralLeadingWildcard_DoesNotTreatItAsAnOperator(string text)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        var context = CreateQueryContext(true);
        context.SetValidationOptions(new QueryValidationOptions { AllowLeadingWildcards = false });

        // Act
        var query = await parser.BuildQueryAsync(text, context);
        var result = await SearchAsync(query);

        // Assert
        Assert.Empty(result.Hits);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    public async Task BuildQueryAsync_WithBoostedNestedGroup_PreservesNestedScope(bool required, bool scoring, bool explicitScope)
    {
        // Arrange
        string index = $"nested_boost_{Guid.NewGuid():N}";
        await CreateIndexAsync(index, descriptor => descriptor
            .Settings(settings => settings.NumberOfShards(1).NumberOfReplicas(0))
            .Mappings(TermTranslationFixture.Mapping));
        TermTranslationFixture.TermTranslationDocument[] documents =
        [
            new() { Id = "same-child", Children = [new("a", "c")] },
            new() { Id = "second-alternative", Children = [new("b", "c")] },
            new() { Id = "split-children", Children = [new("a", "x"), new("x", "c")] }
        ];
        var bulk = await Client.IndexManyAsync(documents, index, TestCancellationToken);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);

        var refresh = await Client.Indices.RefreshAsync(index, cancellationToken: TestCancellationToken);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);

        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        string group = "(children.keyword:a OR children.keyword:b)";
        if (explicitScope)
            group = "children:" + group;
        if (required)
            group = "+" + group;

        string sibling = required ? " +children.text:c" : " AND children.text:c";
        const string correlatedIds = "same-child,second-alternative";
        string expectedIds = explicitScope ? correlatedIds + ",split-children" : correlatedIds;
        var alternatives = new BoolQuery { Should = [new TermQuery("children.keyword", "a"), new TermQuery("children.keyword", "b")] };
        var siblingQuery = new MatchQuery("children.text", "c");
        // An explicit children:(...) group has its own nested boundary; implicit groups
        // correlate with siblings inside a single child document.
        Query reference = explicitScope
            ? new BoolQuery { Must = [new NestedQuery("children", alternatives), new NestedQuery("children", siblingQuery)] }
            : new NestedQuery("children", new BoolQuery { Must = [alternatives, siblingQuery] });

        // Act
        var baselineQuery = await parser.BuildQueryAsync(group + sibling, CreateQueryContext(scoring));
        var boostedQuery = await parser.BuildQueryAsync(group + "^8" + sibling, CreateQueryContext(scoring));
        var baseline = await SearchAsync(baselineQuery, index);
        var boosted = await SearchAsync(boostedQuery, index);
        var referenceResult = await SearchAsync(reference, index);

        // Assert
        Assert.Equal(expectedIds, GetDocumentIds(referenceResult));
        Assert.Equal(expectedIds, GetDocumentIds(baseline));
        Assert.Equal(expectedIds, GetDocumentIds(boosted));

        if (scoring)
        {
            var baselineScores = baseline.Hits.ToDictionary(hit => hit.Id, hit => hit.Score);
            Assert.All(boosted.Hits, hit => Assert.True(hit.Score > baselineScores[hit.Id]));
        }
        else
        {
            Assert.All(boosted.Hits, hit => Assert.Equal(0, hit.Score));
        }
    }

    public static IEnumerable<TheoryDataRow<string, string, bool>> AutoFuzzinessCases()
    {
        (string Query, string Expected)[] cases =
        [
            ("keyword:john~AUTO", "a,b,c,e"),
            ("keyword:john~auto", "a,b,c,e"),
            (@"keyword:john~AUTO\:2,4", "a,b,c,d,e"),
            (@"keyword:john~AUTO\:5,8", "a"),
            (@"keyword:john~AUTO\:0,0", "a,b,c,d,e"),
            (@"keyword:john~AUTO\:4,4", "a,b,c,d,e"),
            ("keyword:jo~AUTO", ""),
            ("keyword:jon~AUTO", "a,b,c"),
            ("keyword:johnn~AUTO", "a,d,e"),
            ("keyword:johnny~AUTO", "a,d,e,f"),
            ("text:john~AUTO", "a,b,e,g"),
            (@"text:john~AUTO\:5,8", "a,e,g"),
            ("children.keyword:john~AUTO", "a,b,c,e"),
            ("children.text:john~AUTO", "a,b,e,g"),
            ("john~AUTO", "a,b,e,g"),
            (@"john~AUTO\:5,8", "a,e,g")
        ];

        foreach (var (query, expected) in cases)
            foreach (bool scoring in new[] { false, true })
                yield return new(query, expected, scoring);
    }

    [Theory]
    [MemberData(nameof(AutoFuzzinessCases))]
    public async Task BuildQueryAsync_WithAutoFuzziness_ReturnsExpectedDocuments(string text, string expected, bool scoring)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);

        // Act
        var query = await parser.BuildQueryAsync(text, CreateQueryContext(scoring));
        var result = await SearchAsync(query);

        // Assert
        Assert.Equal(expected, GetDocumentIds(result));

        if (!scoring)
            Assert.All(result.Hits, hit => Assert.Equal(0, hit.Score));
    }

    [Theory]
    [InlineData("AUTO", "a,b,c,e")]
    [InlineData("AUTO:3,6", "a,b,c,e")]
    [InlineData("AUTO:2,4", "a,b,c,d,e")]
    [InlineData("AUTO:5,8", "a")]
    public async Task BuildQueryAsync_WithBareFuzziness_UsesFixedDistanceInsteadOfAuto(string policy, string expectedAutoIds)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => TermTranslationFixture.Mapping);
        var parser = CreateParser(resolver);
        var autoQuery = new FuzzyQuery("keyword", "john") { Fuzziness = new Fuzziness(policy) };

        // Act
        var fixedQuery = await parser.BuildQueryAsync("keyword:john~", CreateQueryContext(true));
        var fixedResult = await SearchAsync(fixedQuery);
        var autoResult = await SearchAsync(autoQuery);
        var parsedAuto = await parser.BuildQueryAsync("keyword:john~" + policy.Replace(":", @"\:", StringComparison.Ordinal), CreateQueryContext(true));
        var parsedResult = await SearchAsync(parsedAuto);

        // Assert
        Assert.Equal("a,b,c,d,e", GetDocumentIds(fixedResult));
        Assert.Equal(expectedAutoIds, GetDocumentIds(autoResult));
        Assert.Equal(expectedAutoIds, GetDocumentIds(parsedResult));
    }

    private ElasticQueryParser CreateParser(ElasticMappingResolver resolver, string[]? fields = null) => new(configuration => configuration
        .SetLoggerFactory(Log)
        .UseMappings(resolver)
        .SetDefaultFields(fields ?? ["text"])
        .UseIncludes(new Dictionary<string, string> { { "john", "text:john" } })
        .UseNested());

    private static ElasticQueryVisitorContext CreateQueryContext(bool scoring) => new() { UseScoring = scoring, DefaultOperator = GroupOperator.Or };

    private async Task<SearchResponse<TermTranslationFixture.TermTranslationDocument>> SearchAsync(Query query, string? index = null)
    {
        var response = await Client.SearchAsync<TermTranslationFixture.TermTranslationDocument>(descriptor => descriptor
            .Indices(index ?? _fixture.Index).Query(query).Size(100).TrackTotalHits(true).AllowPartialSearchResults(false), TestCancellationToken);

        Assert.True(response.IsValidResponse, response.DebugInformation);
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        Assert.All(response.Hits, hit => Assert.True(hit.Score.HasValue && Double.IsFinite(hit.Score.Value)));

        return response;
    }

    private static string GetDocumentIds(SearchResponse<TermTranslationFixture.TermTranslationDocument> response) => String.Join(',', response.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
}
