using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class ProductionQueryContractTests : ElasticsearchTestBase<ProductionQueryContractFixture>
{
    private readonly ProductionQueryContractFixture _fixture;
    private readonly ITestOutputHelper _output;

    public ProductionQueryContractTests(ITestOutputHelper output, ProductionQueryContractFixture fixture) : base(output, fixture)
    {
        _fixture = fixture;
        _output = output;
    }

    public static IEnumerable<object[]> MatchingCases()
    {
        foreach (var row in ReadRows("production-cases.tsv"))
            foreach (string operatorName in new[] { "DEFAULT", "AND", "OR" })
                foreach (bool scoring in new[] { false, true })
                    yield return [row[0], row[1], operatorName == "OR" ? row[3] : row[2], operatorName, scoring];
    }

    public static IEnumerable<object[]> MigrationCases()
    {
        foreach (var row in ReadRows("production-migrations.tsv"))
            foreach (string operatorName in new[] { "DEFAULT", "AND", "OR" })
                foreach (bool scoring in new[] { false, true })
                    yield return [row[0], row[1], row[2], row[3], operatorName, scoring];
    }

    public static IEnumerable<object[]> DateCases()
    {
        foreach (var row in ReadRows("production-dates.tsv"))
            foreach (bool scoring in new[] { false, true })
                yield return [row[0], row[1], row[2], row[3], scoring];
    }

    private static IEnumerable<string[]> ReadRows(string file)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (string line in File.ReadLines(Path.Combine(AppContext.BaseDirectory, "Compatibility", file)))
        {
            if (line.StartsWith('#') || String.IsNullOrWhiteSpace(line))
                continue;
            string[] row = line.Split('\t');
            if (row.Length != 4 || row.Any(String.IsNullOrWhiteSpace) || !ids.Add(row[0]))
                throw new InvalidDataException($"Invalid or duplicate {file} row: {line}");
            yield return row;
        }
        if (ids.Count == 0)
            throw new InvalidDataException($"Empty contract corpus: {file}");
    }

    [Theory]
    [MemberData(nameof(MatchingCases))]
    public async Task BuildQueryAsync_WithContractCase_PreservesDocumentSet(string id, string text, string expected, string operatorName, bool scoring)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        var query = await parser.BuildQueryAsync(text, Context(operatorName, scoring));
        var response = await SearchAsync(query, scoring);
        _output.WriteLine($"{id}: {text}; operator={operatorName}; scoring={scoring}; ids={Ids(response)}");
        Assert.Equal(expected, Ids(response));
    }

    [Theory]
    [MemberData(nameof(MigrationCases))]
    public async Task BuildQueryAsync_WithMigration_PreservesIndependentExpectedSet(string id, string legacy, string canonical, string expected, string operatorName, bool scoring)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        var before = await SearchAsync(await parser.BuildQueryAsync(legacy, Context(operatorName, scoring)), scoring);
        var after = await SearchAsync(await parser.BuildQueryAsync(canonical, Context(operatorName, scoring)), scoring);
        _output.WriteLine($"{id}: {legacy} -> {canonical}");
        Assert.Equal(expected, Ids(before));
        Assert.Equal(expected, Ids(after));
    }

    [Theory]
    [MemberData(nameof(DateCases))]
    public async Task BuildQueryAsync_WithDateBoundary_PreservesUtcAndNanosecondBounds(string id, string text, string expected, string referenceJson, bool scoring)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.DateIndex);
        var parser = CreateParser(resolver);
        var native = await SearchAsync(await parser.BuildQueryAsync(text, Context("AND", scoring)), scoring, _fixture.DateIndex);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(referenceJson));
        var reference = await Client.RequestResponseSerializer.DeserializeAsync<Query>(stream, TestCancellationToken);
        var external = await SearchAsync(InContext(reference, scoring), scoring, _fixture.DateIndex);
        _output.WriteLine($"{id}: {text}; reference={referenceJson}");
        Assert.Equal(expected, Ids(native));
        Assert.Equal(expected, Ids(external));
    }

    [Theory]
    [InlineData("standard", "Alpha-Beta", "alpha,beta")]
    [InlineData("lowerkeyword", "VIP Member", "vip member")]
    [InlineData("whitespace_lower", "App.Services.Checkout, SECOND", "app.services.checkout,second")]
    [InlineData("standardplus", "App.Services.Checkout", "app,app.services,app.services.checkout,checkout,services")]
    public async Task AnalyzeAsync_WithProfileAnalyzer_PreservesTokenContract(string analyzer, string text, string expected)
    {
        var response = await Client.Indices.AnalyzeAsync(descriptor => descriptor.Index(_fixture.Index).Analyzer(analyzer).Text(text), TestCancellationToken);
        Assert.True(response.IsValidResponse, response.DebugInformation);
        Assert.Equal(expected, String.Join(',', response.Tokens.Select(token => token.Token).Order(StringComparer.Ordinal)));
    }

    [Theory]
    [InlineData("message:alpha")]
    [InlineData("message:\"alpha beta\"")]
    [InlineData("message:alpha AND message:beta")]
    [InlineData("message:alpha OR message:gamma")]
    public async Task BuildQueryAsync_WithScoringControl_AgreesWithSameIndexReference(string text)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        var native = await SearchAsync(await parser.BuildQueryAsync(text, Context("OR", true)), true);
        var reference = await SearchAsync(new QueryStringQuery { Query = text, DefaultField = "message", DefaultOperator = Operator.Or }, true);
        Assert.NotEmpty(native.Hits);
        Assert.Equal(Ids(reference), Ids(native));
        var scores = reference.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        foreach (var hit in native.Hits)
            AssertClose(scores[hit.Id], hit.Score!.Value);
    }

    [Theory]
    [InlineData("message:alpha", "message:alpha^8")]
    [InlineData("message:\"alpha beta\"~1", "message:\"alpha beta\"~1^8")]
    [InlineData("reference:jo?n", "reference:jo?n^8")]
    [InlineData("reference:/john.*/", "reference:/john.*/^8")]
    [InlineData("(message:alpha OR message:gamma)", "(message:alpha OR message:gamma)^8")]
    [InlineData("children.name:alice", "children.name:alice^8")]
    [InlineData("@include:vip", "@include:vip^8")]
    public async Task BuildQueryAsync_WithBoost_PreservesMembershipAndMultipliesScores(string baseline, string boosted)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        var before = await SearchAsync(await parser.BuildQueryAsync(baseline, Context("OR", true)), true);
        var after = await SearchAsync(await parser.BuildQueryAsync(boosted, Context("OR", true)), true);
        Assert.NotEmpty(before.Hits);
        Assert.Equal(Ids(before), Ids(after));
        var scores = before.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        Assert.All(scores.Values, score => Assert.True(score > 0));
        foreach (var hit in after.Hits)
            AssertClose(scores[hit.Id] * 8, hit.Score!.Value);
    }

    [Fact]
    public async Task BuildQueryAsync_WithOptionalClause_ChangesScoresNotMembership()
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        var baseline = await SearchAsync(await parser.BuildQueryAsync("message:alpha", Context("OR", true)), true);
        var optional = await SearchAsync(await parser.BuildQueryAsync("+message:alpha message:gamma", Context("OR", true)), true);
        Assert.Equal("a,b,e", Ids(baseline));
        Assert.Equal("a,b,e", Ids(optional));
        var scores = baseline.Hits.ToDictionary(hit => hit.Id, hit => hit.Score!.Value);
        foreach (var hit in optional.Hits)
        {
            if (hit.Id == "b")
                Assert.True(hit.Score > scores[hit.Id]);
            else
                AssertClose(scores[hit.Id], hit.Score!.Value);
        }
    }

    [Theory]
    [InlineData("message:alpha OR organization:org-b", "a,b,e")]
    [InlineData("NOT organization:org-a", "-")]
    [InlineData("organization:org-b OR message:gamma", "b,d")]
    [InlineData("+message:alpha message:gamma", "a,b,e")]
    public async Task BuildQueryAsync_WithApplicationFilter_CannotBroadenOuterScope(string text, string expected)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        foreach (bool scoring in new[] { false, true })
        {
            var userQuery = await parser.BuildQueryAsync(text, Context("OR", scoring));
            var combined = new BoolQuery
            {
                Must = [userQuery],
                Filter = [new TermQuery("organization_id", "org-a"), new TermQuery("project_id", "p1")]
            };
            var response = await SearchAsync(combined, scoring);
            Assert.Equal(expected, Ids(response));
            Assert.All(response.Hits, hit =>
            {
                Assert.Equal("org-a", hit.Source!.Fields["organization_id"].GetString());
                Assert.Equal("p1", hit.Source.Fields["project_id"].GetString());
            });
        }
    }

    [Theory]
    [InlineData("message", "alpha", "a,b,e")]
    [InlineData("reference_id", "john\\*", "d")]
    [InlineData("message,reference_id", "john", "f")]
    [InlineData("children.name", "alice", "a,b,c")]
    [InlineData("message,children.name", "alice", "a,b,c")]
    public async Task BuildQueryAsync_WithDefaultFields_RestrictsSelection(string fields, string text, string expected)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver, fields.Split(','));
        foreach (bool scoring in new[] { false, true })
            Assert.Equal(expected, Ids(await SearchAsync(await parser.BuildQueryAsync(text, Context("OR", scoring)), scoring)));
    }

    [Theory]
    [InlineData("reference:*ohn")]
    [InlineData("msg:?lpha")]
    [InlineData("who:*lice")]
    [InlineData("field\\.with\\.dots:value")]
    [InlineData("count:-[1 TO 5]")]
    [InlineData("message:alpha~3")]
    [InlineData("message:\"alpha beta\"~1.5")]
    [InlineData("message:alpha^-1")]
    public async Task BuildQueryAsync_WithDisallowedInput_RejectsBeforeSearch(string text)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        foreach (bool scoring in new[] { false, true })
            await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(text, Context("OR", scoring)));
    }

    [Theory]
    [InlineData("reference:\\*john")]
    [InlineData("reference:\"*john\"")]
    [InlineData("reference:\"?ohn\"")]
    public async Task BuildQueryAsync_WithLiteralWildcard_DoesNotRejectAsPattern(string text)
    {
        using var resolver = ElasticMappingResolver.Create(Client, _fixture.Index);
        var parser = CreateParser(resolver);
        Assert.Equal("-", Ids(await SearchAsync(await parser.BuildQueryAsync(text, Context("OR", false)), false)));
    }

    private ElasticQueryParser CreateParser(ElasticMappingResolver resolver, string[]? fields = null) => new(configuration => configuration
        .UseMappings(resolver)
        .SetDefaultFields(fields ?? _fixture.Definition.DefaultFields)
        .UseFieldMap(_fixture.Definition.FieldMap)
        .UseIncludes(new Dictionary<string, string> { { "vip", "tag:VIP" } })
        .UseNested());

    private static ElasticQueryVisitorContext Context(string operatorName, bool scoring)
    {
        var context = new ElasticQueryVisitorContext { UseScoring = scoring };
        if (operatorName != "DEFAULT")
            context.DefaultOperator = operatorName == "AND" ? GroupOperator.And : GroupOperator.Or;
        context.SetValidationOptions(new QueryValidationOptions { AllowLeadingWildcards = false });
        return context;
    }

    private static Query InContext(Query query, bool scoring) => scoring ? query : new BoolQuery { Filter = [query] };

    private async Task<SearchResponse<ProductionQueryContractFixture.Document>> SearchAsync(Query query, bool scoring, string? index = null)
    {
        using var stream = new MemoryStream();
        await Client.RequestResponseSerializer.SerializeAsync(query, stream, TestCancellationToken);
        _output.WriteLine(Encoding.UTF8.GetString(stream.ToArray()));
        var response = await Client.SearchAsync<ProductionQueryContractFixture.Document>(descriptor => descriptor
            .Indices(index ?? _fixture.Index).Query(query).Size(100).TrackTotalHits(true).AllowPartialSearchResults(false), TestCancellationToken);
        Assert.True(response.IsValidResponse, response.DebugInformation);
        Assert.False(response.TimedOut);
        Assert.Equal(0, response.Shards.Failed);
        Assert.Equal(response.Total, response.Hits.Count);
        Assert.Equal(response.Hits.Count, response.Hits.Select(hit => hit.Id).Distinct(StringComparer.Ordinal).Count());
        Assert.All(response.Hits, hit =>
        {
            Assert.NotNull(hit.Source);
            Assert.Equal(hit.Id, hit.Source.Id);
            Assert.True(hit.Score.HasValue && Double.IsFinite(hit.Score.Value) && hit.Score.Value >= 0);
            if (!scoring)
                Assert.Equal(0, hit.Score!.Value);
        });
        _output.WriteLine(String.Join(", ", response.Hits.Select(hit => $"{hit.Id}={hit.Score:R}")));
        return response;
    }

    private static string Ids(SearchResponse<ProductionQueryContractFixture.Document> response) => response.Hits.Count == 0
        ? "-" : String.Join(',', response.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));

    private static void AssertClose(double expected, double actual) => Assert.True(
        Math.Abs(expected - actual) <= 0.00001 * Math.Max(1, Math.Abs(expected)), $"Expected {expected:R}, actual {actual:R}");
}
