using System;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

// These assertions specify the default query pipeline, not just accepted AST syntax.
// Keep the executable contract and both syntax guides synchronized.
public class TermTranslationTests : TestWithLoggingBase
{
    public TermTranslationTests(ITestOutputHelper output) : base(output) { }

    [Theory]
    [InlineData("text:1..5", "match", "text", "query", "1..5")]
    [InlineData("keyword:1..5", "term", "keyword", "value", "1..5")]
    [InlineData("text:john\\*", "match", "text", "query", "john*")]
    [InlineData("keyword:john\\*", "term", "keyword", "value", "john*")]
    [InlineData("keyword:jo\\?n", "term", "keyword", "value", "jo?n")]
    [InlineData("keyword:\"john*\"", "term", "keyword", "value", "john*")]
    public async Task BuildQueryAsync_WithLiteralTerm_DoesNotInventWildcardOperators(string query, string kind, string field, string property, string expected)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal(kind, actual.Name);
        Assert.Equal(expected, actual.Value.GetProperty(field).GetProperty(property).GetString());
    }

    [Theory]
    [InlineData("text:john*", "john*", 1)]
    [InlineData("text:jo?n*", "jo?n*", 1)]
    [InlineData("text:jo?n", "jo?n", 1)]
    [InlineData("text:*john", "*john", 1)]
    [InlineData("text:otherText\\:john*", "otherText\\:john*", 1)]
    [InlineData("john*", "john*", 2)]
    public async Task BuildQueryAsync_WithAnalyzedWildcard_UsesEscapedTermAndExplicitOptions(string query, string value, int fieldCount)
    {
        using var json = await BuildQueryJsonAsync(query, ["text", "otherText"]);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal("query_string", actual.Name);
        Assert.Equal(value, actual.Value.GetProperty("query").GetString());
        Assert.True(actual.Value.GetProperty("analyze_wildcard").GetBoolean());
        Assert.True(actual.Value.GetProperty("allow_leading_wildcard").GetBoolean());
        Assert.Equal(fieldCount, actual.Value.GetProperty("fields").GetArrayLength());
    }

    [Theory]
    [InlineData("keyword:john*", "prefix", "john")]
    [InlineData("keyword:jo\\?n*", "prefix", "jo?n")]
    [InlineData("keyword:john\\**", "prefix", "john*")]
    [InlineData("keyword:jo?n", "wildcard", "jo?n")]
    [InlineData("keyword:jo*n", "wildcard", "jo*n")]
    [InlineData("keyword:*john", "wildcard", "*john")]
    [InlineData("keyword:jo?n*", "wildcard", "jo?n*")]
    public async Task BuildQueryAsync_WithKeywordPattern_DistinguishesPrefixAndWildcard(string query, string kind, string value)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal(kind, actual.Name);
        Assert.Equal(value, actual.Value.GetProperty("keyword").GetProperty("value").GetString());
    }

    [Theory]
    [InlineData("text:/foo.bar/", "text", "foo.bar")]
    [InlineData("keyword:/[0-9]+/", "keyword", "[0-9]+")]
    [InlineData("keyword:/val.*/", "keyword", "val.*")]
    [InlineData("keyword:/foo\\.bar/", "keyword", "foo\\.bar")]
    public async Task BuildQueryAsync_WithRegex_PreservesRegexOperatorsAndEscapes(string query, string field, string pattern)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal("regexp", actual.Name);
        Assert.Equal(pattern, actual.Value.GetProperty(field).GetProperty("value").GetString());
    }

    [Theory]
    [InlineData("text:value~2", "match", "text", 2)]
    [InlineData("text:value~", "match", "text", 2)]
    [InlineData("text:value~0", "match", "text", 0)]
    [InlineData("keyword:value~1", "fuzzy", "keyword", 1)]
    public async Task BuildQueryAsync_WithFuzzyTerm_EmitsEditDistance(string query, string kind, string field, int distance)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal(kind, actual.Name);
        Assert.Equal(distance, actual.Value.GetProperty(field).GetProperty("fuzziness").GetInt32());
    }

    [Theory]
    [InlineData("text:\"a b\"~5", "text", 5)]
    [InlineData("text:\"a b\"~", "text", 0)]
    [InlineData("keyword:\"a b\"~3", "keyword", 3)]
    public async Task BuildQueryAsync_WithPhraseSlop_EmitsSlop(string query, string field, int slop)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal("match_phrase", actual.Name);
        Assert.Equal(slop, actual.Value.GetProperty(field).GetProperty("slop").GetInt32());
    }

    [Theory]
    [InlineData("text:value^2", "match", "text", 2)]
    [InlineData("text:\"a b\"^3", "match_phrase", "text", 3)]
    [InlineData("keyword:value^0", "term", "keyword", 0)]
    [InlineData("keyword:jo?n^4", "wildcard", "keyword", 4)]
    [InlineData("keyword:/val.*/^5", "regexp", "keyword", 5)]
    [InlineData("number:[1 TO 5]^2", "range", "number", 2)]
    public async Task BuildQueryAsync_WithBoost_EmitsNativeBoost(string query, string kind, string field, float boost)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal(kind, actual.Name);
        Assert.Equal(boost, actual.Value.GetProperty(field).GetProperty("boost").GetSingle());
    }

    [Fact]
    public async Task BuildQueryAsync_WithGroupBoost_AppliesItToTheWholeGroup()
    {
        using var json = await BuildQueryJsonAsync("(text:a OR text:b)^8");
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal("bool", actual.Name);
        Assert.Equal(8, actual.Value.GetProperty("boost").GetSingle());
        Assert.Equal(2, actual.Value.GetProperty("must").GetProperty("bool").GetProperty("should").GetArrayLength());
    }

    [Theory]
    [InlineData("date", "America/Chicago")]
    [InlineData("dateNanos", "America/Chicago")]
    [InlineData("date", "2")]
    public async Task BuildQueryAsync_WithDateRangeCaret_EmitsTimeZoneNotBoost(string field, string timeZone)
    {
        using var json = await BuildQueryJsonAsync($"{field}:[2024-01-01 TO *]^\"{timeZone}\"");
        var range = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal("range", range.Name);
        var options = range.Value.GetProperty(field);
        Assert.Equal("2024-01-01", options.GetProperty("gte").GetString());
        Assert.Equal(timeZone, options.GetProperty("time_zone").GetString());
        Assert.False(options.TryGetProperty("boost", out _));
    }

    [Theory]
    [InlineData("john*", "query_string")]
    [InlineData("/john.*/", "query_string")]
    [InlineData("john~1", "multi_match")]
    [InlineData("\"alpha beta\"~2", "multi_match")]
    public async Task BuildQueryAsync_WithoutDefaultFields_RetainsTermSyntax(string query, string kind)
    {
        using var json = await BuildQueryJsonAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());
        Assert.Equal(kind, actual.Name);
        Assert.False(actual.Value.TryGetProperty("fields", out _));
        if (query.Contains('~'))
            Assert.Equal(query.StartsWith('"') ? 2 : 1, actual.Value.GetProperty(query.StartsWith('"') ? "slop" : "fuzziness").GetInt32());
    }

    [Theory]
    [InlineData("text:value~-1")]
    [InlineData("text:value~3")]
    [InlineData("text:value~NaN")]
    [InlineData("text:value~0.8")]
    [InlineData("text:value^NaN")]
    [InlineData("text:value^-1")]
    [InlineData("text:value^1e100")]
    [InlineData("text:value^Infinity")]
    [InlineData("(text:a OR text:b)^garbage")]
    [InlineData("text:foo*~1")]
    [InlineData("text:/foo/~1")]
    [InlineData("text:\"a b\"~-1")]
    [InlineData("text:\"a b\"~1.5")]
    [InlineData("text:\"a b\"~2147483648")]
    [InlineData("text:\"a b\"~junk")]
    public async Task BuildQueryAsync_WithInvalidModifier_ReportsPublicValidationError(string query)
    {
        using var resolver = CreateResolver();
        var parser = new ElasticQueryParser(configuration => configuration.SetLoggerFactory(Log).UseMappings(resolver));
        Assert.False((await parser.ValidateQueryAsync(query)).IsValid);
        await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(query));
    }

    [Fact]
    public async Task BuildQueryAsync_WithBoost_UsesInvariantCulture()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            using var json = await BuildQueryJsonAsync("text:value^1.5");
            Assert.Equal(1.5, json.RootElement.GetProperty("match").GetProperty("text").GetProperty("boost").GetDouble());
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Theory]
    [InlineData("alias:john\\*", "term", "john*")]
    [InlineData("alias:jo?n", "wildcard", "jo?n")]
    [InlineData("alias:/jo.n/", "regexp", "jo.n")]
    public async Task BuildQueryAsync_WithIncludedAlias_PreservesTranslationAndFieldRestrictions(string fragment, string kind, string value)
    {
        using var resolver = CreateResolver();
        var parser = new ElasticQueryParser(configuration => configuration
            .SetLoggerFactory(Log)
            .UseMappings(resolver)
            .UseFieldMap(new System.Collections.Generic.Dictionary<string, string> { ["alias"] = "keyword" })
            .UseIncludes(new System.Collections.Generic.Dictionary<string, string> { ["saved"] = fragment }));
        var options = new QueryValidationOptions { AllowedFields = { "alias" } };
        var context = new ElasticQueryVisitorContext { UseScoring = true };
        context.SetValidationOptions(options);

        var query = await parser.BuildQueryAsync("@include:saved", context);

        Assert.Contains("alias", context.GetValidationResult().ReferencedFields);
        Assert.Contains("saved", context.GetValidationResult().ReferencedIncludes);
        if (kind == "term")
            Assert.Equal(value, query.Term!.Value.ToString());
        else if (kind == "wildcard")
            Assert.Equal(value, query.Wildcard!.Value);
        else
            Assert.Equal(value, query.Regexp!.Value);
        Assert.False((await parser.ValidateQueryAsync("@include:saved", new QueryValidationOptions { RestrictedFields = { "alias" } })).IsValid);
        Assert.False((await parser.ValidateQueryAsync("@include:missing")).IsValid);
    }

    private static ElasticMappingResolver CreateResolver() => new(() => new TypeMapping
    {
        Properties = new Properties
        {
            { "text", new TextProperty() },
            { "otherText", new TextProperty() },
            { "keyword", new KeywordProperty() },
            { "number", new IntegerNumberProperty() },
            { "date", new DateProperty() },
            { "dateNanos", new DateNanosProperty() }
        }
    });

    private async Task<JsonDocument> BuildQueryJsonAsync(string query, string[]? defaultFields = null)
    {
        using var resolver = CreateResolver();
        var parser = new ElasticQueryParser(configuration =>
        {
            configuration.SetLoggerFactory(Log).UseMappings(resolver);
            if (defaultFields is not null)
                configuration.SetDefaultFields(defaultFields);
        });
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var client = new ElasticsearchClient(settings);
        using var stream = new MemoryStream();
        client.RequestResponseSerializer.Serialize<Query>(result, stream);
        stream.Position = 0;
        return await JsonDocument.ParseAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
    }
}
