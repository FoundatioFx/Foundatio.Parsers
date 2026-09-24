using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

// These assertions specify the default query pipeline, not just accepted AST syntax.
// Keep the executable contract and both syntax guides synchronized.
public class TermTranslationTests : TestWithLoggingBase
{
    private readonly ElasticMappingResolver _resolver = CreateResolver();

    public TermTranslationTests(ITestOutputHelper output) : base(output) { }

    [Theory]
    [InlineData("text:1..5", "match", "text", "query", "1..5")]
    [InlineData("keyword:1..5", "term", "keyword", "value", "1..5")]
    [InlineData("text:john\\*", "match", "text", "query", "john*")]
    [InlineData("keyword:john\\*", "term", "keyword", "value", "john*")]
    [InlineData("keyword:jo\\?n", "term", "keyword", "value", "jo?n")]
    [InlineData("keyword:\"john*\"", "term", "keyword", "value", "john*")]
    public async Task BuildQueryAsync_WithLiteralTerm_EmitsLiteralValue(string query, string kind, string field, string property, string expected)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal(kind, actual.Name);

        var options = actual.Value.GetProperty(field);

        Assert.Equal(expected, options.GetProperty(property).GetString());
        Assert.False(options.TryGetProperty("boost", out _));
        Assert.False(options.TryGetProperty("fuzziness", out _));
    }

    [Theory]
    [InlineData("text:john*", "john*", 1)]
    [InlineData("text:jo?n*", "jo?n*", 1)]
    [InlineData("text:jo?n", "jo?n", 1)]
    [InlineData("text:jo*n", "jo*n", 1)]
    [InlineData("text:*john", "*john", 1)]
    [InlineData("text:otherText\\:john*", "otherText\\:john*", 1)]
    [InlineData("john*", "john*", 2)]
    public async Task BuildQueryAsync_WithAnalyzedWildcard_UsesEscapedTermAndExplicitOptions(string query, string value, int fieldCount)
    {
        // Arrange
        var parser = CreateParser(["text", "otherText"]);
        string[] expectedFields = fieldCount == 1 ? ["text"] : ["text", "otherText"];

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("query_string", actual.Name);
        Assert.Equal(value, actual.Value.GetProperty("query").GetString());
        Assert.True(actual.Value.GetProperty("analyze_wildcard").GetBoolean());
        Assert.True(actual.Value.GetProperty("allow_leading_wildcard").GetBoolean());
        Assert.Equal(expectedFields, actual.Value.GetProperty("fields").EnumerateArray().Select(field => field.GetString()));
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
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal(kind, actual.Name);
        Assert.Equal(value, actual.Value.GetProperty("keyword").GetProperty("value").GetString());
    }

    [Theory]
    [InlineData("text:/foo.bar/", "text", "foo.bar")]
    [InlineData("text:/foo\\.bar/", "text", "foo\\.bar")]
    [InlineData("text:/val.*/", "text", "val.*")]
    [InlineData("keyword:/[0-9]+/", "keyword", "[0-9]+")]
    [InlineData("keyword:/val.*/", "keyword", "val.*")]
    [InlineData("keyword:/foo\\.bar/", "keyword", "foo\\.bar")]
    public async Task BuildQueryAsync_WithRegex_PreservesRegexOperatorsAndEscapes(string query, string field, string pattern)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("regexp", actual.Name);
        Assert.Equal(pattern, actual.Value.GetProperty(field).GetProperty("value").GetString());
    }

    [Theory]
    [InlineData("text:value~2", "match", "text", 2)]
    [InlineData("text:value~", "match", "text", 2)]
    [InlineData("text:value~0", "match", "text", 0)]
    [InlineData("keyword:value~1", "fuzzy", "keyword", 1)]
    [InlineData("keyword:value~2", "fuzzy", "keyword", 2)]
    public async Task BuildQueryAsync_WithFuzzyTerm_EmitsEditDistance(string query, string kind, string field, int distance)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
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
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("match_phrase", actual.Name);
        Assert.Equal(slop, actual.Value.GetProperty(field).GetProperty("slop").GetInt32());
    }

    [Theory]
    [InlineData("text:value^2", "match", "text", 2)]
    [InlineData("text:\"a b\"^2", "match_phrase", "text", 2)]
    [InlineData("keyword:value^2", "term", "keyword", 2)]
    [InlineData("text:\"a b\"^3", "match_phrase", "text", 3)]
    [InlineData("keyword:value^0", "term", "keyword", 0)]
    [InlineData("keyword:jo?n^4", "wildcard", "keyword", 4)]
    [InlineData("keyword:/val.*/^5", "regexp", "keyword", 5)]
    [InlineData("number:[1 TO 5]^2", "range", "number", 2)]
    public async Task BuildQueryAsync_WithBoost_EmitsNativeBoost(string query, string kind, string field, float boost)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal(kind, actual.Name);
        Assert.Equal(boost, actual.Value.GetProperty(field).GetProperty("boost").GetSingle());
    }

    [Fact]
    public async Task BuildQueryAsync_WithGroupBoost_BoostsTheWholeGroup()
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync("(text:a OR text:b)^8", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("bool", actual.Name);
        Assert.Equal(8, actual.Value.GetProperty("boost").GetSingle());
        Assert.Equal(2, actual.Value.GetProperty("must").GetProperty("bool").GetProperty("should").GetArrayLength());
    }

    [Theory]
    [InlineData("(text:a OR text:b)")]
    [InlineData("+(text:a OR text:b)")]
    [InlineData("@include:saved")]
    [InlineData("+@include:saved")]
    public async Task BuildQueryAsync_WithEscapedGroupBoost_MatchesUnescapedBoost(string expression)
    {
        // Arrange
        var parser = new ElasticQueryParser(configuration => configuration
            .SetLoggerFactory(Log)
            .UseMappings(_resolver)
            .UseIncludes(new Dictionary<string, string> { ["saved"] = "text:a OR text:b" }));

        // Act
        var expected = await parser.BuildQueryAsync(expression + "^2", new ElasticQueryVisitorContext { UseScoring = true });
        var actual = await parser.BuildQueryAsync(expression + @"^\+2", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var expectedJson = await SerializeQueryAsync(expected);
        using var actualJson = await SerializeQueryAsync(actual);

        Assert.Equal(expectedJson.RootElement.GetRawText(), actualJson.RootElement.GetRawText());
    }

    [Theory]
    [InlineData("keyword:*john", "*john")]
    [InlineData("text:?ohn", "?ohn")]
    [InlineData("*john", "*john")]
    public async Task BuildQueryAsync_WithDisallowedLeadingWildcard_ReportsOneDiagnostic(string query, string term)
    {
        // Arrange
        var parser = CreateParser();
        var options = new QueryValidationOptions { AllowLeadingWildcards = false };
        var context = new ElasticQueryVisitorContext();
        context.SetValidationOptions(options);
        string expectedMessage = "Terms must not start with a wildcard: " + term;

        // Act
        var validation = await parser.ValidateQueryAsync(query, options);
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(query, context));

        // Assert
        Assert.Equal(expectedMessage, Assert.Single(validation.ValidationErrors).Message);
        Assert.Equal(expectedMessage, validation.Message);
        Assert.NotNull(exception.Result);
        Assert.Equal(expectedMessage, Assert.Single(exception.Result.ValidationErrors).Message);
        Assert.Equal("Invalid query: " + expectedMessage, exception.Message);
    }

    [Theory]
    [InlineData("*john")]
    [InlineData("?ohn")]
    public async Task GetDefaultQueryAsync_WithDisallowedLeadingWildcard_ReportsOneDiagnostic(string term)
    {
        // Arrange
        var node = new TermNode { Field = "keyword", Term = term };
        var context = new ElasticQueryVisitorContext();
        context.SetValidationOptions(new QueryValidationOptions { AllowLeadingWildcards = false });

        // Act
        var query = await node.GetDefaultQueryAsync(context);

        // Assert
        Assert.NotNull(query?.MatchNone);
        Assert.Equal("Terms must not start with a wildcard: " + term, Assert.Single(context.GetValidationErrors()).Message);
    }

    [Theory]
    [InlineData("text")]
    [InlineData(null)]
    public async Task GetDefaultQueryAsync_WithMissingTerm_ReturnsNoQuery(string? field)
    {
        // Arrange
        var node = new TermNode { Field = field };
        var context = new ElasticQueryVisitorContext { DefaultFields = ["text", "keyword"] };

        // Act
        var query = await node.GetDefaultQueryAsync(context);

        // Assert
        Assert.Null(query);
        Assert.Empty(context.GetValidationErrors());
    }

    [Theory]
    [InlineData("john*", "query_string")]
    [InlineData("/john.*/", "query_string")]
    [InlineData("john~1", "multi_match")]
    [InlineData("\"alpha beta\"~2", "multi_match")]
    public async Task BuildQueryAsync_WithoutDefaultFields_RetainsTermSyntax(string query, string kind)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        using var json = await SerializeQueryAsync(result);
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
        // Arrange
        var parser = CreateParser();

        // Act
        var validation = await parser.ValidateQueryAsync(query);
        var error = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(query));

        // Assert
        Assert.False(validation.IsValid);
        Assert.NotNull(error.Result);
    }

    [Fact]
    public async Task BuildQueryAsync_WithBoost_UsesInvariantCulture()
    {
        // Arrange
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            var parser = CreateParser();

            // Act
            var result = await parser.BuildQueryAsync("text:value^1.5", new ElasticQueryVisitorContext { UseScoring = true });

            // Assert
            using var json = await SerializeQueryAsync(result);

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
        // Arrange
        var parser = new ElasticQueryParser(configuration => configuration
            .SetLoggerFactory(Log)
            .UseMappings(_resolver)
            .UseFieldMap(new Dictionary<string, string> { ["alias"] = "keyword" })
            .UseIncludes(new Dictionary<string, string> { ["saved"] = fragment }));
        var options = new QueryValidationOptions { AllowedFields = { "alias" } };
        var context = new ElasticQueryVisitorContext { UseScoring = true };
        context.SetValidationOptions(options);

        // Act
        var query = await parser.BuildQueryAsync("@include:saved", context);
        var restricted = await parser.ValidateQueryAsync("@include:saved", new QueryValidationOptions { RestrictedFields = { "alias" } });
        var unresolved = await parser.ValidateQueryAsync("@include:missing");

        // Assert
        using var json = await SerializeQueryAsync(query);
        var actual = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal(kind, actual.Name);
        Assert.Equal(value, actual.Value.GetProperty("keyword").GetProperty("value").GetString());
        Assert.Contains("alias", context.GetValidationResult().ReferencedFields);
        Assert.Contains("saved", context.GetValidationResult().ReferencedIncludes);
        Assert.False(restricted.IsValid);
        Assert.False(unresolved.IsValid);
    }

    public override ValueTask DisposeAsync()
    {
        _resolver.Dispose();
        return base.DisposeAsync();
    }

    private ElasticQueryParser CreateParser(string[]? defaultFields = null) => new(configuration =>
    {
        configuration.SetLoggerFactory(Log).UseMappings(_resolver);
        if (defaultFields is not null)
            configuration.SetDefaultFields(defaultFields);
    });

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

    private static async Task<JsonDocument> SerializeQueryAsync(Query query)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var client = new ElasticsearchClient(settings);
        using var stream = new MemoryStream();
        client.RequestResponseSerializer.Serialize(query, stream);
        stream.Position = 0;
        return await JsonDocument.ParseAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
    }
}
