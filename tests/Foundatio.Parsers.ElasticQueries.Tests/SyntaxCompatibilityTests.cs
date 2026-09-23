using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

// Characterizes the default query pipeline for docs/guide/syntax-compatibility.md.
// The unsupported modifier cases describe the current limitations tracked in #278,
// not desired behavior. Update these assertions and the documentation when fixing them.
public class SyntaxCompatibilityTests : TestWithLoggingBase
{
    private readonly ElasticMappingResolver _resolver = new(() => new TypeMapping
    {
        Properties = new Properties
            {
                { "text", new TextProperty() },
                { "otherText", new TextProperty() },
                { "keyword", new KeywordProperty() },
                { "date", new DateProperty() },
                { "dateNanos", new DateNanosProperty() },
                { "location", new GeoPointProperty() },
                { "city", new TextProperty() }
            }
    });

    public SyntaxCompatibilityTests(ITestOutputHelper output) : base(output) { }

    [Theory]
    [InlineData("field:1..5", "field", "1..5")]
    [InlineData("1..5", null, "1..5")]
    [InlineData("field:jo?n", "field", "jo?n")]
    [InlineData("field.with.dots:value", "field.with.dots", "value")]
    [InlineData("first\\ name:Alice", "first name", "Alice")]
    public void Parse_WithOrdinaryTerm_PreservesFieldAndValue(string query, string? field, string value)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var root = parser.Parse(query);

        // Assert
        var term = Assert.IsType<TermNode>(root.Left);

        Assert.Equal(field, term.UnescapedField);
        Assert.Equal(value, term.UnescapedTerm);
        Assert.Null(root.Right);
    }

    [Theory]
    [InlineData("field:[1 TO 5]", true, true)]
    [InlineData("field:[1 .. 5]", true, true)]
    [InlineData("field:[1..5]", true, true)]
    [InlineData("field:{1 .. 5}", false, false)]
    [InlineData("field:[1 .. 5}", true, false)]
    [InlineData("field:{1 .. 5]", false, true)]
    public void Parse_WithBracketedRange_PreservesBounds(string query, bool minInclusive, bool maxInclusive)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var root = parser.Parse(query);

        // Assert
        var range = Assert.IsType<TermRangeNode>(root.Left);

        Assert.Equal("field", range.Field);
        Assert.Equal("1", range.Min);
        Assert.Equal("5", range.Max);
        Assert.Equal(minInclusive, range.MinInclusive);
        Assert.Equal(maxInclusive, range.MaxInclusive);
        Assert.Null(root.Right);
    }

    [Theory]
    [InlineData("field\\.with\\.dots:value")]
    [InlineData("field:-[1 TO 5]")]
    [InlineData("field:NOT [1 TO 5]")]
    public void Parse_WithUnsupportedSyntax_ThrowsFormatException(string query)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        Action parse = () => parser.Parse(query);

        // Assert
        Assert.Throws<FormatException>(parse);
    }

    [Theory]
    [InlineData("text:value~2", "2", null, false, false)]
    [InlineData("text:\"a b\"~5", "5", null, true, false)]
    [InlineData("text:value^2", null, "2", false, false)]
    [InlineData("text:/foo.bar/", null, null, false, true)]
    public void Parse_WithModifier_PreservesAstMetadata(string query, string? proximity, string? boost, bool quoted, bool regex)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var root = parser.Parse(query);

        // Assert
        var term = Assert.IsType<TermNode>(root.Left);

        Assert.Equal(proximity, term.Proximity);
        Assert.Equal(boost, term.Boost);
        Assert.Equal(quoted, term.IsQuotedTerm);
        Assert.Equal(regex, term.IsRegexTerm);
    }

    [Theory]
    [InlineData("text:1..5", "text", "1..5")]
    [InlineData("text:jo?n", "text", "jo?n")]
    [InlineData("text:jo*n", "text", "jo*n")]
    [InlineData("text:*john", "text", "*john")]
    [InlineData("text:/foo.bar/", "text", "foo.bar")]
    [InlineData("text:value~2", "text", "value")]
    [InlineData("text:value^2", "text", "value")]
    public async Task BuildQueryAsync_WithTextTerm_EmitsMatchWithoutModifiers(string query, string field, string value)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        using var json = await BuildQueryJsonAsync(parser, query);

        // Assert
        var match = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("match", match.Name);
        var options = match.Value.GetProperty(field);
        Assert.Equal(value, options.GetProperty("query").GetString());
        Assert.False(options.TryGetProperty("fuzziness", out _));
        Assert.False(options.TryGetProperty("boost", out _));
    }

    [Theory]
    [InlineData("keyword:1..5", "1..5")]
    [InlineData("keyword:jo?n", "jo?n")]
    [InlineData("keyword:jo*n", "jo*n")]
    [InlineData("keyword:*john", "*john")]
    [InlineData("keyword:/[0-9]+/", "[0-9]+")]
    [InlineData("keyword:value~2", "value")]
    [InlineData("keyword:value^2", "value")]
    public async Task BuildQueryAsync_WithKeywordTerm_EmitsTermWithoutModifiers(string query, string value)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        using var json = await BuildQueryJsonAsync(parser, query);

        // Assert
        var term = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("term", term.Name);
        var options = term.Value.GetProperty("keyword");
        Assert.Equal(value, options.GetProperty("value").GetString());
        Assert.False(options.TryGetProperty("boost", out _));
    }

    [Theory]
    [InlineData("text:\"a b\"~5")]
    [InlineData("text:\"a b\"^2")]
    public async Task BuildQueryAsync_WithPhraseModifier_EmitsMatchPhraseWithoutSlopOrBoost(string query)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        using var json = await BuildQueryJsonAsync(parser, query);

        // Assert
        var phrase = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("match_phrase", phrase.Name);
        var options = phrase.Value.GetProperty("text");
        Assert.Equal("a b", options.GetProperty("query").GetString());
        Assert.False(options.TryGetProperty("slop", out _));
        Assert.False(options.TryGetProperty("boost", out _));
    }

    [Theory]
    [InlineData("text:john*", "john*")]
    [InlineData("text:jo?n*", "jo?n*")]
    [InlineData("text:/val.*/", "val.*")]
    [InlineData("text:john\\*", "john*")]
    [InlineData("john*", "john*")]
    public async Task BuildQueryAsync_WithAnalyzedTrailingStar_EmitsExplicitWildcardOptions(string query, string value)
    {
        // Arrange
        var parser = CreateParser(["text", "otherText"]);

        // Act
        using var json = await BuildQueryJsonAsync(parser, query);

        // Assert
        var queryString = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("query_string", queryString.Name);
        Assert.Equal(value, queryString.Value.GetProperty("query").GetString());
        Assert.True(queryString.Value.GetProperty("analyze_wildcard").GetBoolean());
        Assert.False(queryString.Value.GetProperty("allow_leading_wildcard").GetBoolean());
        Assert.Equal(query.StartsWith("text:", StringComparison.Ordinal) ? 1 : 2,
            queryString.Value.GetProperty("fields").GetArrayLength());
    }

    [Theory]
    [InlineData("keyword:john*", "john")]
    [InlineData("keyword:jo?n*", "jo?n")]
    [InlineData("keyword:/val.*/", "val.")]
    [InlineData("keyword:john\\*", "john")]
    public async Task BuildQueryAsync_WithKeywordTrailingStar_EmitsLiteralPrefix(string query, string value)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        using var json = await BuildQueryJsonAsync(parser, query);

        // Assert
        var prefix = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("prefix", prefix.Name);
        Assert.Equal(value, prefix.Value.GetProperty("keyword").GetProperty("value").GetString());
    }

    [Theory]
    [InlineData("date", "America/Chicago")]
    [InlineData("dateNanos", "America/Chicago")]
    [InlineData("date", "2")]
    public async Task BuildQueryAsync_WithDateRangeCaret_EmitsTimeZoneNotBoost(string field, string timeZone)
    {
        // Arrange
        var parser = CreateParser();
        string query = $"{field}:[2024-01-01 TO *]^\"{timeZone}\"";

        // Act
        using var json = await BuildQueryJsonAsync(parser, query);

        // Assert
        var range = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("range", range.Name);
        var options = range.Value.GetProperty(field);
        Assert.Equal("2024-01-01", options.GetProperty("gte").GetString());
        Assert.Equal(timeZone, options.GetProperty("time_zone").GetString());
        Assert.False(options.TryGetProperty("boost", out _));
    }

    [Fact]
    public async Task BuildQueryAsync_WithoutDefaultFields_DoesNotUseTrailingStarQueryStringPath()
    {
        // Arrange
        var parser = CreateParser();

        // Act
        using var json = await BuildQueryJsonAsync(parser, "john*");

        // Assert
        var multiMatch = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("multi_match", multiMatch.Name);
        Assert.Equal("john*", multiMatch.Value.GetProperty("query").GetString());
    }

    [Fact]
    public async Task BuildQueryAsync_WithQuotedCity_ResolvesWholeNameAndBuildsGeoDistance()
    {
        // Arrange
        string? resolvedLocation = null;
        var parser = new ElasticQueryParser(c => c.UseMappings(_resolver).UseGeo(location =>
        {
            resolvedLocation = location;
            return "40.7128,-74.0060";
        }));

        // Act
        using var json = await BuildQueryJsonAsync(parser, "location:\"New York, NY\"~75mi");

        // Assert
        Assert.Equal("New York, NY", resolvedLocation);
        var geo = json.RootElement.GetProperty("geo_distance");
        Assert.Equal("75mi", geo.GetProperty("distance").GetString());
        Assert.Equal("40.7128,-74.0060", geo.GetProperty("location").GetString());
    }

    [Fact]
    public async Task BuildQueryAsync_WithQuotedFieldGroup_PreservesPhraseAndFieldScope()
    {
        // Arrange
        var parser = CreateParser();

        // Act
        using var json = await BuildQueryJsonAsync(parser, "city:(\"New York\" OR Madison)");

        // Assert
        var clauses = json.RootElement.GetProperty("bool").GetProperty("should");
        Assert.Equal(2, clauses.GetArrayLength());
        Assert.Equal("New York", clauses[0].GetProperty("match_phrase").GetProperty("city").GetProperty("query").GetString());
        Assert.Equal("Madison", clauses[1].GetProperty("match").GetProperty("city").GetProperty("query").GetString());
    }

    [Fact]
    public async Task BuildQueryAsync_WithCoordinateBounds_DoesNotInvokeCityResolver()
    {
        // Arrange
        bool resolverCalled = false;
        var parser = new ElasticQueryParser(c => c.UseMappings(_resolver).UseGeo(location =>
        {
            resolverCalled = true;
            return location;
        }));

        // Act
        using var json = await BuildQueryJsonAsync(parser, "location:[40.92,-74.26 TO 40.49,-73.70]");

        // Assert
        Assert.False(resolverCalled);
        var bounds = json.RootElement.GetProperty("geo_bounding_box").GetProperty("location");
        Assert.Equal("40.92,-74.26", bounds.GetProperty("top_left").GetString());
        Assert.Equal("40.49,-73.70", bounds.GetProperty("bottom_right").GetString());
    }

    [Theory]
    [InlineData(' ')]
    [InlineData('+')]
    [InlineData('-')]
    [InlineData('!')]
    [InlineData('(')]
    [InlineData(')')]
    [InlineData('{')]
    [InlineData('}')]
    [InlineData('[')]
    [InlineData(']')]
    [InlineData('^')]
    [InlineData('"')]
    [InlineData('~')]
    [InlineData('*')]
    [InlineData('?')]
    [InlineData(':')]
    [InlineData('\\')]
    [InlineData('/')]
    public void Parse_WithDocumentedEscape_PreservesLiteralFieldAndTerm(char character)
    {
        // Arrange
        var parser = new LuceneQueryParser();
        string query = $"before\\{character}after:before\\{character}after";

        // Act
        var root = parser.Parse(query);

        // Assert
        var term = Assert.IsType<TermNode>(root.Left);
        Assert.Equal($"before{character}after", term.UnescapedField);
        Assert.Equal($"before{character}after", term.UnescapedTerm);
        Assert.Null(root.Right);
    }

    [Theory]
    [InlineData('.')]
    [InlineData('&')]
    [InlineData('|')]
    [InlineData('=')]
    [InlineData('<')]
    [InlineData('>')]
    public void Parse_WithUnsupportedEscape_ThrowsFormatException(char character)
    {
        // Arrange
        var parser = new LuceneQueryParser();
        string query = $"field:before\\{character}after";

        // Act
        Action parse = () => parser.Parse(query);

        // Assert
        Assert.Throws<FormatException>(parse);
    }

    private ElasticQueryParser CreateParser(string[]? defaultFields = null) => new(c =>
        {
            c.UseMappings(_resolver);
            if (defaultFields is not null)
                c.SetDefaultFields(defaultFields);
        });

    public override ValueTask DisposeAsync()
    {
        _resolver.Dispose();
        return base.DisposeAsync();
    }

    private static async Task<JsonDocument> BuildQueryJsonAsync(ElasticQueryParser parser, string query)
    {
        // Scoring mode prevents a filter wrapper from hiding missing score modifiers.
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var client = new ElasticsearchClient(settings);
        using var stream = new MemoryStream();
        client.RequestResponseSerializer.Serialize<Query>(result, stream);
        stream.Position = 0;
        return await JsonDocument.ParseAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
    }
}
