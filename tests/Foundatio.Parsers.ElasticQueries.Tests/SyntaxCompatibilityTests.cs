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
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

// Characterizes the default query pipeline for docs/guide/syntax-compatibility.md.
// The unsupported modifier cases describe the current limitations tracked in #278,
// not desired behavior. Update these assertions and the documentation when fixing them.
public class SyntaxCompatibilityTests
{
    [Theory]
    [InlineData("field:1..5", "field", "1..5")]
    [InlineData("1..5", null, "1..5")]
    [InlineData("field:jo?n", "field", "jo?n")]
    [InlineData("field.with.dots:value", "field.with.dots", "value")]
    [InlineData("first\\ name:Alice", "first name", "Alice")]
    public void Parse_WithOrdinaryTerm_PreservesFieldAndValue(string query, string? field, string value)
    {
        var parser = new LuceneQueryParser();
        var root = parser.Parse(query);
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
        var root = new LuceneQueryParser().Parse(query);
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
        Assert.Throws<FormatException>(() => new LuceneQueryParser().Parse(query));
    }

    [Theory]
    [InlineData("text:value~2", "2", null, false, false)]
    [InlineData("text:\"a b\"~5", "5", null, true, false)]
    [InlineData("text:value^2", null, "2", false, false)]
    [InlineData("text:/foo.bar/", null, null, false, true)]
    public void Parse_WithModifier_PreservesAstMetadata(string query, string? proximity, string? boost, bool quoted, bool regex)
    {
        var root = new LuceneQueryParser().Parse(query);
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
        using var json = await BuildQueryJsonAsync(query);
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
        using var json = await BuildQueryJsonAsync(query);
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
        using var json = await BuildQueryJsonAsync(query);
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
        using var json = await BuildQueryJsonAsync(query, ["text", "otherText"]);
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
        using var json = await BuildQueryJsonAsync(query);
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
        using var json = await BuildQueryJsonAsync($"{field}:[2024-01-01 TO *]^\"{timeZone}\"");
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
        using var json = await BuildQueryJsonAsync("john*");
        var multiMatch = Assert.Single(json.RootElement.EnumerateObject());

        Assert.Equal("multi_match", multiMatch.Name);
        Assert.Equal("john*", multiMatch.Value.GetProperty("query").GetString());
    }

    private static async Task<JsonDocument> BuildQueryJsonAsync(string query, string[]? defaultFields = null)
    {
        using var resolver = new ElasticMappingResolver(() => new TypeMapping
        {
            Properties = new Properties
            {
                { "text", new TextProperty() },
                { "otherText", new TextProperty() },
                { "keyword", new KeywordProperty() },
                { "date", new DateProperty() },
                { "dateNanos", new DateNanosProperty() }
            }
        });
        var parser = new ElasticQueryParser(c =>
        {
            c.UseMappings(resolver);
            if (defaultFields is not null)
                c.SetDefaultFields(defaultFields);
        });

        // Scoring mode prevents a filter wrapper from hiding missing score modifiers.
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var client = new ElasticsearchClient(settings);
        using var stream = new MemoryStream();
        client.RequestResponseSerializer.Serialize<Query>(result, stream);
        stream.Position = 0;
        return JsonDocument.Parse(stream);
    }
}
