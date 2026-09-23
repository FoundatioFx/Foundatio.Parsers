using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

// Characterizes the default query pipeline for docs/guide/syntax-compatibility.md.
// The unsupported modifier cases describe the current limitations tracked in #278,
// not desired behavior. Update these assertions and the documentation when fixing them.
// Scoring contexts expose the generated query directly, without a filter wrapper.
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
    [InlineData("field:+term")]
    [InlineData("field:-term")]
    [InlineData("field:!term")]
    [InlineData("field:NOT term")]
    [InlineData("field:+(a OR b)")]
    [InlineData("field:-(a OR b)")]
    [InlineData("field:!(a OR b)")]
    [InlineData("field:NOT (a OR b)")]
    [InlineData("field:+[1 TO 2]")]
    [InlineData("field:-[1 TO 2]")]
    [InlineData("field:![1 TO 2]")]
    [InlineData("field:NOT [1 TO 2]")]
    public async Task ParseAsync_WithPostColonOperator_PreservesValidationContract(string query)
    {
        // Arrange
        var parser = new ElasticQueryParser();
        var context = new ElasticQueryVisitorContext();

        // Act
        var result = await parser.ParseAsync(query, context);
        var validation = await parser.ValidateQueryAsync(query);

        // Assert
        Assert.Null(result);
        var error = Assert.Single(context.GetValidationResult().ValidationErrors);
        Assert.Contains("before the field name", error.Message);
        Assert.True(error.Index > 0);
        Assert.False(validation.IsValid);
        Assert.Contains("before the field name", validation.Message);
        await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync(query));
    }

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

        // Act & Assert
        Assert.Throws<FormatException>(() => parser.Parse(query));
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
    [InlineData("text:/foo\\.bar/", "text", "foo.bar")]
    [InlineData("text:value~2", "text", "value")]
    [InlineData("text:value^2", "text", "value")]
    public async Task BuildQueryAsync_WithTextTerm_EmitsMatchWithoutModifiers(string query, string field, string value)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var match = Assert.IsType<MatchQuery>(result.Match);

        Assert.Equal(field, match.Field.Name);
        Assert.Equal(value, match.Query);
        Assert.Null(match.Fuzziness);
        Assert.Null(match.Boost);
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
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var term = Assert.IsType<TermQuery>(result.Term);

        Assert.Equal("keyword", term.Field.Name);
        Assert.True(term.Value.TryGetString(out string? actualValue));
        Assert.Equal(value, actualValue);
        Assert.Null(term.Boost);
    }

    [Theory]
    [InlineData("text:\"a b\"~5")]
    [InlineData("text:\"a b\"^2")]
    public async Task BuildQueryAsync_WithPhraseModifier_EmitsMatchPhraseWithoutSlopOrBoost(string query)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var phrase = Assert.IsType<MatchPhraseQuery>(result.MatchPhrase);

        Assert.Equal("text", phrase.Field.Name);
        Assert.Equal("a b", phrase.Query);
        Assert.Null(phrase.Slop);
        Assert.Null(phrase.Boost);
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
        string[] expectedFields = query.StartsWith("text:", StringComparison.Ordinal) ? ["text"] : ["text", "otherText"];

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var queryString = Assert.IsType<QueryStringQuery>(result.QueryString);

        Assert.Equal(value, queryString.Query);
        Assert.True(queryString.AnalyzeWildcard);
        Assert.False(queryString.AllowLeadingWildcard);
        Assert.NotNull(queryString.Fields);
        Assert.Equal(expectedFields, queryString.Fields.Select(field => field.Name));
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
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var prefix = Assert.IsType<PrefixQuery>(result.Prefix);

        Assert.Equal("keyword", prefix.Field.Name);
        Assert.Equal(value, prefix.Value);
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
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var range = Assert.IsType<DateRangeQuery>(result.Range);

        Assert.Equal(field, range.Field.Name);
        Assert.Equal("2024-01-01", range.Gte?.ToString());
        Assert.Equal(timeZone, range.TimeZone);
        Assert.Null(range.Boost);
    }

    [Fact]
    public async Task BuildQueryAsync_WithoutDefaultFields_DoesNotUseTrailingStarQueryStringPath()
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync("john*", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var multiMatch = Assert.IsType<MultiMatchQuery>(result.MultiMatch);

        Assert.Equal("john*", multiMatch.Query);
    }

    [Theory]
    [InlineData("location:\"New York, NY\"~75mi", "New York, NY", "40.7128,-74.0060", "75mi")]
    [InlineData("location:10001~10mi", "10001", "40.7506,-73.9972", "10mi")]
    public async Task BuildQueryAsync_WithNamedLocation_ResolvesWholeValueAndBuildsGeoDistance(string query, string location, string coordinates, string distance)
    {
        // Arrange
        string? resolvedLocation = null;
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(_resolver).UseGeo(value =>
        {
            resolvedLocation = value;
            return coordinates;
        }));

        // Act
        var result = await parser.BuildQueryAsync(query, new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        Assert.Equal(location, resolvedLocation);

        var geo = Assert.IsType<GeoDistanceQuery>(result.GeoDistance);

        Assert.Equal("location", geo.Field.Name);
        Assert.Equal(distance, geo.Distance);
        Assert.True(geo.Location.TryGetText(out string? actualCoordinates));
        Assert.Equal(coordinates, actualCoordinates);
    }

    [Fact]
    public async Task BuildQueryAsync_WithQuotedFieldGroup_PreservesPhraseAndFieldScope()
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var result = await parser.BuildQueryAsync("city:(\"New York\" OR Madison)", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var boolean = Assert.IsType<BoolQuery>(result.Bool);

        Assert.NotNull(boolean.Should);
        Assert.Collection(boolean.Should,
            clause =>
            {
                var phrase = Assert.IsType<MatchPhraseQuery>(clause.MatchPhrase);
                Assert.Equal("city", phrase.Field.Name);
                Assert.Equal("New York", phrase.Query);
            },
            clause =>
            {
                var term = Assert.IsType<MatchQuery>(clause.Match);
                Assert.Equal("city", term.Field.Name);
                Assert.Equal("Madison", term.Query);
            });
    }

    [Fact]
    public async Task BuildQueryAsync_WithCoordinateBounds_DoesNotInvokeCityResolver()
    {
        // Arrange
        bool resolverCalled = false;
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(_resolver).UseGeo(location =>
        {
            resolverCalled = true;
            return location;
        }));

        // Act
        var result = await parser.BuildQueryAsync("location:[40.92,-74.26 TO 40.49,-73.70]", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        Assert.False(resolverCalled);

        var bounds = Assert.IsType<GeoBoundingBoxQuery>(result.GeoBoundingBox);

        Assert.Equal("location", bounds.Field.Name);
        Assert.True(bounds.BoundingBox.TryGetTopLeftBottomRight(out var corners));
        Assert.True(corners.TopLeft.TryGetText(out string? topLeft));
        Assert.True(corners.BottomRight.TryGetText(out string? bottomRight));
        Assert.Equal("40.92,-74.26", topLeft);
        Assert.Equal("40.49,-73.70", bottomRight);
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

        // Act & Assert
        Assert.Throws<FormatException>(() => parser.Parse($"field:before\\{character}after"));
        Assert.Throws<FormatException>(() => parser.Parse($"before\\{character}after:value"));
    }

    [Theory]
    [InlineData('.')]
    [InlineData('&')]
    [InlineData('|')]
    [InlineData('=')]
    [InlineData('<')]
    [InlineData('>')]
    public void Parse_WithUnescapedOrdinaryCharacter_PreservesFieldAndTerm(char character)
    {
        // Arrange
        var parser = new LuceneQueryParser();
        string value = $"before{character}after";

        // Act
        var root = parser.Parse($"{value}:{value}");

        // Assert
        var term = Assert.IsType<TermNode>(root.Left);

        Assert.Equal(value, term.UnescapedField);
        Assert.Equal(value, term.UnescapedTerm);
        Assert.Null(root.Right);
    }

    [Theory]
    [InlineData('.')]
    [InlineData('&')]
    [InlineData('|')]
    [InlineData('=')]
    [InlineData('<')]
    [InlineData('>')]
    public void Parse_WithQuotedEscape_PreservesRawTermAndUnescapesValue(char character)
    {
        // Arrange
        var parser = new LuceneQueryParser();
        string value = $"before\\{character}after";

        // Act
        var root = parser.Parse($"field:\"{value}\"");

        // Assert
        var term = Assert.IsType<TermNode>(root.Left);

        Assert.True(term.IsQuotedTerm);
        Assert.Equal(value, term.Term);
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
    public void Parse_WithRegexEscape_PreservesRawTermAndUnescapesValue(char character)
    {
        // Arrange
        var parser = new LuceneQueryParser();
        string value = $"before\\{character}after";

        // Act
        var root = parser.Parse($"field:/{value}/");

        // Assert
        var term = Assert.IsType<TermNode>(root.Left);

        Assert.True(term.IsRegexTerm);
        Assert.Equal(value, term.Term);
        Assert.Equal($"before{character}after", term.UnescapedTerm);
        Assert.Null(root.Right);
    }

    private ElasticQueryParser CreateParser(string[]? defaultFields = null) => new(c =>
        {
            c.SetLoggerFactory(Log).UseMappings(_resolver);
            if (defaultFields is not null)
                c.SetDefaultFields(defaultFields);
        });

    public override ValueTask DisposeAsync()
    {
        _resolver.Dispose();
        return base.DisposeAsync();
    }
}
