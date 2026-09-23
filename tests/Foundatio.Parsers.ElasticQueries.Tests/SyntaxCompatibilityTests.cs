using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
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

    [Fact]
    public async Task ParseAsync_WithConcurrentRequiredIncludes_KeepsContextsIndependent()
    {
        // Arrange
        const int requestCount = 32;
        int arrived = 0;
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parser = new ElasticQueryParser(configuration => configuration
            .UseMappings(_resolver)
            .UseIncludes(async name =>
            {
                if (Interlocked.Increment(ref arrived) is requestCount)
                    ready.SetResult();

                await ready.Task.WaitAsync(TimeSpan.FromSeconds(10), TestCancellationToken);
                return $"keyword:{name}";
            }));

        // Act
        var results = await Task.WhenAll(Enumerable.Range(0, requestCount).Select(index => Task.Run(async () =>
        {
            var context = new ElasticQueryVisitorContext { DefaultOperator = GroupOperator.Or, UseScoring = index % 2 is 0 };
            var node = await parser.ParseAsync($"+@include:value{index} keyword:optional", context);
            return (Index: index, Node: node, Context: context);
        }, TestCancellationToken)));

        // Assert
        foreach (var result in results)
        {
            Assert.True(result.Context.IsValid(), result.Context.GetValidationResult().Message);
            Assert.Equal($"value{result.Index}", Assert.Single(result.Context.GetValidationResult().ReferencedIncludes));
            var root = Assert.IsType<GroupNode>(result.Node);
            var required = Assert.IsType<GroupNode>(root.Left);
            Assert.Equal("+", required.Prefix);
            var included = Assert.IsType<GroupNode>(required.Left);
            Assert.Equal($"value{result.Index}", Assert.IsType<TermNode>(included.Left).Term);
        }
    }

    [Theory]
    [InlineData("date:(date~1d @offset:\"-6h\")", "-6h")]
    [InlineData("date:(date~1d @offset:\"+6h\")", "+6h")]
    [InlineData("date:(date~1d @offset:6h)", "6h")]
    public async Task BuildAggregationsAsync_WithLiteralOffset_PreservesSignedValue(string expression, string expected)
    {
        // Arrange
        var parser = CreateParser();

        // Act
        var aggregations = await parser.BuildAggregationsAsync(expression);

        // Assert
        Assert.NotNull(aggregations);
        var histogram = Assert.Single(aggregations.ToDictionary()).Value.DateHistogram;
        Assert.NotNull(histogram);
        Assert.Equal(expected, histogram.Offset);
    }

    [Theory]
    [InlineData("date:(date~1d @offset:-6h)")]
    [InlineData("date:(date~1d @offset:+6h)")]
    public async Task BuildAggregationsAsync_WithPostColonOffsetOperator_ReportsMigrationError(string expression)
    {
        // Arrange
        var parser = CreateParser();

        // Act & Assert
        var error = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildAggregationsAsync(expression));
        Assert.Contains("before the field name", error.Message);
    }

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
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
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
    [InlineData(false)]
    [InlineData(true)]
    public async Task BuildQueryAsync_WithRequiredCustomGroup_PreservesCustomQuery(bool scoring)
    {
        // Arrange
        var parser = new ElasticQueryParser(configuration => configuration
            .UseMappings(_resolver)
            .AddQueryVisitor(new CustomFilterVisitor()));
        var context = new ElasticQueryVisitorContext { DefaultOperator = GroupOperator.Or, UseScoring = scoring };

        // Act
        var query = await parser.BuildQueryAsync("+@custom:(one) keyword:b", context);

        // Assert
        var clauses = scoring ? query.Bool : Assert.Single(query.Bool!.Filter!).Bool;
        Assert.NotNull(clauses);
        var required = Assert.Single(scoring ? clauses.Must! : clauses.Filter!).Terms;
        Assert.NotNull(required);
        Assert.Equal("id", required.Field!.ToString());
        Assert.Equal("b", Assert.Single(clauses.Should!).Term!.Value.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BuildQueryAsync_WithRequiredAndOptionalClauses_PreservesBooleanScope(bool scoring)
    {
        // Arrange
        var parser = new ElasticQueryParser(configuration => configuration.UseMappings(_resolver));
        var context = new ElasticQueryVisitorContext { DefaultOperator = GroupOperator.Or, UseScoring = scoring };

        // Act
        var query = await parser.BuildQueryAsync("+(keyword:a OR keyword:b) keyword:c", context);

        // Assert
        var clauses = scoring ? query.Bool : Assert.Single(query.Bool!.Filter!).Bool;
        Assert.NotNull(clauses);
        var required = Assert.Single(scoring ? clauses.Must! : clauses.Filter!).Bool;
        Assert.NotNull(required);
        Assert.Equal(2, required.Should!.Count);
        Assert.Null(required.Must);
        Assert.Null(required.Filter);
        Assert.Equal("c", Assert.Single(clauses.Should!).Term!.Value.ToString());
    }

    [Theory]
    [InlineData(GroupOperator.And)]
    [InlineData(GroupOperator.Or)]
    public Task BuildQueryAsync_WithUntranslatableRequiredClause_ThrowsValidationException(GroupOperator op)
    {
        // Arrange
        var parser = new ElasticQueryParser(configuration => configuration.UseMappings(_resolver));
        var context = new ElasticQueryVisitorContext { DefaultOperator = op };

        // Act & Assert
        return Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync("+[1 TO 5] keyword:a", context));
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

    [Theory]
    [InlineData("+max:price", SortOrder.Asc)]
    [InlineData("-max:price", SortOrder.Desc)]
    public async Task BuildAggregationsAsync_WithExplicitMaxDirection_AppliesTermsOrder(string metric, SortOrder expectedOrder)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));

        // Act
        var aggregations = await parser.BuildAggregationsAsync($"terms:(category {metric})");

        // Assert
        Assert.NotNull(aggregations);
        var terms = aggregations.ToDictionary()["terms_category"];
        var order = Assert.Single(terms.Terms!.Order!);
        Assert.Equal(new Field("max_price"), order.Key);
        Assert.Equal(expectedOrder, order.Value);
        Assert.NotNull(terms.Aggregations!["max_price"].Max);
    }

    [Theory]
    [InlineData("terms:(!category)", "!", "category")]
    [InlineData("terms:(NOT category)", "NOT", "category")]
    [InlineData("terms:(NOT +category)", "NOT", "category")]
    [InlineData("terms:(NOT -category)", "NOT", "category")]
    [InlineData("+terms:(!category)", "!", "category")]
    [InlineData("-terms:(NOT +category)", "NOT", "category")]
    [InlineData("terms:(!category -max:price)", "!", "category")]
    [InlineData("terms:(category +max:(NOT price))", "NOT", "price")]
    public async Task BuildAggregationsAsync_WithNegatedPrimaryField_MatchesValidationError(string aggregations, string expectedOperator, string expectedField)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        string expectedMessage = $"Boolean operator ({expectedOperator}) is not supported in aggregation expressions for field ({expectedField}): use + for ascending or - for descending order.";

        // Act
        var result = await parser.ValidateAggregationsAsync(aggregations);
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildAggregationsAsync(aggregations));

        // Assert
        Assert.False(result.IsValid);
        Assert.Equal(expectedMessage, Assert.Single(result.ValidationErrors).Message);
        Assert.NotNull(exception.Result);
        Assert.Equal(expectedMessage, Assert.Single(exception.Result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData("!price", "!")]
    [InlineData("NOT price", "NOT")]
    [InlineData("NOT +price", "NOT")]
    [InlineData("NOT -price", "NOT")]
    [InlineData("!(price name)", "!")]
    [InlineData("NOT (price name)", "NOT")]
    public async Task BuildSortAsync_WithBooleanNegation_MatchesValidationError(string sort, string expectedOperator)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));

        // Act
        var result = await parser.ValidateSortAsync(sort);
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildSortAsync(sort));

        // Assert
        Assert.False(result.IsValid);
        string expectedMessage = Assert.Single(result.ValidationErrors).Message;
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", expectedMessage);
        Assert.NotNull(exception.Result);
        Assert.Equal(expectedMessage, Assert.Single(exception.Result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData("!@include:ordering", "!")]
    [InlineData("NOT @include:ordering", "NOT")]
    public async Task BuildSortAsync_WithNegatedInclude_ThrowsValidationException(string sort, string expectedOperator)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseIncludes(_ => "price"));
        var context = new ElasticQueryVisitorContext();
        context.SetIncludeResolver(_ => Task.FromResult<string?>("price"));

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildSortAsync(sort, context));

        // Assert
        Assert.NotNull(exception.Result);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", Assert.Single(exception.Result.ValidationErrors).Message);
        Assert.Empty(exception.Result.UnresolvedIncludes);
    }

    [Fact]
    public async Task BuildSortAsync_WithDescendingGroup_PreservesExplicitAscendingOverride()
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));

        // Act
        var sort = await parser.BuildSortAsync("-(price name +rank)");

        // Assert
        Assert.Collection(sort,
            item => Assert.Equal(SortOrder.Desc, item.Field!.Order),
            item => Assert.Equal(SortOrder.Desc, item.Field!.Order),
            item => Assert.Equal(SortOrder.Asc, item.Field!.Order));
    }

    [Theory]
    [InlineData("terms:(!category)")]
    [InlineData("terms:(NOT category)")]
    public async Task ValidateAggregationsAsync_WithThrowingOptions_RejectsNegatedPrimaryField(string expression)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var options = new QueryValidationOptions { ShouldThrow = true };

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.ValidateAggregationsAsync(expression, options));

        // Assert
        Assert.NotNull(exception.Result);
        Assert.Contains("is not supported in aggregation expressions", Assert.Single(exception.Result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData("!@include:ordering", "!")]
    [InlineData("NOT @include:ordering", "NOT")]
    public async Task BuildAggregationsAsync_WithNegatedInclude_ThrowsValidationException(string expression, string expectedOperator)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseIncludes(_ => "max:price", priority: -1));
        var context = new ElasticQueryVisitorContext();
        context.SetIncludeResolver(_ => Task.FromResult<string?>("max:price"));

        // Act
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildAggregationsAsync(expression, context));

        // Assert
        Assert.NotNull(exception.Result);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in aggregation expressions", Assert.Single(exception.Result.ValidationErrors).Message);
        Assert.Empty(exception.Result.UnresolvedIncludes);
    }

    [Theory]
    [InlineData(QueryTypes.Sort, "!max:+price")]
    [InlineData(QueryTypes.Sort, "+max:!(price)")]
    [InlineData(QueryTypes.Sort, "!@include:+ordering")]
    [InlineData(QueryTypes.Aggregation, "!max:-price")]
    [InlineData(QueryTypes.Aggregation, "-max:!(price)")]
    [InlineData(QueryTypes.Aggregation, "!@include:-ordering")]
    public async Task BuildAsync_WithPostColonOrderingOperator_ReportsParseError(string queryType, string expression)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));

        // Act
        var error = queryType is QueryTypes.Sort
            ? await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildSortAsync(expression))
            : await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildAggregationsAsync(expression));

        // Assert
        Assert.Contains("before the field name", error.Message);
        Assert.NotNull(error.Result);
        Assert.True(Assert.Single(error.Result.ValidationErrors).Index > 0);
    }

    [Theory]
    [InlineData(QueryTypes.Sort, @"max:\!price")]
    [InlineData(QueryTypes.Sort, "max:\"!price\"")]
    [InlineData(QueryTypes.Sort, "max:/!price/")]
    [InlineData(QueryTypes.Sort, "max:\"NOT price\"")]
    [InlineData(QueryTypes.Aggregation, @"max:\!price")]
    [InlineData(QueryTypes.Aggregation, "max:\"!price\"")]
    [InlineData(QueryTypes.Aggregation, "max:/!price/")]
    [InlineData(QueryTypes.Aggregation, "max:\"NOT price\"")]
    public async Task ValidateAsync_WithLiteralOrderingOperatorText_RemainsValid(string queryType, string expression)
    {
        // Arrange
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));

        // Act
        var result = queryType is QueryTypes.Sort
            ? await parser.ValidateSortAsync(expression)
            : await parser.ValidateAggregationsAsync(expression);

        // Assert
        Assert.True(result.IsValid, result.Message);
    }

    [Theory]
    [InlineData(QueryTypes.Sort, "!price")]
    [InlineData(QueryTypes.Sort, "NOT +price")]
    [InlineData(QueryTypes.Aggregation, "!max:price")]
    [InlineData(QueryTypes.Aggregation, "NOT max:price")]
    public async Task BuildAsync_WithNegationInsideNestedInclude_RejectsOrdering(string queryType, string expression)
    {
        // Arrange
        Task<string?> Resolve(string name) => Task.FromResult<string?>(name is "outer" ? "@include:inner" : expression);
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseIncludes(Resolve, priority: -1));
        var context = new ElasticQueryVisitorContext();
        context.SetIncludeResolver(Resolve);

        // Act
        var error = queryType is QueryTypes.Sort
            ? await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildSortAsync("@include:outer", context))
            : await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildAggregationsAsync("@include:outer", context));

        // Assert
        Assert.NotNull(error.Result);
        Assert.Contains("is not supported", Assert.Single(error.Result.ValidationErrors).Message);
        Assert.Empty(error.Result.UnresolvedIncludes);
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
