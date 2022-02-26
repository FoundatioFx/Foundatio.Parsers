using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticQueryParserTests : ElasticsearchTestBase {
    public ElasticQueryParserTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture) {
        Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    [Fact]
    public void CanUseElasticQueryParser() {
        var sut = new ElasticQueryParser();

        var result = sut.Parse("NOT (dog parrot)");

        Assert.IsType<GroupNode>(result.Left);
        Assert.True((result.Left as GroupNode).HasParens);
        Assert.True((result.Left as GroupNode).IsNegated);
    }

    [Fact]
    public void CanUseElasticQueryParserWithVisitor() {
        var testQueryVisitor = new TestQueryVisitor();
        var sut = new ElasticQueryParser(c => c.AddQueryVisitor(testQueryVisitor));
        var context = new ElasticQueryVisitorContext();

        var result = sut.Parse("NOT (dog parrot)", context) as GroupNode;
        Assert.Equal(2, testQueryVisitor.GroupNodeCount);

        Assert.IsType<GroupNode>(result.Left);
        Assert.True((result.Left as GroupNode).HasParens);
        Assert.True((result.Left as GroupNode).IsNegated);
    }

    [Fact]
    public void SimpleFilterProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field1, "value1")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void IncludeProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var includes = new Dictionary<string, string> {
                {"stuff", "field2:value2"}
            };

        var processor = new ElasticQueryParser(c => c.UseIncludes(includes).SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1 @include:stuff", new ElasticQueryVisitorContext { UseScoring = true }).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f =>
            f.Term(m => m.Field1, "value1")
            && f.Term(m => m.Field2, "value2")));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void ShouldGenerateORedTermsQuery() {
        var index = CreateRandomIndex<MyType>();
        Client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field3 = "value3" }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1 field2:value2 field3:value3",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f =>
            f.Term(m => m.Field1, "value1") || f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3")));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void ShouldHandleMultipleTermsForAnalyzedFields() {
        var index = CreateRandomIndex<MyType>(d => d
            .Dynamic().Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))
                .Text(e => e.Name(m => m.Field1).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))
                .Keyword(e => e.Name(m => m.Field2))
            ));
        Client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field3 = "value3" }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetDefaultFields(new[] { "field1" }).UseMappings(Client, index));

        var result = processor.BuildQueryAsync("field1:(value1 abc def ghi) field2:(value2 jhk)",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f =>
            f.Match(m => m.Field(mf => mf.Field1).Query("value1"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("abc"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("def"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("ghi"))
            || f.Term(m => m.Field2, "value2") || f.Term(m => m.Field2, "jhk")));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).SetDefaultFields(new[] { "field1" }).UseMappings(Client, index));
        result = processor.BuildQueryAsync("value1 abc def ghi", new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f =>
            f.Match(m => m.Field(mf => mf.Field1).Query("value1"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("abc"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("def"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("ghi"))));

        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        // multi-match on multiple default fields
        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).SetDefaultFields(new[] { "field1", "field2" }).UseMappings(Client, index));
        result = processor.BuildQueryAsync("value1 abc def ghi", new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f =>
            f.MultiMatch(m => m.Fields(mf => mf.Fields("field1", "field2")).Query("value1"))
            || f.MultiMatch(m => m.Fields(mf => mf.Fields("field1", "field2")).Query("abc"))
            || f.MultiMatch(m => m.Fields(mf => mf.Fields("field1", "field2")).Query("def"))
            || f.MultiMatch(m => m.Fields(mf => mf.Fields("field1", "field2")).Query("ghi"))));

        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanGetMappingsFromCode() {
        TypeMappingDescriptor<MyType> GetCodeMappings(TypeMappingDescriptor<MyType> d) =>
            d.Dynamic()
                .Properties(p => p
                    .GeoPoint(g => g.Name(f => f.Field3))
                    .Text(e => e.Name(m => m.Field1)));

        var index = CreateRandomIndex<MyType>(d => d.Dynamic()
            .Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field3))
                .Keyword(e => e.Name(m => m.Field2))));

        var res = Client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field4 = 1, Field5 = DateTime.Now }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var parser = new ElasticQueryParser(c => c.SetDefaultFields(new[] { "field1" }).UseMappings<MyType>(GetCodeMappings, Client, index));

        var dynamicServerMappingProperty = parser.Configuration.MappingResolver.GetMapping("field5").Property;
        var serverMappingProperty = parser.Configuration.MappingResolver.GetMapping("field2").Property;
        var codeMappingProperty = parser.Configuration.MappingResolver.GetMapping("field1").Property;
        var codeAndServerMappingProperty = parser.Configuration.MappingResolver.GetMapping("field3").Property;

        Assert.Equal("date", dynamicServerMappingProperty.Type);
        Assert.Equal("keyword", serverMappingProperty.Type);
        Assert.Equal("text", codeMappingProperty.Type);
        Assert.Equal("geo_point", codeAndServerMappingProperty.Type);
    }

    [Fact]
    public void EscapeFilterProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "\"now there\"", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = processor.BuildQueryAsync("\"\\\"now there\"").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Query(q => q
                .Bool(b => b
                    .Filter(f => f
                        .MultiMatch(m => m.Query("\"now there").Type(TextQueryType.Phrase))))));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public void CanHandleEscapedQueryWithWildcards() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "one/two/three" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = processor.BuildQueryAsync(@"field1:one\\/two*").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Query(q => q
                .Bool(b => b
                    .Filter(f => f
                        .QueryString(m => m.Query("one\\/two*").Fields(f => f.Field(f2 => f2.Field1)).AllowLeadingWildcard(false).AnalyzeWildcard())))));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public void CanHandleEscapedQuery() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "one/two/three" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = processor.BuildQueryAsync(@"field1:one\\/two").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Query(q => q
                .Bool(b => b
                    .Filter(f => f
                        .Match(m => m.Query("one\\/two").Field(f => f.Field1))))));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public void ExistsFilterProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync($"_exists_:{nameof(MyType.Field2)}").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index)
            .Query(q => q.Bool(b => b.Filter(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void MissingFilterProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync($"_missing_:{nameof(MyType.Field2)}",
                new ElasticQueryVisitorContext { UseScoring = true }).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d
                .Index(index)
                .Query(q => q.Bool(b => b.MustNot(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void MinMaxWithDateHistogramAggregation() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now },
                new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now },
                new MyType { Field2 = "value4", Field5 = DateTime.Now }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = processor.BuildAggregationsAsync("min:field2 max:field2 date:(field5~1d^\"America/Chicago\" min:field2 max:field2 min:field1 @offset:-6h)").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(i => i.Index(index).Aggregations(f => f
            .Max("max_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "text")))
            .DateHistogram("date_field5",
                d =>
                    d.Field(d2 => d2.Field5)
                        .CalendarInterval(DateInterval.Day)
                        .Format("date_optional_time")
                        .MinimumDocumentCount(0)
                        .TimeZone("America/Chicago")
                        .Offset("-6h")
                        .Meta(m2 => m2.Add("@timezone", "America/Chicago"))
                        .Aggregations(l => l
                            .Min("min_field1", m => m.Field("field1.keyword").Meta(m2 => m2.Add("@field_type", "text")))
                            .Max("max_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "text")))
                            .Min("min_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "text")))
                        ))
            .Min("min_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "text")))
        ));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    /// <summary>
    /// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html
    /// </summary>
    [InlineData("1s", DateInterval.Second)]
    [InlineData("second", DateInterval.Second)]
    [InlineData("m", DateInterval.Minute)]
    [InlineData("1m", DateInterval.Minute)]
    [InlineData("minute", DateInterval.Minute)]
    [InlineData("23m")]
    [InlineData("h", DateInterval.Hour)]
    [InlineData("1h", DateInterval.Hour)]
    [InlineData("hour", DateInterval.Hour)]
    [InlineData("1.5h")]
    [InlineData("d", DateInterval.Day)]
    [InlineData("1d", DateInterval.Day)]
    [InlineData("day", DateInterval.Day)]
    [InlineData("2d")]
    [InlineData("w", DateInterval.Week)]
    [InlineData("1w", DateInterval.Week)]
    [InlineData("week", DateInterval.Week)]
    [InlineData("M", DateInterval.Month)]
    [InlineData("1M", DateInterval.Month)]
    [InlineData("month", DateInterval.Month)]
    [InlineData("q", DateInterval.Quarter)]
    [InlineData("1q", DateInterval.Quarter)]
    [InlineData("quarter", DateInterval.Quarter)]
    [InlineData("y", DateInterval.Year)]
    [InlineData("1y", DateInterval.Year)]
    [InlineData("year", DateInterval.Year)]
    [Theory]
    public async Task CanUseDateHistogramAggregationInterval(string interval, object expectedInterval = null) {
        var index = CreateRandomIndex<MyType>();
        await Client.IndexManyAsync(new[] { new MyType { Field5 = DateTime.Now } }, index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));

        var result = await processor.BuildAggregationsAsync($"date:(field5~{interval})");
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(i => i.Index(index).Aggregations(f => f
            .DateHistogram("date_field5", d => {
                d.Field(d2 => d2.Field5);

                if (expectedInterval is DateInterval dateInterval)
                    d.CalendarInterval(dateInterval);
                else if (expectedInterval is string stringInterval)
                    d.FixedInterval(stringInterval);
                else
                    d.FixedInterval(interval);

                d.Format("date_optional_time");
                d.MinimumDocumentCount(0);

                return d;
            })
        ));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanDoNestDateHistogram() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] { new MyType { Field5 = DateTime.Now } }, index);
        Client.Indices.Refresh(index);

        var response = Client.Search<MyType>(i => i.Index(index).Aggregations(f => f
            .DateHistogram("myagg", d => d.Field(d2 => d2.Field5).CalendarInterval(DateInterval.Day))
        ));

        Assert.True(response.IsValid);
    }

    [Fact]
    public void DateAggregation() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now },
                new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now },
                new MyType { Field2 = "value4", Field5 = DateTime.Now }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildAggregationsAsync("date:field5").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(i => i.Index(index).Aggregations(f => f
            .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).CalendarInterval(DateInterval.Day).Format("date_optional_time").MinimumDocumentCount(0))
        ));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void SimpleQueryProcessor() {
        var index = CreateRandomIndex<MyType>(t => t
            .Properties(p => p
                .Text(e => e.Name(n => n.Field3).Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256))))));

        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4", Field3 = "hey now" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = processor.BuildQueryAsync("field1:value1", new ElasticQueryVisitorContext().UseSearchMode()).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.Match(e => e.Field(m => m.Field1).Query("value1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = processor.BuildQueryAsync("field3:hey", new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.Match(m => m
            .Field(f => f.Field3)
            .Query("hey")
        )));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = processor.BuildQueryAsync("field3:\"hey now\"", new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.MatchPhrase(m => m
            .Field(f => f.Field3)
            .Query("hey now")
        )));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = processor.BuildQueryAsync("field3:hey*", new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.QueryString(m => m
            .AllowLeadingWildcard(false)
            .AnalyzeWildcard(true)
            .Fields(f => f.Field(f1 => f1.Field3))
            .Query("hey*")
        )));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void NegativeQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value3" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1 AND -field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        result = processor.BuildQueryAsync("field1:value1 AND NOT field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = Client.Search<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        result = processor.BuildQueryAsync("field1:value1 OR NOT field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = Client.Search<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        result = processor.BuildQueryAsync("field1:value1 OR -field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;
        actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = Client.Search<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void NestedQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)",
                new ElasticQueryVisitorContext().UseSearchMode()).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
                .Query(f => f.Term(m => m.Field1, "value1") ||
                    (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void NestedQuery() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)").Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse =
            Client.Search<MyType>(d => d.Index(index)
                .Query(q => q.Bool(b => b.Filter(f => f
                    .Term(m => m.Field1, "value1") &&
                        (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void MixedCaseTermFilterQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.Index(new MyType { Field1 = "Testing.Casing" }, i => i.Index(index));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:Testing.Casing", new ElasticQueryVisitorContext { UseScoring = true }).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f => f.Term(m => m.Field1, "Testing.Casing")));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void MultipleWordsTermFilterQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.Index(new MyType { Field1 = "Blake Niemyjski" }, i => i.Index(index));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:\"Blake Niemyjski\"", new ElasticQueryVisitorContext { UseScoring = true }).Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(f => f.Term(p => p.Field1, "Blake Niemyjski")));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanTranslateTermQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.Index(new MyType { Field1 = "Testing.Casing" }, i => i.Index(index));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).AddVisitor(new UpdateFixedTermFieldToDateFixedExistsQueryVisitor()));
        var result = processor.BuildQueryAsync("fixed:true").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index).Query(f => f.Bool(b => b.Filter(filter => filter.Exists(m => m.Field("date_fixed"))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void GroupedOrFilterProcessor() {
        var index = CreateRandomIndex<MyType>();
        Client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)",
                new ElasticQueryVisitorContext().SetDefaultOperator(Operator.And).UseScoring()).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Query(f => f.Term(m => m.Field1, "value1") &&
                    (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void NestedFilterProcessor() {
        var index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1).Index())
            .Text(e => e.Name(n => n.Field2).Index())
            .Text(e => e.Name(n => n.Field3).Index())
            .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Index())
                .Text(e => e.Name(n => n.Field2).Index())
                .Text(e => e.Name(n => n.Field3).Index())
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));
        Client.IndexManyAsync(new[] {
                new MyNestedType {
                    Field1 = "value1",
                    Field2 = "value2",
                    Nested = {new MyType {Field1 = "value1", Field4 = 4}}
                },
                new MyNestedType {Field1 = "value2", Field2 = "value2"},
                new MyNestedType {Field1 = "value1", Field2 = "value4"}
            });
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseFieldMap(new FieldMap { { "blah", "nested" } }).UseMappings<MyNestedType>(Client).UseNested());
        var result = processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1)", new ElasticQueryVisitorContext().UseScoring()).Result;

        var actualResponse = Client.Search<MyNestedType>(d => d.Query(f => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d
            .Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n
                    .Path(p => p.Nested)
                    .Query(q2 => q2
                        .Match(m => m
                            .Field("nested.field1")
                            .Query("value1"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1 blah.field4:4)", new ElasticQueryVisitorContext().UseScoring()).Result;

        actualResponse = Client.Search<MyNestedType>(d => d.Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = Client.Search<MyNestedType>(d => d
            .Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n
                    .Path(p => p.Nested)
                    .Query(q2 => q2
                        .Match(m => m
                            .Field("nested.field1")
                            .Query("value1"))
                        && q2.Term("nested.field4", "4")))));

        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void NestedFilterProcessor2() {
        var index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1).Index())
            .Text(e => e.Name(n => n.Field2).Index())
            .Text(e => e.Name(n => n.Field3).Index())
            .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Index())
                .Text(e => e.Name(n => n.Field2).Index())
                .Text(e => e.Name(n => n.Field3).Index())
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        Client.IndexMany(new[] {
                new MyNestedType {
                    Field1 = "value1",
                    Field2 = "value2",
                    Nested = {new MyType {Field1 = "value1", Field4 = 4}}
                },
                new MyNestedType {Field1 = "value2", Field2 = "value2"},
                new MyNestedType {Field1 = "value1", Field2 = "value4", Field3 = "value3"}
            });
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());
        var result = processor.BuildQueryAsync("field1:value1 nested:(nested.field1:value1 nested.field4:4 nested.field3:value3)",
                new ElasticQueryVisitorContext { UseScoring = true }).Result;

        var actualResponse = Client.Search<MyNestedType>(d => d.Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
            && q.Nested(n => n.Path(p => p.Nested).Query(q2 =>
                q2.Match(m => m.Field("nested.field1").Query("value1"))
                && q2.Term("nested.field4", "4")
                && q2.Match(m => m.Field("nested.field3").Query("value3"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanGenerateMatchQuery() {
        var index = CreateRandomIndex<MyType>(m => m.Properties(p => p
            .Text(f => f.Name(e => e.Field1)
                .Fields(f1 => f1
                    .Keyword(k => k.Name("keyword").IgnoreAbove(256))))));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = processor.BuildQueryAsync("field1:test", new ElasticQueryVisitorContext().UseScoring()).GetAwaiter().GetResult();

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(f => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.Match(m => m.Field(e => e.Field1).Query("test"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanBuildAliasQueryProcessor() {
        var index = CreateRandomIndex<MyType>(m => m.Properties(p => p
            .Object<Dictionary<string, object>>(f => f.Name(e => e.Data).Properties(p2 => p2
                .Text(e => e.Name("@browser_version"))
                .FieldAlias(a => a.Name("browser.version").Path("data.@browser_version"))))));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("browser.version:1", new ElasticQueryVisitorContext().UseScoring()).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(f => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q => q.Term(m => m.Field("browser.version").Value("1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void NonAnalyzedPrefixQuery() {
        var index = CreateRandomIndex<MyType>(d => d.Properties(p => p.Keyword(e => e.Name(m => m.Field1))));
        Client.Index(new MyType { Field1 = "value123" }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));
        var result = processor.BuildQueryAsync("field1:value*", new ElasticQueryVisitorContext().UseSearchMode()).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index)
            .Query(f => f.Prefix(m => m.Field(f2 => f2.Field1).Value("value"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void RangeQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1 }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result =
            processor.BuildQueryAsync("field4:[1 TO 2} OR field1:value1",
                new ElasticQueryVisitorContext { UseScoring = true }).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse =
            Client.Search<MyType>(
                d =>
                    d.Index(index)
                        .Query(
                            f =>
                                f.TermRange(m => m.Field(f2 => f2.Field4).GreaterThanOrEquals("1").LessThan("2")) ||
                                f.Term(m => m.Field1, "value1")));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void DateRangeWithWildcardMinQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = processor.BuildQueryAsync("field5:[* TO 2017-01-31} OR field1:value1", ctx).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d
                .Index(index)
                .Query(f => f
                    .DateRange(m => m.Field(f2 => f2.Field5).LessThan("2017-01-31").TimeZone("America/Chicago"))
                        || f.Match(e => e.Field(m => m.Field1).Query("value1"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void DateRangeWithDateMathQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = processor.BuildQueryAsync("field5:[now-1d/d TO now/d]", ctx).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index)
            .Query(f => f.DateRange(m => m.Field(f2 => f2.Field5).GreaterThanOrEquals("now-1d/d").LessThanOrEquals("now/d").TimeZone("America/Chicago"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void DateRangeWithWildcardMaxQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = processor.BuildQueryAsync("field5:[2017-01-31 TO   *  } OR field1:value1", ctx).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index)
            .Query(f => f
                .DateRange(m => m.Field(f2 => f2.Field5).GreaterThanOrEquals("2017-01-31").TimeZone("America/Chicago"))
                    || f.Match(e => e.Field(m => m.Field1).Query("value1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void DateRangeWithTimeZone() {
        var index = CreateRandomIndex<MyType>();
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = processor.BuildQueryAsync("field5:[2017-01-31 TO   *  }^\"America/Chicago\" OR field1:value1", ctx).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d
            .Index(index)
            .Query(f => f
                .DateRange(m => m.Field(f2 => f2.Field5).GreaterThanOrEquals("2017-01-31").TimeZone("America/Chicago"))
                    || f.Match(e => e.Field(m => m.Field1).Query("value1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void DateRangeQueryProcessor() {
        var index = CreateRandomIndex<MyType>();
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index).SetLoggerFactory(Log));
        var result = processor.BuildQueryAsync("field5:[2017-01-01T00\\:00\\:00Z TO 2017-01-31} OR field1:value1", ctx).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse =
            Client.Search<MyType>(d => d
                .Index(index)
                .Query(f => f
                    .DateRange(m => m
                        .Field(f2 => f2.Field5).GreaterThanOrEquals("2017-01-01T00:00:00Z").LessThan("2017-01-31").TimeZone("America/Chicago"))
                            || f.Match(e => e.Field(m => m.Field1).Query("value1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void SimpleGeoRangeQuery() {
        var index = CreateRandomIndex<MyType>(m => m.Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" },
            i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c
            .UseMappings(Client, index)
            .UseGeo(l => "51.5032520,-0.1278990"));
        var result =
            processor.BuildQueryAsync("field3:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990]",
                new ElasticQueryVisitorContext { UseScoring = true }).Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Query(q =>
            q.GeoBoundingBox(
                m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanUseValidationToGetUnresolvedFields() {
        var index = CreateRandomIndex<MyType>(d => d.Properties(p => p.Keyword(e => e.Name(m => m.Field1))));
        Client.Index(new MyType { Field1 = "value123" }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var context = new ElasticQueryVisitorContext();
        var parser = new ElasticQueryParser(c => c.UseMappings(Client, index).UseValidation(new QueryValidationOptions { AllowUnresolvedFields = false }));
        var query = await parser.BuildQueryAsync("field1:value", context);

        var validationInfo = context.GetValidationResult();
        Assert.True(validationInfo.IsValid);
        Assert.Single(validationInfo.ReferencedFields, "field1");
        Assert.Empty(validationInfo.UnresolvedFields);

        context = new ElasticQueryVisitorContext();
        parser = new ElasticQueryParser(c => c.UseMappings(Client, index).UseValidation(new QueryValidationOptions { AllowUnresolvedFields = false }));
        query = await parser.BuildQueryAsync("field2:value", context);

        validationInfo = context.GetValidationResult();
        Assert.False(validationInfo.IsValid);
        Assert.Single(validationInfo.ReferencedFields, "field2");
        Assert.Single(validationInfo.UnresolvedFields, "field2");

        var aliasMap = new FieldMap { { "field2", "field1" } };
        context = new ElasticQueryVisitorContext();
        parser = new ElasticQueryParser(c => c.UseMappings(Client, index).UseFieldMap(aliasMap).UseValidation(new QueryValidationOptions { AllowUnresolvedFields = false }));
        query = await parser.BuildQueryAsync("field2:value", context);

        validationInfo = context.GetValidationResult();
        Assert.True(validationInfo.IsValid);
        Assert.Single(validationInfo.ReferencedFields, "field1");
        Assert.Empty(validationInfo.UnresolvedFields);

        context = new ElasticQueryVisitorContext();
        //context.RuntimeFieldResolver = f => f == "field2" ? new ElasticRuntimeField { Name = "field2" } : null;
        parser = new ElasticQueryParser(c => c.UseMappings(Client, index).UseValidation(new QueryValidationOptions { AllowUnresolvedFields = false, ShouldThrow = true }));
        var ex = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync("field2:value", context));
        Assert.Contains("resolved", ex.Message);
        Assert.Contains("field2", ex.ValidationInfo.ReferencedFields);
        Assert.Contains("field2", ex.ValidationInfo.UnresolvedFields);
        Assert.False(ex.ValidationInfo.IsValid);
        Assert.NotNull(ex.ValidationInfo.Message);
        Assert.Contains("resolved", ex.ValidationInfo.Message);
    }

    [Fact]
    public void CanSortByUnmappedField() {
        var index = CreateRandomIndex<MyType>(m => m.Dynamic());

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));
        var sort = processor.BuildSortAsync("-field1").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Sort(sort));

        Assert.True(actualResponse.IsValid);

        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualResponse);
        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Sort(
                s => s.Field(f => f.Field(new Field("field1")).Descending().UnmappedType(FieldType.Keyword))
            ));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanParseSort() {
        var index = CreateRandomIndex<MyType>(d => d.Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))
                .Text(e => e.Name(m => m.Field1).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))
                .Text(e => e.Name(m => m.Field2).Fields(f2 => f2.Keyword(e1 => e1.Name("keyword")).Keyword(e2 => e2.Name("sort"))))
            ));
        var res = Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" },
            i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var aliasMap = new FieldMap { { "geo", "field3" } };
        var processor = new ElasticQueryParser(c => c
            .UseMappings(Client, index)
            .UseFieldMap(aliasMap));
        var sort = processor.BuildSortAsync("geo -field1 -(field2 field3 +field4) (field5 field3)").Result;

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Sort(s => s
            .Field(f => f.Field(new Field("field3")).Ascending().UnmappedType(FieldType.GeoPoint))
            .Field(f => f.Field(new Field("field1.keyword")).Descending().UnmappedType(FieldType.Keyword))
            .Field(f => f.Field(new Field("field2.sort")).Descending().UnmappedType(FieldType.Keyword))
            .Field(f => f.Field(new Field("field3")).Descending().UnmappedType(FieldType.GeoPoint))
            .Field(f => f.Field(new Field("field4")).Ascending().UnmappedType(FieldType.Long))
            .Field(f => f.Field(new Field("field5")).Ascending().UnmappedType(FieldType.Date))
            .Field(f => f.Field(new Field("field3")).Ascending().UnmappedType(FieldType.GeoPoint))
        ));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanHandleSpacedFields() {
        var index = CreateRandomIndex<MyNestedType>();

        Client.IndexMany(new[] {
                new MyNestedType { Field1 = "value1", Field2 = "value2", Nested = new MyType[] {
                    new MyType { Field1 = "banana", Data = {
                        { "number-0001", 23 },
                        { "text-0001", "Hey" },
                        { "spaced field", "hey" }
                    }}
                }},
                new MyNestedType { Field1 = "value2", Field2 = "value2" },
                new MyNestedType { Field1 = "value1", Field2 = "value4" }
            }, index);
        Client.Indices.Refresh(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var sort = processor.BuildSortAsync("nested.data.spaced\\ field").Result;
        var query = processor.BuildQueryAsync("nested.data.spaced\\ field:hey").Result;
        var aggs = processor.BuildAggregationsAsync("terms:nested.data.spaced\\ field").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Sort(sort).Query(q => query).Aggregations(a => aggs));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Sort(s => s
                .Field(f => f.Field("nested.data.spaced field.keyword").UnmappedType(FieldType.Keyword).Ascending()))
            .Query(q => q.Bool(b => b.Filter(f => f
                .Match(f => f.Field("nested.data.spaced field").Query("hey")))))
            .Aggregations(a => a
                .Terms("terms_nested.data.spaced field", f => f.Field("nested.data.spaced field.keyword").Meta(m2 => m2.Add("@field_type", "text")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);
        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void CanParseMixedCaseSort() {
        var index = CreateRandomIndex<MyType>(d => d.Properties(p => p
            .Text(e => e.Name(m => m.MultiWord).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))
        ));

        var res = Client.Index(new MyType { MultiWord = "value1" }, i => i.Index(index));
        Client.Indices.Refresh(index);
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var sort = processor.BuildSortAsync("multiWord -multiword").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Sort(s => s
                .Field(f => f.Field("multiWord.keyword").UnmappedType(FieldType.Keyword).Ascending())
                .Field(f => f.Field("multiWord.keyword").UnmappedType(FieldType.Keyword).Descending())
            ));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);
        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public void GeoRangeQueryProcessor() {
        var index = CreateRandomIndex<MyType>(m => m.Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field3))
                .Text(e => e.Name(m => m.Field1).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))));

        Client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index(index));
        Client.Index(new MyType { Field4 = 2 }, i => i.Index(index));
        Client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        Client.Indices.Refresh(index);

        var aliasMap = new FieldMap { { "geo", "field3" } };
        var processor = new ElasticQueryParser(c => c
            .UseMappings(Client, index)
            .UseGeo(l => "51.5032520,-0.1278990")
            .UseFieldMap(aliasMap)
            .SetLoggerFactory(Log));

        var result = processor.BuildQueryAsync("geo:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi",
                new ElasticQueryVisitorContext { UseScoring = true }).Result;
        var sort = processor.BuildSortAsync("geo -field1").Result;
        var actualResponse = Client.Search<MyType>(d => d.Index(index).Query(q => result).Sort(sort));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index)
            .Sort(s => s
                .Field(f => f.Field(new Field("field3")).Ascending().UnmappedType(FieldType.GeoPoint))
                .Field(f => f.Field(new Field("field1.keyword")).Descending().UnmappedType(FieldType.Keyword))
            ).Query(q => q
                .GeoBoundingBox(m => m
                    .Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))
                || q.Match(y => y.Field(e => e.Field1).Query("value1"))
                || q.TermRange(m => m.Field(g => g.Field2).GreaterThanOrEquals("1").LessThanOrEquals("4"))
                || !q.GeoDistance(m => m.Field(p => p.Field3).Location("51.5032520,-0.1278990").Distance("75mi"))));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanExpandElasticIncludesAsync() {
        var client = new ElasticClient(new ConnectionSettings().DisableDirectStreaming().PrettyJson());
        var aliases = new FieldMap { { "field", "aliased" }, { "included", "aliasedincluded" } };

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseIncludes(GetIncludeAsync).UseFieldMap(aliases));
        var result = await processor.BuildQueryAsync("@include:other");
        var actualResponse = Client.Search<MyType>(d => d.Index("stuff").Query(q => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index("stuff").Query(f => f.Bool(b => b.Filter(f1 => f1.Term("aliasedincluded", "value")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = await processor.BuildQueryAsync("@include:other");
        actualResponse = Client.Search<MyType>(d => d.Index("stuff").Query(q => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    private async Task<string> GetIncludeAsync(string name) {
        await Task.Delay(150);
        return "included:value";
    }
}

public class MyType {
    public string Id { get; set; }
    public string Field1 { get; set; }
    public string Field2 { get; set; }
    public string Field3 { get; set; }
    public int Field4 { get; set; }
    public DateTime Field5 { get; set; }
    public string MultiWord { get; set; }
    public Dictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
}

public class MyNestedType {
    public string Field1 { get; set; }
    public string Field2 { get; set; }
    public string Field3 { get; set; }
    public int Field4 { get; set; }
    public IList<MyType> Nested { get; set; } = new List<MyType>();
}

public class UpdateFixedTermFieldToDateFixedExistsQueryVisitor : ChainableQueryVisitor {

    public override void Visit(TermNode node, IQueryVisitorContext context) {
        if (!String.Equals(node.Field, "fixed", StringComparison.OrdinalIgnoreCase))
            return;

        if (!Boolean.TryParse(node.Term, out var isFixed))
            return;

        var query = new ExistsQuery { Field = "date_fixed" };
        node.SetQuery(isFixed ? query : !query);
    }
}

public class TestQueryVisitor : ChainableQueryVisitor {
    public int GroupNodeCount { get; private set; } = 0;

    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
        GroupNodeCount++;
        return base.VisitAsync(node, context);
    }
}
