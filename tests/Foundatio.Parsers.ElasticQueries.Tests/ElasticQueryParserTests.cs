using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticQueryParserTests : ElasticsearchTestBase
{
    public ElasticQueryParserTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultMinimumLevel = LogLevel.Trace;
    }

    [Fact]
    public async Task CanHandleEmptyAndNullString()
    {
        var sut = new ElasticQueryParser();

        var queryResult = await sut.BuildQueryAsync("");
        Assert.NotNull(queryResult);

        queryResult = await sut.BuildQueryAsync((string)null);
        Assert.NotNull(queryResult);

        var aggResult = await sut.BuildAggregationsAsync("");
        Assert.NotNull(aggResult);

        aggResult = await sut.BuildAggregationsAsync((string)null);
        Assert.NotNull(aggResult);

        var sortResult = await sut.BuildSortAsync("");
        Assert.NotNull(sortResult);

        sortResult = await sut.BuildSortAsync((string)null);
        Assert.NotNull(sortResult);
    }

    [Fact]
    public void CanUseElasticQueryParser()
    {
        var sut = new ElasticQueryParser();
        var result = sut.Parse("NOT (dog parrot)");

        Assert.NotNull(result);
        Assert.IsType<GroupNode>(result.Left);
        Assert.True(((GroupNode)result.Left).HasParens);
        Assert.True(((GroupNode)result.Left).IsNegated);
    }

    [Fact]
    public void CanUseElasticQueryParserWithVisitor()
    {
        var testQueryVisitor = new TestQueryVisitor();
        var sut = new ElasticQueryParser(c => c.AddQueryVisitor(testQueryVisitor));
        var context = new ElasticQueryVisitorContext();

        var result = sut.Parse("NOT (dog parrot)", context) as GroupNode;
        Assert.Equal(2, testQueryVisitor.GroupNodeCount);

        Assert.NotNull(result);
        Assert.IsType<GroupNode>(result.Left);
        Assert.True(((GroupNode)result.Left).HasParens);
        Assert.True((result.Left as GroupNode)?.IsNegated);
    }

    [Fact]
    public async Task SimpleFilterProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1"))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task IncludeProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var includes = new Dictionary<string, string> {
                {"stuff", "field2:value2"}
            };

        var processor = new ElasticQueryParser(c => c.UseIncludes(includes).SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1 @include:stuff", new ElasticQueryVisitorContext { UseScoring = true });
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f =>
            f.Term(m => m.Field(tf => tf.Field1).Value("value1"))
            && f.Term(m => m.Field(tf => tf.Field2).Value("value2"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ShouldGenerateORedTermsQuery()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexAsync(new MyType { Field1 = "value1", Field2 = "value2", Field3 = "value3" }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1 field2:value2 field3:value3",
                new ElasticQueryVisitorContext().UseSearchMode());
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f =>
            f.Term(m => m.Field(tf => tf.Field1).Value("value1")) || f.Term(m => m.Field(tf => tf.Field2).Value("value2")) || f.Term(m => m.Field(tf => tf.Field3).Value("value3"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ShouldHandleMultipleTermsForAnalyzedFields()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d
            .Dynamic(DynamicMapping.True).Properties(p => p.GeoPoint(g => g.Field3)
                .Text(e => e.Field1, o => o.Fields(f1 => f1.Keyword("keyword")))
                .Keyword(e => e.Field2)
            ));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field2 = "value2", Field3 = "value3" }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetDefaultFields(["field1"]).UseMappings(Client, index));

        var result = await processor.BuildQueryAsync("field1:(value1 abc def ghi) field2:(value2 jhk)",
                new ElasticQueryVisitorContext().UseSearchMode());
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f =>
            f.Match(m => m.Field(mf => mf.Field1).Query("value1"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("abc"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("def"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("ghi"))
            || f.Term(m => m.Field(tf => tf.Field2).Value("value2")) || f.Term(m => m.Field2, "jhk")));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).SetDefaultFields(["field1"]).UseMappings(Client, index));
        result = await processor.BuildQueryAsync("value1 abc def ghi", new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f =>
            f.Match(m => m.Field(mf => mf.Field1).Query("value1"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("abc"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("def"))
            || f.Match(m => m.Field(mf => mf.Field1).Query("ghi"))));

        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        // multi-match on multiple default fields
        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).SetDefaultFields(["field1", "field2"]).UseMappings(Client, index));
        result = await processor.BuildQueryAsync("value1 abc def ghi", new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f =>
            f.MultiMatch(m => m.Fields(Fields.FromStrings(["field1", "field2"])).Query("value1"))
            || f.MultiMatch(m => m.Fields(Fields.FromStrings(["field1", "field2"])).Query("abc"))
            || f.MultiMatch(m => m.Fields(Fields.FromStrings(["field1", "field2"])).Query("def"))
            || f.MultiMatch(m => m.Fields(Fields.FromStrings(["field1", "field2"])).Query("ghi"))));

        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanGetMappingsFromCode()
    {
        TypeMappingDescriptor<MyType> GetCodeMappings(TypeMappingDescriptor<MyType> d) =>
            d.Dynamic(DynamicMapping.True)
                .Properties(p => p
                    .GeoPoint(g => g.Field3)
                    .Text(e => e.Field1));

        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True)
            .Properties(p => p
                .GeoPoint(g => g.Field3)
                .Keyword(e => e.Field2)));

        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field2 = "value2", Field4 = 1, Field5 = DateTime.Now }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var parser = new ElasticQueryParser(c => c.SetDefaultFields(["field1"]).UseMappings<MyType>(GetCodeMappings, Client, index));

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
    public async Task EscapeFilterProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "\"now there\"", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = await processor.BuildQueryAsync("\"\\\"now there\"");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
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
    public async Task CanHandleEscapedQueryWithWildcards()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "one/two/three" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = await processor.BuildQueryAsync(@"field1:one\\/two*");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
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
    public async Task CanHandleEscapedQuery()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "one/two/three" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = await processor.BuildQueryAsync(@"field1:one\\/two");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
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
    public async Task ExistsFilterProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync($"_exists_:{nameof(MyType.Field2)}");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index)
            .Query(q => q.Bool(b => b.Filter(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task MissingFilterProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync($"_missing_:{nameof(MyType.Field2)}",
                new ElasticQueryVisitorContext { UseScoring = true });
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
                .Index(index)
                .Query(q => q.Bool(b => b.MustNot(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task MinMaxWithDateHistogramAggregation()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now },
            new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now },
            new MyType { Field2 = "value4", Field5 = DateTime.Now }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = await processor.BuildAggregationsAsync("min:field2 max:field2 date:(field5~1d^\"America/Chicago\" min:field2 max:field2 min:field1 @offset:-6h)");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(i => i.Index(index).Aggregations(f => f
            .Max("max_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "text")))
            .DateHistogram("date_field5",
                d =>
                    d.Field(d2 => d2.Field5)
                        .CalendarInterval(CalendarInterval.Day)
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
    [InlineData("1s", CalendarInterval.Second)]
    [InlineData("second", CalendarInterval.Second)]
    [InlineData("m", CalendarInterval.Minute)]
    [InlineData("1m", CalendarInterval.Minute)]
    [InlineData("minute", CalendarInterval.Minute)]
    [InlineData("23m")]
    [InlineData("h", CalendarInterval.Hour)]
    [InlineData("1h", CalendarInterval.Hour)]
    [InlineData("hour", CalendarInterval.Hour)]
    [InlineData("1.5h")]
    [InlineData("d", CalendarInterval.Day)]
    [InlineData("1d", CalendarInterval.Day)]
    [InlineData("day", CalendarInterval.Day)]
    [InlineData("2d")]
    [InlineData("w", CalendarInterval.Week)]
    [InlineData("1w", CalendarInterval.Week)]
    [InlineData("week", CalendarInterval.Week)]
    [InlineData("M", CalendarInterval.Month)]
    [InlineData("1M", CalendarInterval.Month)]
    [InlineData("month", CalendarInterval.Month)]
    [InlineData("q", CalendarInterval.Quarter)]
    [InlineData("1q", CalendarInterval.Quarter)]
    [InlineData("quarter", CalendarInterval.Quarter)]
    [InlineData("y", CalendarInterval.Year)]
    [InlineData("1y", CalendarInterval.Year)]
    [InlineData("year", CalendarInterval.Year)]
    [Theory]
    public async Task CanUseDateHistogramAggregationInterval(string interval, object expectedInterval = null)
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field5 = DateTime.Now }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));

        var result = await processor.BuildAggregationsAsync($"date:(field5~{interval})");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(i => i.Index(index).Aggregations(f => f
            .DateHistogram("date_field5", d =>
            {
                d.Field(d2 => d2.Field5);

                if (expectedInterval is CalendarInterval calendarInterval)
                    d.CalendarInterval(calendarInterval);
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
    public async Task CanDoNestDateHistogram()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field5 = DateTime.Now }], index);
        await Client.Indices.RefreshAsync(index);

        var response = await Client.SearchAsync<MyType>(i => i.Index(index).Aggregations(f => f
            .DateHistogram("myagg", d => d.Field(d2 => d2.Field5).CalendarInterval(CalendarInterval.Day))
        ));

        Assert.True(response.IsValidResponse);
    }

    [Fact]
    public async Task DateAggregation()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now },
            new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now },
            new MyType { Field2 = "value4", Field5 = DateTime.Now }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildAggregationsAsync("date:field5");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(i => i.Index(index).Aggregations(f => f
            .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).CalendarInterval(CalendarInterval.Day).Format("date_optional_time").MinimumDocumentCount(0))
        ));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task SimpleQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>(t => t
            .Properties(p => p
                .Text(e => e.Name(n => n.Field3).Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256))))));

        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4", Field3 = "hey now" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = await processor.BuildQueryAsync("field1:value1", new ElasticQueryVisitorContext().UseSearchMode());
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.Match(e => e.Field(m => m.Field1).Query("value1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = await processor.BuildQueryAsync("field3:hey", new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.Match(m => m
            .Field(f => f.Field3)
            .Query("hey")
        )));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = await processor.BuildQueryAsync("field3:\"hey now\"", new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.MatchPhrase(m => m
            .Field(f => f.Field3)
            .Query("hey now")
        )));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = await processor.BuildQueryAsync("field3:hey*", new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.QueryString(m => m
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
    public async Task NegativeQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value3" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1 AND -field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode());
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1")) && !f.Term(m => m.Field(tf => tf.Field2).Value("value2"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        result = await processor.BuildQueryAsync("field1:value1 AND NOT field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1")) && !f.Term(m => m.Field(tf => tf.Field2).Value("value2"))));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        result = await processor.BuildQueryAsync("field1:value1 OR NOT field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1")) || !f.Term(m => m.Field(tf => tf.Field2).Value("value2"))));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        result = await processor.BuildQueryAsync("field1:value1 OR -field2:value2",
                new ElasticQueryVisitorContext().UseSearchMode());
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index).Query(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1")) || !f.Term(m => m.Field(tf => tf.Field2).Value("value2"))));
        expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)",
                new ElasticQueryVisitorContext().UseSearchMode());

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
                .Query(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1")) ||
                    (f.Term(m => m.Field(tf => tf.Field2).Value("value2")) || f.Term(m => m.Field(tf => tf.Field3).Value("value3")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse =
            await Client.SearchAsync<MyType>(d => d.Index(index)
                .Query(q => q.Bool(b => b.Filter(f => f
                    .Term(m => m.Field(tf => tf.Field1).Value("value1")) &&
                        (f.Term(m => m.Field(tf => tf.Field2).Value("value2")) || f.Term(m => m.Field(tf => tf.Field3).Value("value3")))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task MixedCaseTermFilterQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexAsync(new MyType { Field1 = "Testing.Casing" }, i => i.Index(index));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:Testing.Casing", new ElasticQueryVisitorContext { UseScoring = true });
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f => f.Term(m => m.Field1, "Testing.Casing")));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task MultipleWordsTermFilterQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexAsync(new MyType { Field1 = "Blake Niemyjski" }, i => i.Index(index));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:\"Blake Niemyjski\"", new ElasticQueryVisitorContext { UseScoring = true });
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(f => f.Term(p => p.Field1, "Blake Niemyjski")));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanTranslateTermQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexAsync(new MyType { Field1 = "Testing.Casing" }, i => i.Index(index));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).AddVisitor(new UpdateFixedTermFieldToDateFixedExistsQueryVisitor()));
        var result = await processor.BuildQueryAsync("fixed:true");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index).Query(f => f.Bool(b => b.Filter(filter => filter.Exists(m => m.Field("date_fixed"))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task GroupedOrFilterProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([
            new MyType { Field1 = "value1", Field2 = "value2" },
            new MyType { Field1 = "value2", Field2 = "value2" },
            new MyType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)",
                new ElasticQueryVisitorContext().SetDefaultOperator(Operator.And).UseScoring());

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
            .Query(f => f.Term(m => m.Field(tf => tf.Field1).Value("value1")) &&
                    (f.Term(m => m.Field(tf => tf.Field2).Value("value2")) || f.Term(m => m.Field(tf => tf.Field3).Value("value3")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedFilterProcessor()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Text(e => e.Field2, o => o.Index())
            .Text(e => e.Field3, o => o.Index())
            .IntegerNumber(e => e.Field4)
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Field1, o => o.Index())
                .Text(e => e.Field2, o => o.Index())
                .Text(e => e.Field3, o => o.Index())
                .IntegerNumber(e => e.Field4)
            ))
        ));
        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
                Nested = { new MyType { Field1 = "value1", Field4 = 4 } }
            },
            new MyNestedType { Field1 = "value2", Field2 = "value2" },
            new MyNestedType { Field1 = "value1", Field2 = "value4" }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseFieldMap(new FieldMap { { "blah", "nested" } }).UseMappings<MyNestedType>(Client).UseNested());
        var result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1)", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d
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

        result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1 blah.field4:4)", new ElasticQueryVisitorContext().UseScoring());

        actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        expectedResponse = await Client.SearchAsync<MyNestedType>(d => d
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
    public async Task NestedFilterProcessor2()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Text(e => e.Field2, o => o.Index())
            .Text(e => e.Field3, o => o.Index())
            .IntegerNumber(e => e.Field4)
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Field1, o => o.Index())
                .Text(e => e.Field2, o => o.Index())
                .Text(e => e.Field3, o => o.Index())
                .IntegerNumber(e => e.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
                Nested = { new MyType { Field1 = "value1", Field4 = 4 } }
            },
            new MyNestedType { Field1 = "value2", Field2 = "value2" },
            new MyNestedType { Field1 = "value1", Field2 = "value4", Field3 = "value3" }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());
        var result = await processor.BuildQueryAsync("field1:value1 nested:(nested.field1:value1 nested.field4:4 nested.field3:value3)",
                new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
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
    public async Task CanGenerateMatchQuery()
    {
        string index = await CreateRandomIndexAsync<MyType>(m => m.Properties(p => p
           .Text(e => e.Field1)
                .Fields(f1 => f1
                    .Keyword(k => k.Name("keyword").IgnoreAbove(256)))));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var result = await processor.BuildQueryAsync("field1:test", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.Match(m => m.Field(e => e.Field1).Query("test"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanBuildAliasQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>(m => m.Properties(p => p
            .Object<Dictionary<string, object>>(f => f.Name(e => e.Data).Properties(p2 => p2
                .Text(e => e.Name("@browser_version"))
                .FieldAlias(a => a.Name("browser.version").Path("data.@browser_version"))))));

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("browser.version:1", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q => q.Term(m => m.Field("browser.version").Value("1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NonAnalyzedPrefixQuery()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Properties(p => p.Keyword(e => e.Field1)));
        await Client.IndexAsync(new MyType { Field1 = "value123" }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));
        var result = await processor.BuildQueryAsync("field1:value*", new ElasticQueryVisitorContext().UseSearchMode());

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index)
            .Query(f => f.Prefix(m => m.Field(f2 => f2.Field1).Value("value"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task RangeQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        var result =
            await processor.BuildQueryAsync("field4:[1 TO 2} OR field1:value1",
                new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse =
            await Client.SearchAsync<MyType>(
                d =>
                    d.Index(index)
                        .Query(
                            f =>
                                f.TermRange(m => m.Field(f2 => f2.Field4).GreaterThanOrEquals("1").LessThan("2")) ||
                                f.Term(m => m.Field(tf => tf.Field1).Value("value1"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task DateRangeWithWildcardMinQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = await processor.BuildQueryAsync("field5:[* TO 2017-01-31} OR field1:value1", ctx);

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
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
    public async Task DateRangeWithDateMathQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = await processor.BuildQueryAsync("field5:[now-1d/d TO now/d]", ctx);

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
            .Index(index)
            .Query(f => f.DateRange(m => m.Field(f2 => f2.Field5).GreaterThanOrEquals("now-1d/d").LessThanOrEquals("now/d").TimeZone("America/Chicago"))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task DateRangeWithWildcardMaxQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = await processor.BuildQueryAsync("field5:[2017-01-31 TO   *  } OR field1:value1", ctx);

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
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
    public async Task DateRangeWithTimeZone()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));

        var result = await processor.BuildQueryAsync("field5:[2017-01-31 TO   *  }^\"America/Chicago\" OR field1:value1", ctx);

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d
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
    public async Task DateRangeQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var ctx = new ElasticQueryVisitorContext { UseScoring = true, DefaultTimeZone = () => Task.FromResult("America/Chicago") };

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index).SetLoggerFactory(Log));
        var result = await processor.BuildQueryAsync("field5:[2017-01-01T00\\:00\\:00Z TO 2017-01-31} OR field1:value1", ctx);

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse =
            await Client.SearchAsync<MyType>(d => d
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
    public async Task SimpleGeoRangeQuery()
    {
        string index = await CreateRandomIndexAsync<MyType>(m => m.Properties(p => p.GeoPoint(g => g.Field3)));
        var res = await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" },
            i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c
            .UseMappings(Client, index)
            .UseGeo(_ => "51.5032520,-0.1278990"));
        var result =
            await processor.BuildQueryAsync("field3:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990]",
                new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(q =>
            q.GeoBoundingBox(
                m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanUseValidationToGetUnresolvedFields()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Properties(p => p.Keyword(e => e.Field1)));
        await Client.IndexAsync(new MyType { Field1 = "value123" }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var context = new ElasticQueryVisitorContext();
        var parser = new ElasticQueryParser(c => c.UseMappings(Client, index).SetValidationOptions(new QueryValidationOptions { AllowUnresolvedFields = false }));
        var query = await parser.BuildQueryAsync("field1:value", context);

        var validationResult = context.GetValidationResult();
        Assert.True(validationResult.IsValid);
        Assert.Single(validationResult.ReferencedFields, "field1");
        Assert.Empty(validationResult.UnresolvedFields);

        context = new ElasticQueryVisitorContext();
        parser = new ElasticQueryParser(c => c.UseMappings(Client, index).SetValidationOptions(new QueryValidationOptions { AllowUnresolvedFields = false }));
        var ex = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildQueryAsync("field2:value", context));
        Assert.Contains("resolved", ex.Message);
        Assert.Contains("field2", ex.Result.ReferencedFields);
        Assert.Contains("field2", ex.Result.UnresolvedFields);
        Assert.False(ex.Result.IsValid);
        Assert.NotNull(ex.Result.Message);
        Assert.Contains("resolved", ex.Result.Message);

        validationResult = await parser.ValidateQueryAsync("field2:value");
        Assert.False(validationResult.IsValid);
        Assert.Single(validationResult.ReferencedFields, "field2");
        Assert.Single(validationResult.UnresolvedFields, "field2");

        var aliasMap = new FieldMap { { "field2", "field1" } };
        context = new ElasticQueryVisitorContext();
        parser = new ElasticQueryParser(c => c.UseMappings(Client, index).UseFieldMap(aliasMap).SetValidationOptions(new QueryValidationOptions { AllowUnresolvedFields = false }));
        query = await parser.BuildQueryAsync("field2:value", context);

        validationResult = context.GetValidationResult();
        Assert.True(validationResult.IsValid);
        Assert.Single(validationResult.ReferencedFields, "field2");
        Assert.Empty(validationResult.UnresolvedFields);
    }

    [Fact]
    public async Task CanSortByUnmappedField()
    {
        string index = await CreateRandomIndexAsync<MyType>(m => m.Dynamic(DynamicMapping.True));

        var processor = new ElasticQueryParser(c => c.UseMappings(Client, index));
        var sort = await processor.BuildSortAsync("-field1");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Sort(sort));

        Assert.True(actualResponse.IsValidResponse);

        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualResponse);
        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
            .Sort(
                s => s.Field(f => f.Field(new Field("field1")).Descending().UnmappedType(FieldType.Keyword))
            ));
        string expectedRequest = expectedResponse.GetRequest(true);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanParseSort()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Properties(p => p.GeoPoint(g => g.Field3)
                .Text(e => e.Field1, o => o.Fields(f1 => f1.Keyword("keyword")))
                .Text(e => e.Field2, o => o.Fields(f2 => f2.Keyword("keyword").Keyword("sort")))
            ));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var aliasMap = new FieldMap { { "geo", "field3" } };
        var processor = new ElasticQueryParser(c => c
            .UseMappings(Client, index)
            .UseFieldMap(aliasMap));
        var sort = await processor.BuildSortAsync("geo -field1 -(field2 field3 +field4) (field5 field3)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Sort(s => s
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
    public async Task CanHandleSpacedFields()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>();

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
                Nested =
            [
                new MyType
                {
                    Field1 = "banana",
                    Data = {
                    { "number-0001", 23 },
                    { "text-0001", "Hey" },
                    { "spaced field", "hey" }
                }
                }
            ]
            },
            new MyNestedType { Field1 = "value2", Field2 = "value2" },
            new MyNestedType { Field1 = "value1", Field2 = "value4" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var sort = await processor.BuildSortAsync("nested.data.spaced\\ field");
        var query = await processor.BuildQueryAsync("nested.data.spaced\\ field:hey");
        var aggs = await processor.BuildAggregationsAsync("terms:nested.data.spaced\\ field");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Sort(sort).Query(_ => query).Aggregations(_ => aggs));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
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
    public async Task CanParseMixedCaseSort()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Properties(p => p
            .Text(e => e.MultiWord, o => o.Fields(f1 => f1.Keyword("keyword")))
        ));

        var res = await Client.IndexAsync(new MyType { MultiWord = "value1" }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var sort = await processor.BuildSortAsync("multiWord -multiword");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
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
    public async Task GeoRangeQueryProcessor()
    {
        string index = await CreateRandomIndexAsync<MyType>(m => m.Properties(p => p
                .GeoPoint(g => g.Field3)
                .Text(e => e.Field1, o => o.Fields(f1 => f1.Keyword("keyword")))));

        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field4 = 2 }, i => i.Index(index));
        await Client.IndexAsync(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index(index));
        await Client.Indices.RefreshAsync(index);

        var aliasMap = new FieldMap { { "geo", "field3" } };
        var processor = new ElasticQueryParser(c => c
            .UseMappings(Client, index)
            .UseGeo(_ => "51.5032520,-0.1278990")
            .UseFieldMap(aliasMap)
            .SetLoggerFactory(Log));

        var result = await processor.BuildQueryAsync("geo:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi",
                new ElasticQueryVisitorContext { UseScoring = true });
        var sort = await processor.BuildSortAsync("geo -field1");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index(index).Query(_ => result).Sort(sort));
        string actualRequest = actualResponse.GetRequest(true);
        _logger.LogInformation("Actual: {Request}", actualResponse);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index(index)
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
    public async Task CanExpandElasticIncludesAsync()
    {
        var client = new ElasticsearchClient(new ElasticsearchClientSettings().DisableDirectStreaming().PrettyJson());
        var aliases = new FieldMap { { "field", "aliased" }, { "included", "aliasedincluded" } };

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseIncludes(GetIncludeAsync).UseFieldMap(aliases));
        var result = await processor.BuildQueryAsync("@include:other");
        var actualResponse = await Client.SearchAsync<MyType>(d => d.Index("stuff").Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Index("stuff").Query(f => f.Bool(b => b.Filter(f1 => f1.Term("aliasedincluded", "value")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);

        result = await processor.BuildQueryAsync("@include:other");
        actualResponse = await Client.SearchAsync<MyType>(d => d.Index("stuff").Query(_ => result));
        actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    private async Task<string> GetIncludeAsync(string name)
    {
        await Task.Delay(150);
        return "included:value";
    }

    [Theory]
    [InlineData("terms:field1")]
    [InlineData("terms:(field1~100 @missing:__missing__)")]
    [InlineData("terms:(field1~100 (@missing:__missing__))")]
    public async Task CanValidateAggregation(string aggregation)
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Properties(p => p.Keyword(e => e.Field1)));
        var context = new ElasticQueryVisitorContext { QueryType = QueryTypes.Aggregation };
        var parser = new ElasticQueryParser(c => c.UseMappings(Client, index).SetLoggerFactory(Log));
        var node = await parser.ParseAsync(aggregation, context);

        var result = await ValidationVisitor.RunAsync(node, context);
        Assert.True(result.IsValid, result.Message);
        Assert.Single(result.ReferencedFields, "field1");
        Assert.Empty(result.UnresolvedFields);
    }
}

public record MyType
{
    public string Id { get; set; }
    public string Field1 { get; set; }
    public string Field2 { get; set; }
    public string Field3 { get; set; }
    public int Field4 { get; set; }
    public DateTime Field5 { get; set; }
    public string MultiWord { get; set; }
    public Dictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
}

public record MyNestedType
{
    public string Field1 { get; set; }
    public string Field2 { get; set; }
    public string Field3 { get; set; }
    public int Field4 { get; set; }
    public string Field5 { get; set; }
    public string Payload { get; set; }
    public IList<MyType> Nested { get; set; } = new List<MyType>();
}

public class UpdateFixedTermFieldToDateFixedExistsQueryVisitor : ChainableQueryVisitor
{
    public override void Visit(TermNode node, IQueryVisitorContext context)
    {
        if (!String.Equals(node.Field, "fixed", StringComparison.OrdinalIgnoreCase))
            return;

        if (!Boolean.TryParse(node.Term, out bool isFixed))
            return;

        var query = Query.Exists(new ExistsQuery { Field = "date_fixed" });
        node.SetQuery(isFixed ? query : !query);
    }
}

public class TestQueryVisitor : ChainableQueryVisitor
{
    public int GroupNodeCount { get; private set; } = 0;

    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        GroupNodeCount++;
        return base.VisitAsync(node, context);
    }
}
