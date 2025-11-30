using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class AggregationParserTests : ElasticsearchTestBase
{
    public AggregationParserTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture) { }

    [Fact]
    public async Task ProcessSingleAggregationAsync()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True).Properties(p => p.GeoPoint(g => g.Field3)));
        await Client.IndexManyAsync([
            new MyType
            {
                Field1 = "value1",
                Field4 = 1,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)),
                Field2 = "field2"
            },
            new MyType
            {
                Field1 = "value2",
                Field4 = 2,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4))
            },
            new MyType
            {
                Field1 = "value3",
                Field4 = 3,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3))
            },
            new MyType
            {
                Field1 = "value4",
                Field4 = 4,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2))
            },
            new MyType
            {
                Field1 = "value5",
                Field4 = 5,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1))
            }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseGeo(_ => "51.5032520,-0.1278990"));
        var aggregations = await processor.BuildAggregationsAsync("min:field4");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("min_field4", a1 => a1.Min(c => c.Field("field4")).Meta(m => m.Add("@field_type", "long")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessSingleAggregationWithAliasAsync()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True).Properties(p => p.GeoPoint(g => g.Field3)));
        await Client.IndexManyAsync([
            new MyType
            {
                Field1 = "value1",
                Field4 = 1,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)),
                Field2 = "field2"
            },
            new MyType
            {
                Field1 = "value2",
                Field4 = 2,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4))
            },
            new MyType
            {
                Field1 = "value3",
                Field4 = 3,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3))
            },
            new MyType
            {
                Field1 = "value4",
                Field4 = 4,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2))
            },
            new MyType
            {
                Field1 = "value5",
                Field4 = 5,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1))
            }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var fieldMap = new FieldMap { { "heynow", "field4" } };
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseFieldMap(fieldMap).UseGeo(_ => "51.5032520,-0.1278990"));
        var aggregations = await processor.BuildAggregationsAsync("min:heynow");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("min_heynow", a1 => a1.Min(c => c.Field(cf => cf.Field4)).Meta(m => m.Add("@field_type", "long")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessAnalyzedAggregationWithAliasAsync()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True).Properties(p => p
            .Text(f => f.Field1, o => o
                .Fields(k => k.Keyword("keyword")))
            .FieldAlias("heynow", o => o.Path(k => k.Field1))
            .GeoPoint(g => g.Field3)));
        await Client.IndexManyAsync([
            new MyType
            {
                Field1 = "value1",
                Field4 = 1,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)),
                Field2 = "field2"
            },
            new MyType
            {
                Field1 = "value2",
                Field4 = 2,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4))
            },
            new MyType
            {
                Field1 = "value3",
                Field4 = 3,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3))
            },
            new MyType
            {
                Field1 = "value4",
                Field4 = 4,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2))
            },
            new MyType
            {
                Field1 = "value5",
                Field4 = 5,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1))
            }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var fieldMap = new FieldMap { { "heynow2", "field1" } };
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseFieldMap(fieldMap));
        var aggregations = await processor.BuildAggregationsAsync("terms:heynow");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("terms_heynow", a1 => a1.Terms(c => c.Field("field1.keyword")).Meta(m => m.Add("@field_type", "text")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessAggregationsAsync()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True).Properties(p => p.GeoPoint(g => g.Field3)));
        await Client.IndexManyAsync([
            new MyType
            {
                Field1 = "value1",
                Field4 = 1,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)),
                Field2 = "field2"
            },
            new MyType
            {
                Field1 = "value2",
                Field4 = 2,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4))
            },
            new MyType
            {
                Field1 = "value3",
                Field4 = 3,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3))
            },
            new MyType
            {
                Field1 = "value4",
                Field4 = 4,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2))
            },
            new MyType
            {
                Field1 = "value5",
                Field4 = 5,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1))
            }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseGeo(_ => "51.5032520,-0.1278990"));
        var aggregations = await processor.BuildAggregationsAsync("min:field4 max:field4 avg:field4 sum:field4 percentiles:field4~50,100 cardinality:field4 missing:field2 date:field5 histogram:field4 geogrid:field3 terms:field1");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("geogrid_field3", a1 => a1.GeohashGrid(h => h.Field(f => f.Field3).Precision(new GeohashPrecision(1)))
                .Aggregations(a2 => a2
                    .Add("avg_lat", a3 => a3.Avg(s => s.Script(ss => ss.Source("doc['field3'].lat"))))
                    .Add("avg_lon", a3 => a3.Avg(s => s.Script(ss => ss.Source("doc['field3'].lon"))))
            ))
            .Add("terms_field1", a1 => a1.Terms(t => t.Field("field1.keyword")).Meta(m => m.Add("@field_type", "text")))
            .Add("histogram_field4", a1 => a1.Histogram(h => h.Field(f => f.Field4).Interval(50).MinDocCount(0)))
            .Add("date_field5", a1 => a1.DateHistogram(d1 => d1.Field(f => f.Field5).CalendarInterval(CalendarInterval.Day).Format("date_optional_time").MinDocCount(0)))
            .Add("missing_field2", a1 => a1.Missing(t => t.Field("field2.keyword")))
            .Add("cardinality_field4", a1 => a1.Cardinality(c => c.Field(f => f.Field4)))
            .Add("percentiles_field4", a1 => a1.Percentiles(c => c.Field(f => f.Field4).Percents([50, 100])))
            .Add("sum_field4", a1 => a1.Sum(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))
            .Add("avg_field4", a1 => a1.Avg(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))
            .Add("max_field4", a1 => a1.Max(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))
            .Add("min_field4", a1 => a1.Min(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessNestedAggregationsWithAliasesAsync()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True).Properties(p => p
            .GeoPoint(g => g.Field3)
            .Object(o1 => o1.Data, o => o.Properties(p1 => p1
                .Object("@user", o1 => o1.Properties(p2 => p2
                    .Text("identity", o2 => o2
                        .Fields(f => f.Keyword("keyword", o3 => o3.IgnoreAbove(256))))))))));

        await Client.IndexManyAsync([new MyType { Field1 = "value1" }], index);
        await Client.Indices.RefreshAsync(index);

        var aliasMap = new FieldMap { { "user", "data.@user.identity" }, { "alias1", "field1" } };
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseFieldMap(aliasMap));
        var aggregations = await processor.BuildAggregationsAsync("terms:(alias1 cardinality:user)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("terms_alias1", a1 => a1.Terms(t => t.Field("field1.keyword")).Meta(m => m.Add("@field_type", "keyword"))
                .Aggregations(a2 => a2.Add("cardinality_user", a3 => a3.Cardinality(c => c.Field("data.@user.identity.keyword")))))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessSingleAggregationWithAlias()
    {
        string index = await CreateRandomIndexAsync<MyType>();

        await Client.IndexManyAsync([
            new MyType { Field2 = "field2" }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var aliasMap = new FieldMap { { "alias2", "field2" } };
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseFieldMap(aliasMap));
        var aggregations = await processor.BuildAggregationsAsync("missing:alias2");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("missing_alias2", a1 => a1.Missing(t => t.Field("field2.keyword")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessAggregationsWithAliasesAsync()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Dynamic(DynamicMapping.True).Properties(p => p
            .GeoPoint(g => g.Field3)
            .Object(o1 => o1.Data, o2 => o2.Properties(p1 => p1
                .Object("@user", o3 => o3.Properties(p2 => p2
                    .Text("identity", o4 => o4.Fields(f => f.Keyword("keyword", o5 => o5.IgnoreAbove(256))))))))));

        await Client.IndexManyAsync([
            new MyType
            {
                Field1 = "value1",
                Field4 = 1,
                Field3 = "51.5032520,-0.1278990",
                Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)),
                Field2 = "field2"
            }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var aliasMap = new FieldMap
        {
            { "user", "data.@user.identity" },
            { "alias1", "field1" },
            { "alias2", "field2" },
            { "alias3", "field3" },
            { "alias4", "field4" },
            { "alias5", "field5" }
        };
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index).UseGeo(_ => "51.5032520,-0.1278990").UseFieldMap(aliasMap));
        var aggregations = await processor.BuildAggregationsAsync("min:alias4 max:alias4 avg:alias4 sum:alias4 percentiles:alias4 cardinality:user missing:alias2 date:alias5 histogram:alias4 geogrid:alias3 terms:alias1");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
                .Add("geogrid_alias3", a1 => a1.GeohashGrid(h => h.Field(f => f.Field3).Precision(new GeohashPrecision(1)))
                    .Aggregations(a2 => a2
                        .Add("avg_lat", a3 => a3.Avg(s => s.Script(ss => ss.Source("doc['field3'].lat"))))
                        .Add("avg_lon", a3 => a3.Avg(s => s.Script(ss => ss.Source("doc['field3'].lon"))))
                    ))
                .Add("terms_alias1", a1 => a1.Terms(t => t.Field("field1.keyword")).Meta(m => m.Add("@field_type", "text")))
                .Add("histogram_alias4", a1 => a1.Histogram(h => h.Field(f => f.Field4).Interval(50).MinDocCount(0)))
                .Add("date_alias5", a1 => a1.DateHistogram(d1 => d1.Field(f => f.Field5).CalendarInterval(CalendarInterval.Day).Format("date_optional_time").MinDocCount(0)))
                .Add("missing_alias2", a1 => a1.Missing(t => t.Field("field2.keyword")))
                .Add("cardinality_user", a1 => a1.Cardinality(c => c.Field("data.@user.identity.keyword")))
                .Add("percentiles_alias4", a1 => a1.Percentiles(c => c.Field(f => f.Field4)))
                .Add("sum_alias4", a1 => a1.Sum(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))
                .Add("avg_alias4", a1 => a1.Avg(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))
                .Add("max_alias4", a1 => a1.Max(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))
                .Add("min_alias4", a1 => a1.Min(c => c.Field(f => f.Field4)).Meta(m => m.Add("@field_type", "long")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessTermAggregations()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field1 = "value1" }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("terms:(field1 @exclude:myexclude @include:myinclude @include:otherinclude @missing:mymissing @exclude:otherexclude @min:1)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("terms_field1", a1 => a1.Terms(t => t
                .Field("field1.keyword")
                .MinDocCount(1)
                .Include(new TermsInclude(["otherinclude", "myinclude"]))
                .Exclude(new TermsExclude(["otherexclude", "myexclude"]))
                .Missing("mymissing"))
                .Meta(m => m.Add("@field_type", "keyword")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessTermAggregationsWithRegex()
    {
        string index = CreateRandomIndex<MyType>();
        await Client.IndexManyAsync([new MyType { Field1 = "value1" }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("terms:(field1 @exclude:/A.*/ @include:/B.*/)");

        var actualResponse = Client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyType>(d => d.Index(index).Aggregations(a => a
            .Terms("terms_field1", t => t
                .Field("field1.keyword")
                .Include("B.*")
                .Exclude("A.*")
                .Meta(m => m.Add("@field_type", "keyword")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessHistogramIntervalAggregations()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field1 = "value1" }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("histogram:(field1~0.1)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("histogram_field1", a1 => a1.Histogram(t => t
                .Field("field1.keyword")
                .Interval(0.1)
                .MinDocCount(0)
            ))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessTermTopHitsAggregations()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field1 = "value1" }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("terms:(field1~1000^2 tophits:(_~1000 @include:myinclude))");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("terms_field1", a1 => a1
                .Terms(t => t.Field("field1.keyword").Size(1000).MinDocCount(2))
                    .Aggregations(a2 => a2.Add("tophits", a3 => a3.TopHits(t2 => t2.Size(1000).Source(new SourceConfig(new SourceFilter { Includes = Fields.FromString("myinclude")})))))
                .Meta(m => m.Add("@field_type", "keyword")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessSortedTermAggregations()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field1 = "value1" }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("terms:(field1 -cardinality:field4)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
                .Add("terms_field1", a1 => a1
                    .Terms(t => t.Field("field1.keyword").Order(new List<KeyValuePair<Field, SortOrder>> { new ("cardinality_field4", SortOrder.Desc) }))
                        .Aggregations(a2 => a2.Add("cardinality_field4", a3 => a3.Cardinality(c => c.Field(f => f.Field4))))
                    .Meta(m => m.Add("@field_type", "keyword")))));
        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task ProcessDateHistogramAggregations()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field5 = DateTime.UtcNow }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("date:(field5^1h @missing:\"0001-01-01T00:00:00\" min:field5^1h max:field5^1h)");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("date_field5", a1 => a1.DateHistogram(d1 => d1
                .Field(f => f.Field5)
                .CalendarInterval(CalendarInterval.Day)
                .Format("date_optional_time")
                .MinDocCount(0)
                .TimeZone("+01:00")
                .Missing(DateTime.MinValue))
                .Meta(m => m.Add("@timezone", "1h"))
                .Aggregations(a2 => a2
                    .Add("min_field5", a3 => a3.Min(c => c.Field(f => f.Field5)).Meta(m => m.Add("@field_type", "date").Add("@timezone", "1h")))
                    .Add("max_field5", a3 => a3.Max(c => c.Field(f => f.Field5)).Meta(m => m.Add("@field_type", "date").Add("@timezone", "1h")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse, actualResponse.DebugInformation);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task CanSpecifyDefaultValuesAggregations()
    {
        string index = await CreateRandomIndexAsync<MyType>();
        await Client.IndexManyAsync([new MyType { Field1 = "test" }, new MyType { Field4 = 1 }], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(Client, index));
        var aggregations = await processor.BuildAggregationsAsync("min:field4~0 max:field4~0 avg:field4~0 sum:field4~0 cardinality:field4~0");

        var actualResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(aggregations));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyType>(d => d.Indices(index).Aggregations(a => a
            .Add("sum_field4", a1 => a1.Sum(c => c.Field(f => f.Field4).Missing(0)).Meta(m => m.Add("@field_type", "integer")))
            .Add("cardinality_field4", a1 => a1.Cardinality(c => c.Field(f => f.Field4).Missing(0)))
            .Add("avg_field4", a1 => a1.Avg(c => c.Field(f => f.Field4).Missing(0)).Meta(m => m.Add("@field_type", "integer")))
            .Add("max_field4", a1 => a1.Max(c => c.Field(f => f.Field4).Missing(0)).Meta(m => m.Add("@field_type", "integer")))
            .Add("min_field4", a1 => a1.Min(c => c.Field(f => f.Field4).Missing(0)).Meta(m => m.Add("@field_type", "integer")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
        Assert.True(expectedResponse.IsValidResponse);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task GeoGridDoesNotResolveLocationForAggregation()
    {
        string index = await CreateRandomIndexAsync<MyType>(d => d.Properties(p => p
            .GeoPoint(g => g.Field1)
            .FieldAlias("geo", o => o.Path(f => f.Field1))));

        var processor = new ElasticQueryParser(c => c
                .UseGeo(_ => "someinvalidvaluehere")
                .UseMappings(Client, index));

        await processor.BuildAggregationsAsync("geogrid:geo~3");
    }

    [Theory]
    [InlineData("avg", false)]
    [InlineData("avg:", false)]
    [InlineData("avg:value", true)]
    [InlineData("    avg     :   value", true)]
    [InlineData("avg:value cardinality:value sum:value min:value max:value", true)]
    public Task CanParseAggregations(string query, bool isValid)
    {
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        return GetAggregationQueryInfoAsync(parser, query, isValid);
    }

    public static IEnumerable<object[]> AggregationTestCases =>
    [
        [null, true, 1, new HashSet<string>(), new Dictionary<string, ICollection<string>>()],
        [String.Empty, true, 1, new HashSet<string>(), new Dictionary<string, ICollection<string>>()],
        ["avg",
            false,
            1,
            new HashSet<string> { "" },
            new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { null } } }
        ],
        ["avg:", false, 1, new HashSet<string>(), new Dictionary<string, ICollection<string>>()],
        [
            "avg:value",
            true,
            1,
            new HashSet<string> { "value" },
            new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value" } } }
        ],
        [
            "    avg    :    value",
            true,
            1,
            new HashSet<string> { "value" },
            new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value" } } }
        ],
        [
            "avg:value cardinality:value sum:value min:value max:value",
            true,
            1,
            new HashSet<string> { "value" },
            new Dictionary<string, ICollection<string>> {
                    { "avg", new HashSet<string> { "value" } },
                    { "cardinality", new HashSet<string> { "value" } },
                    { "sum", new HashSet<string> { "value" } },
                    { "min", new HashSet<string> { "value" } },
                    { "max", new HashSet<string> { "value" } }
                }
        ],
        [
            "avg:value avg:value2",
            true,
            1,
            new HashSet<string> { "value", "value2" },
            new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value", "value2" } } }
        ],
        [
            "avg:value avg:value",
            true,
            1,
            new HashSet<string> { "value" },
            new Dictionary<string, ICollection<string>> { { "avg", new HashSet<string> { "value" } } }
        ]
    ];

    [Theory]
    [MemberData(nameof(AggregationTestCases))]
    public Task GetElasticAggregationQueryInfoAsync(string query, bool isValid, int maxNodeDepth, HashSet<string> fields, Dictionary<string, ICollection<string>> operations)
    {
        var parser = new ElasticQueryParser(c => c.SetLoggerFactory(Log));
        return GetAggregationQueryInfoAsync(parser, query, isValid, maxNodeDepth, fields, operations);
    }

    private async Task GetAggregationQueryInfoAsync(IQueryParser parser, string query, bool isValid, int maxNodeDepth = -1, HashSet<string> fields = null, Dictionary<string, ICollection<string>> operations = null)
    {
        var context = new ElasticQueryVisitorContext { QueryType = QueryTypes.Aggregation };
        var queryNode = await parser.ParseAsync(query, context);
        Assert.NotNull(queryNode);

        var result = context.GetValidationResult();
        Assert.Equal(QueryTypes.Aggregation, result.QueryType);
        if (!result.IsValid)
            _logger.LogInformation("Result {Request}", result.Message);

        Assert.Equal(isValid, result.IsValid);
        if (maxNodeDepth >= 0)
            Assert.Equal(maxNodeDepth, result.MaxNodeDepth);
        if (fields != null)
            Assert.Equal(fields.ToList(), result.ReferencedFields);

        if (operations != null)
            Assert.Equal(operations, result.Operations.ToDictionary(pair => pair.Key, pair => pair.Value));
    }
}
