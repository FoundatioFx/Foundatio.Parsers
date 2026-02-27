using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticNestedQueryParserTests : ElasticsearchTestBase
{
    public ElasticNestedQueryParserTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultLogLevel = LogLevel.Trace;
    }

    [Fact]
    public async Task NestedQuery_WithFieldMapAndSingleNestedField_BuildsCorrectNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Text(e => e.Field2, o => o.Index())
            .Text(e => e.Field3, o => o.Index())
            .IntegerNumber(e => e.Field4)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Index())
                .Text(e1 => e1.Field2, o1 => o1.Index())
                .Text(e1 => e1.Field3, o1 => o1.Index())
                .IntegerNumber(e1 => e1.Field4)
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

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d
            .Query(q => q.Bool(b => b.Must(
                m => m.Match(ma => ma.Field(e => e.Field1).Query("value1")),
                m => m.Nested(n => n
                    .Path(p => p.Nested)
                    .Query(q2 => q2
                        .Match(ma => ma
                            .Field("nested.field1")
                            .Query("value1"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery_WithFieldMapAndMultipleNestedFields_BuildsCorrectNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Text(e => e.Field2, o => o.Index())
            .Text(e => e.Field3, o => o.Index())
            .IntegerNumber(e => e.Field4)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Index())
                .Text(e1 => e1.Field2, o1 => o1.Index())
                .Text(e1 => e1.Field3, o1 => o1.Index())
                .IntegerNumber(e1 => e1.Field4)
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
        var result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1 blah.field4:4)", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d
            .Query(q => q.Bool(b => b.Must(
                m => m.Match(ma => ma.Field(e => e.Field1).Query("value1")),
                m => m.Nested(n => n
                    .Path(p => p.Nested)
                    .Query(q2 => q2.Bool(b2 => b2.Must(
                        m2 => m2.Match(ma => ma
                            .Field("nested.field1")
                            .Query("value1")),
                        m2 => m2.Term(mt => mt.Field("nested.field4").Value("4"))))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery_WithMultipleNestedFieldsAndConditions_BuildsCorrectNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Text(e => e.Field2, o => o.Index())
            .Text(e => e.Field3, o => o.Index())
            .IntegerNumber(e => e.Field4)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Index())
                .Text(e1 => e1.Field2, o1 => o1.Index())
                .Text(e1 => e1.Field3, o1 => o1.Index())
                .IntegerNumber(e1 => e1.Field4)
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

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Query(q => q.Bool(b => b.Must(
            m => m.Match(ma => ma.Field(e => e.Field1).Query("value1")),
            m => m.Nested(n => n.Path(p => p.Nested).Query(q2 => q2.Bool(b2 => b2.Must(
                m2 => m2.Match(ma => ma.Field("nested.field1").Query("value1")),
                m2 => m2.Term(mt => mt.Field("nested.field4").Value("4")),
                m2 => m2.Match(ma => ma.Field("nested.field3").Query("value3"))))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedIndividualFieldQuery_WithSingleNestedField_WrapsInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent1",
                Nested = { new MyType { Field1 = "child1", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field1 = "child2", Field4 = 3 } }
            }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("nested.field4:5", new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Term(t => t.Field("nested.field4").Value("5"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedIndividualFieldQuery_WithMultipleNestedFieldsOrCondition_CombinesIntoSingleNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent1",
                Nested = { new MyType { Field1 = "target", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field1 = "other", Field4 = 10 } }
            }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("nested.field1:target OR nested.field4:10", new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Bool(b => b
                    .Should(
                        s => s.Match(m => m.Field("nested.field1").Query("target")),
                        s => s.Term(t => t.Field("nested.field4").Value("10"))
                    ))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total);
    }

    [Fact]
    public async Task NestedIndividualFieldQuery_WithRangeQuery_WrapsInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 15 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 25 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("nested.field4:[10 TO 20]", new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Range(r => r.Term(tr => tr.Field("nested.field4").Gte("10").Lte("20")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery_WithNegation_BuildsCorrectMustNotNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Index())
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Field1 = "parent1", Nested = { new MyType { Field1 = "excluded_value", Field4 = 10 } } },
            new MyNestedType { Field1 = "parent2", Nested = { new MyType { Field1 = "included_value", Field4 = 20 } } },
            new MyNestedType { Field1 = "parent3", Nested = { new MyType { Field1 = "another_value", Field4 = 30 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("NOT nested:(nested.field1:excluded_value)", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Bool(b => b
                .MustNot(mn => mn
                    .Nested(n => n
                        .Path(p => p.Nested)
                        .Query(nq => nq
                            .Match(m => m.Field("nested.field1").Query("excluded_value"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithSingleNestedField_AutomaticallyWrapsInNestedAggregation()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 10 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 5 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildAggregationsAsync("terms:nested.field4");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Aggregations(a => a
                .Add("nested_nested", n => n
                    .Nested(ne => ne.Path("nested"))
                    .Aggregations(na => na
                        .Add("terms_nested.field4", t => t
                            .Terms(te => te.Field("nested.field4"))
                            .Meta(m => m.Add("@field_type", "integer")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithMultipleNestedFields_CombinesIntoSingleNestedAggregation()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Fields(f => f.Keyword("keyword")))
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "test", Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other", Field4 = 10 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildAggregationsAsync("terms:nested.field1 terms:nested.field4 max:nested.field4");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Aggregations(a => a
                .Add("nested_nested", n => n
                    .Nested(ne => ne.Path("nested"))
                    .Aggregations(na => na
                        .Add("terms_nested.field4", t => t
                            .Terms(te => te.Field("nested.field4"))
                            .Meta(m => m.Add("@field_type", "integer")))
                        .Add("max_nested.field4", m => m
                            .Max(ma => ma.Field("nested.field4"))
                            .Meta(m2 => m2.Add("@field_type", "integer")))
                        .Add("terms_nested.field1", t => t
                            .Terms(te => te.Field("nested.field1.keyword"))
                            .Meta(m => m.Add("@field_type", "text")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithIncludeCommaSeparatedValues_FiltersCorrectly()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Fields(f => f.Keyword("keyword")))
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "apple", Field4 = 1 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "banana", Field4 = 2 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "cherry", Field4 = 3 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "date", Field4 = 4 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @include:apple @include:banana @include:cherry)");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Aggregations(a => a
                .Add("nested_nested", n => n
                    .Nested(ne => ne.Path("nested"))
                    .Aggregations(na => na
                        .Add("terms_nested.field1", t => t
                            .Terms(te => te
                                .Field("nested.field1.keyword")
                                .Include(new TermsInclude(["apple", "banana", "cherry"])))
                            .Meta(m => m.Add("@field_type", "keyword")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithIncludeExcludeMissingMin_BuildsCorrectTermsAggregation()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Fields(f => f.Keyword("keyword")))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "apple" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "banana" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "cherry" } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @exclude:myexclude @include:myinclude @include:otherinclude @missing:mymissing @exclude:otherexclude @min:1)");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Aggregations(a => a
                .Add("nested_nested", n => n
                    .Nested(ne => ne.Path("nested"))
                    .Aggregations(na => na
                        .Add("terms_nested.field1", t => t
                            .Terms(te => te
                                .Field("nested.field1.keyword")
                                .MinDocCount(1)
                                .Include(new TermsInclude(["myinclude", "otherinclude"]))
                                .Exclude(new TermsExclude(["myexclude", "otherexclude"]))
                                .Missing("mymissing"))
                            .Meta(m => m.Add("@field_type", "keyword")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedDefaultSearch_WithNestedFieldInDefaultFields_SearchesNestedFields()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent",
                Nested = { new MyType { Field1 = "special_value" } }
            },
            new MyNestedType
            {
                Field1 = "other_parent",
                Nested = { new MyType { Field1 = "normal_value" } }
            }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<MyNestedType>(Client)
            .UseNested()
            .SetDefaultFields(["field1", "nested.field1"]));

        var result = await processor.BuildQueryAsync("special_value", new ElasticQueryVisitorContext().UseSearchMode());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Bool(b => b.Should(
                s => s.Match(m => m.Field("field1").Query("special_value")),
                s => s.Nested(n => n
                    .Path("nested")
                    .Query(q2 => q2.Match(m => m.Field("nested.field1").Query("special_value"))))
            ).MinimumShouldMatch(1))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedDefaultSearch_WithMultipleNestedFieldsSamePath_CombinesIntoSingleNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .Text(e1 => e1.Field2)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent",
                Nested = { new MyType { Field1 = "findme", Field2 = "other" } }
            },
            new MyNestedType
            {
                Field1 = "another",
                Nested = { new MyType { Field1 = "other", Field2 = "findme" } }
            },
            new MyNestedType
            {
                Field1 = "nomatch",
                Nested = { new MyType { Field1 = "no", Field2 = "match" } }
            }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<MyNestedType>(Client)
            .UseNested()
            .SetDefaultFields(["field1", "nested.field1", "nested.field2"]));

        var result = await processor.BuildQueryAsync("findme", new ElasticQueryVisitorContext().UseSearchMode());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Bool(b => b.Should(
                s => s.Match(m => m.Field("field1").Query("findme")),
                s => s.Nested(n => n
                    .Path("nested")
                    .Query(q2 => q2.MultiMatch(mm => mm
                        .Fields(Fields.FromStrings(["nested.field1", "nested.field2"]))
                        .Query("findme"))))
            ).MinimumShouldMatch(1))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total);
    }

    [Fact]
    public async Task NestedExistsQuery_WithNestedField_WrapsInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent1",
                Nested = { new MyType { Field1 = "child1", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("_exists_:nested.field1");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Bool(b => b.Filter(f => f.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Exists(e => e.Field("nested.field1"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedMissingQuery_WithNestedField_WrapsInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent1",
                Nested = { new MyType { Field1 = "child1", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("_missing_:nested.field1", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Bool(b => b.MustNot(mn => mn.Exists(e => e.Field("nested.field1"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedExistsQuery_WithRootNestedPath_WrapsInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent1",
                Nested = { new MyType { Field1 = "child1", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field1 = "child2", Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("_exists_:nested");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Bool(b => b.Filter(f => f.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Exists(e => e.Field("nested"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedMissingQuery_WithRootNestedPath_WrapsInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent1",
                Nested = { new MyType { Field1 = "child1", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field1 = "child2", Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("_missing_:nested", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Bool(b => b.MustNot(mn => mn.Exists(e => e.Field("nested"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedIndividualFieldQuery_WithAndCondition_CombinesIntoSingleNestedQueryWithMust()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Field1 = "parent1", Nested = { new MyType { Field1 = "target", Field4 = 5 } } },
            new MyNestedType { Field1 = "parent2", Nested = { new MyType { Field1 = "target", Field4 = 10 } } },
            new MyNestedType { Field1 = "parent3", Nested = { new MyType { Field1 = "other", Field4 = 5 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("nested.field1:target AND nested.field4:5", new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Bool(b => b.Must(
                    m => m.Match(ma => ma.Field("nested.field1").Query("target")),
                    m => m.Term(t => t.Field("nested.field4").Value("5"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedMixedFieldQuery_WithNestedAndNonNestedFields_BuildsCorrectQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1, o => o.Index())
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Field1 = "match_parent", Nested = { new MyType { Field1 = "match_child", Field4 = 5 } } },
            new MyNestedType { Field1 = "match_parent", Nested = { new MyType { Field1 = "other", Field4 = 10 } } },
            new MyNestedType { Field1 = "no_match", Nested = { new MyType { Field1 = "match_child", Field4 = 5 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("field1:match_parent nested.field1:match_child", new ElasticQueryVisitorContext { UseScoring = true });

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Bool(b => b.Must(
                m => m.Match(ma => ma.Field("field1").Query("match_parent")),
                m => m.Nested(n => n
                    .Path(p => p.Nested)
                    .Query(q2 => q2.Match(ma => ma.Field("nested.field1").Query("match_child"))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedSort_WithNestedField_AddsNestedPathToSort()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Field1 = "a", Nested = { new MyType { Field4 = 10 } } },
            new MyNestedType { Field1 = "b", Nested = { new MyType { Field4 = 1 } } },
            new MyNestedType { Field1 = "c", Nested = { new MyType { Field4 = 5 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var sort = await processor.BuildSortAsync("-nested.field4");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Sort(s => s.Field(f => f
                .Field("nested.field4")
                .Order(SortOrder.Desc)
                .UnmappedType(FieldType.Integer)
                .Nested(n => n.Path("nested")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValidResponse);
    }

    [Fact]
    public async Task NestedQuery_WithWildcardOnAnalyzedField_WrapsQueryStringInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "testing" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other" } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("nested.field1:test*", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.QueryString(qs => qs
                    .Fields(Fields.FromStrings(["nested.field1"]))
                    .AllowLeadingWildcard(false)
                    .AnalyzeWildcard(true)
                    .Query("test*"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery_WithWildcardOnNonAnalyzedField_WrapsPrefixInNestedQuery()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Keyword(e1 => e1.Field1)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "testing" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other" } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("nested.field1:test*", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Prefix(pq => pq
                    .Field("nested.field1")
                    .Value("test"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery_WithDeeplyNestedType_ProducesNestedWrapper()
    {
        string index = await CreateRandomIndexAsync<MyDeeplyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested("parent", o => o.Properties(p1 => p1
                .Text("field1")
                .Nested("child", o1 => o1.Properties(p2 => p2
                    .Text("field1")
                    .IntegerNumber("field4")
                ))
            ))
        ));

        await Client.IndexManyAsync([
            new MyDeeplyNestedType
            {
                Field1 = "root",
                Parent =
                {
                    new MyMiddleNestedType
                    {
                        Field1 = "mid",
                        Child = { new MyType { Field1 = "deep_value", Field4 = 42 } }
                    }
                }
            }
        ], index);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyDeeplyNestedType>(Client).UseNested());

        var result = await processor.BuildQueryAsync("parent.field1:mid", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = await Client.SearchAsync<MyDeeplyNestedType>(d => d.Indices(index).Query(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyDeeplyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path("parent")
                .Query(q2 => q2.Match(m => m.Field("parent.field1").Query("mid"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedMixedOperations_WithQueryAndAggregation_HandlesNestedContextCorrectly()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Field1)
            .Nested(e => e.Nested, o => o.Properties(p1 => p1
                .Text(e1 => e1.Field1, o1 => o1.Fields(f => f.Keyword("keyword")))
                .IntegerNumber(e1 => e1.Field4)
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "high", Field4 = 10 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "medium", Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "low", Field4 = 1 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        var queryResult = await processor.BuildQueryAsync("nested.field4:>=5", new ElasticQueryVisitorContext { UseScoring = true });
        var aggResult = await processor.BuildAggregationsAsync("terms:nested.field1 max:nested.field4");

        var actualResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index).Query(queryResult).Aggregations(aggResult));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = await Client.SearchAsync<MyNestedType>(d => d.Indices(index)
            .Query(q => q.Nested(n => n
                .Path("nested")
                .Query(q2 => q2.Range(r => r.Term(tr => tr.Field("nested.field4").Gte("5"))))))
            .Aggregations(a => a
                .Add("nested_nested", n => n
                    .Nested(ne => ne.Path("nested"))
                    .Aggregations(na => na
                        .Add("terms_nested.field1", t => t
                            .Terms(te => te.Field("nested.field1.keyword"))
                            .Meta(m => m.Add("@field_type", "text")))
                        .Add("max_nested.field4", m => m
                            .Max(ma => ma.Field("nested.field4"))
                            .Meta(m2 => m2.Add("@field_type", "integer")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total);
    }

    public class MyDeeplyNestedType
    {
        public string Field1 { get; set; }
        public IList<MyMiddleNestedType> Parent { get; set; } = new List<MyMiddleNestedType>();
    }

    public class MyMiddleNestedType
    {
        public string Field1 { get; set; }
        public IList<MyType> Child { get; set; } = new List<MyType>();
    }
}
