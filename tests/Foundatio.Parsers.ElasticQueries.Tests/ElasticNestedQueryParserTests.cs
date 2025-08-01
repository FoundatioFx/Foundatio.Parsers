using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticNestedQueryParserTests : ElasticsearchTestBase
{
    public ElasticNestedQueryParserTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    [Fact]
    public async Task NestedFilterProcessorWithFieldMapAsync()
    {
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
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

        var actualResponse = Client.Search<MyNestedType>(d => d.Query(_ => result));
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

        result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1 blah.field4:4)", new ElasticQueryVisitorContext().UseScoring());

        actualResponse = Client.Search<MyNestedType>(d => d.Query(_ => result));
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
    public async Task NestedFilterProcessor()
    {
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
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

        var actualResponse = Client.Search<MyNestedType>(d => d.Query(_ => result));
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
    public async Task NestedIndividualFieldQuery_WithSingleNestedField_WrapsInNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
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

        // Act
        var result = await processor.BuildQueryAsync("nested.field4:5", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Term("nested.field4", "5")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedIndividualFieldQuery_WithMultipleNestedFieldsOrCondition_CombinesIntoSingleNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
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

        // Act
        var result = await processor.BuildQueryAsync("nested.field1:target OR nested.field4:10", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Match(m => m.Field("nested.field1").Query("target"))
                    || q2.Term("nested.field4", "10")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total);
    }

    [Fact]
    public async Task NestedIndividualFieldQuery_WithRangeQuery_WrapsInNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 15 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 25 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("nested.field4:[10 TO 20]", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.TermRange(r => r.Field("nested.field4").GreaterThanOrEquals("10").LessThanOrEquals("20"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithSingleNestedField_AutomaticallyWrapsInNestedAggregation()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 10 } } },
            new MyNestedType { Nested = { new MyType { Field4 = 5 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("terms:nested.field4");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_nested", n => n
                    .Path("nested")
                    .Aggregations(na => na
                        .Terms("terms_nested.field4", t => t
                            .Field("nested.field4")
                            .Meta(m => m.Add("@field_type", "integer")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithMultipleNestedFields_CombinesIntoSingleNestedAggregation()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "test", Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other", Field4 = 10 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("terms:nested.field1 terms:nested.field4 max:nested.field4");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_nested", n => n
                    .Path("nested")
                    .Aggregations(na => na
                        .Terms("terms_nested.field1", t => t
                            .Field("nested.field1.keyword")
                            .Meta(m => m.Add("@field_type", "text")))
                        .Terms("terms_nested.field4", t => t
                            .Field("nested.field4")
                            .Meta(m => m.Add("@field_type", "integer")))
                        .Max("max_nested.field4", m => m
                            .Field("nested.field4")
                            .Meta(m2 => m2.Add("@field_type", "integer")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithIncludeCommaSeparatedValues_FiltersCorrectly()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Fields(f => f.Keyword(k => k.Name("keyword"))))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
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

        // Act
        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @include:apple,banana,cherry @include:1,2,3)");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_nested", n => n
                    .Path("nested")
                    .Aggregations(na => na
                        .Terms("terms_nested.field1", t => t
                            .Field("nested.field1.keyword")
                            .Include(["apple", "banana", "cherry"])
                            .Meta(m => m.Add("@field_type", "text")))
                        .Terms("terms_nested.field4", t => t
                            .Field("nested.field4")
                            .Include(["1", "2", "3"])
                            .Meta(m => m.Add("@field_type", "integer")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithExcludeCommaSeparatedValues_FiltersCorrectly()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Fields(f => f.Keyword(k => k.Name("keyword"))))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
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

        // Act
        
        var result = await processor.BuildAggregationsAsync(
            "terms:(nested.field1 @exclude:cherry @exclude:date) " +
            "terms:(nested.field4 @exclude:3 @exclude:4)"
        );

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Aggregations(result));        
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_nested", n => n
                    .Path("nested")
                    .Aggregations(na => na
                        .Terms("terms_nested.field1", t => t
                            .Field("nested.field1.keyword")
                            .Exclude(["cherry","date"])
                            .Meta(m => m.Add("@field_type", "keyword")))
                        .Terms("terms_nested.field4", t => t
                            .Field("nested.field4")
                            .Exclude(["3","4"])
                            .Meta(m => m.Add("@field_type", "integer")))))));

        // expected response buckets
        var expectedNestedAgg = expectedResponse.Aggregations.Nested("nested_nested");
        var expectedField1Terms = expectedNestedAgg.Terms("terms_nested.field1");
        var expectedField4Terms = expectedNestedAgg.Terms("terms_nested.field4");
                
        // actual response buckets
        var actualNestedAgg = actualResponse.Aggregations.Nested("nested_nested");
        var actualField1Terms = actualNestedAgg.Terms("terms_nested.field1");
        var actualField4Terms = actualNestedAgg.Terms("terms_nested.field4");

        // Add assertions for bucket counts and contents
        Assert.Equal(expectedField1Terms.Buckets.Count, actualField1Terms.Buckets.Count);
        Assert.Equal(expectedField4Terms.Buckets.Count, actualField4Terms.Buckets.Count);

        // Verify field1 buckets (apple and banana should be included, cherry and date excluded)
        Assert.Contains(actualField1Terms.Buckets, b => b.Key == "apple" && b.DocCount == 1);
        Assert.Contains(actualField1Terms.Buckets, b => b.Key == "banana" && b.DocCount == 1);
        Assert.DoesNotContain(actualField1Terms.Buckets, b => b.Key == "cherry");
        Assert.DoesNotContain(actualField1Terms.Buckets, b => b.Key == "date");

        // Verify field4 buckets (1 and 2 should be included, 3 and 4 excluded)
        Assert.Contains(actualField4Terms.Buckets, b => b.Key == "1" && b.DocCount == 1);
        Assert.Contains(actualField4Terms.Buckets, b => b.Key == "2" && b.DocCount == 1);
        Assert.DoesNotContain(actualField4Terms.Buckets, b => b.Key == "3");
        Assert.DoesNotContain(actualField4Terms.Buckets, b => b.Key == "4");

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedDefaultSearch_WithNestedFieldInDefaultFields_SearchesNestedFields()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
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

        // Act
        var result = await processor.BuildQueryAsync("special_value", new ElasticQueryVisitorContext().UseSearchMode());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Match(m => m.Field("field1").Query("special_value"))
                || q.Nested(n => n
                    .Path("nested")
                    .Query(q2 => q2.Match(m => m.Field("nested.field1").Query("special_value"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedMixedOperations_WithQueryAndAggregation_HandlesNestedContextCorrectly()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Fields(f => f.Keyword(k => k.Name("keyword"))))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "high", Field4 = 10 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "medium", Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "low", Field4 = 1 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act - Query with nested field filter
        var queryResult = await processor.BuildQueryAsync("nested.field4:>=5", new ElasticQueryVisitorContext { UseScoring = true });

        // Act - Aggregation on nested fields
        var aggResult = await processor.BuildAggregationsAsync("terms:nested.field1 max:nested.field4");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => queryResult).Aggregations(aggResult));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path("nested")
                .Query(q2 => q2.TermRange(r => r.Field("nested.field4").GreaterThanOrEquals("5")))))
            .Aggregations(a => a
                .Nested("nested_nested", n => n
                    .Path("nested")
                    .Aggregations(na => na
                        .Terms("terms_nested.field1", t => t
                            .Field("nested.field1.keyword")
                            .Meta(m => m.Add("@field_type", "text")))
                        .Max("max_nested.field4", m => m
                            .Field("nested.field4")
                            .Meta(m2 => m2.Add("@field_type", "integer")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total);
        var documents = actualResponse.Documents.ToList();
        var field1Values = documents.Select(d => d.Nested.First().Field1).ToList();

        // Verify that we have both high and medium values in the results
        Assert.Contains("high", field1Values);
        Assert.Contains("medium", field1Values);
        Assert.DoesNotContain("low", field1Values);
    }


    public class MyNestedType
    {
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        public string Field3 { get; set; }
        public int Field4 { get; set; }
        public string Field5 { get; set; }
        public string Payload { get; set; }
        public IList<MyType> Nested { get; set; } = new List<MyType>();
    }
}
