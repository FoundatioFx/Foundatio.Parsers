using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Microsoft.Extensions.Logging;
using Nest;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticNestedQueryParserTests : ElasticsearchTestBase
{
    public ElasticNestedQueryParserTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    [Fact]
    public async Task NestedQuery_WithFieldMapAndSingleNestedField_BuildsCorrectNestedQuery()
    {
        // Arrange
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        // Act
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseFieldMap(new FieldMap { { "blah", "nested" } }).UseMappings<MyNestedType>(Client).UseNested());
        var result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1)", new ElasticQueryVisitorContext().UseScoring());

        // Assert
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
    }

    [Fact]
    public async Task NestedQuery_WithFieldMapAndMultipleNestedFields_BuildsCorrectNestedQuery()
    {
        // Arrange
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        // Act
        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseFieldMap(new FieldMap { { "blah", "nested" } }).UseMappings<MyNestedType>(Client).UseNested());
        var result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1 blah.field4:4)", new ElasticQueryVisitorContext().UseScoring());

        // Assert
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
                            .Query("value1"))
                        && q2.Term(t => t.Field("nested.field4").Value(4))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedQuery_WithMultipleNestedFieldsAndConditions_BuildsCorrectNestedQuery()
    {
        // Arrange
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("field1:value1 nested:(nested.field1:value1 nested.field4:4 nested.field3:value3)",
                new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
            && q.Nested(n => n.Path(p => p.Nested).Query(q2 =>
                q2.Match(m => m.Field("nested.field1").Query("value1"))
                && q2.Term(t => t.Field("nested.field4").Value(4))
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

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
                .Query(q2 => q2.Term(t => t.Field("nested.field4").Value(5))))));

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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

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
                    || q2.Term(t => t.Field("nested.field4").Value(10))))));

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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

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
    public async Task NestedQuery_WithNegation_BuildsCorrectMustNotNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1).Index())
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Index())
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Field1 = "parent1", Nested = { new MyType { Field1 = "excluded_value", Field4 = 10 } } },
            new MyNestedType { Field1 = "parent2", Nested = { new MyType { Field1 = "included_value", Field4 = 20 } } },
            new MyNestedType { Field1 = "parent3", Nested = { new MyType { Field1 = "another_value", Field4 = 30 } } }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("NOT nested:(nested.field1:excluded_value)", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
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
        Assert.Equal(2, actualResponse.Total); // Should match parent2 and parent3
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("terms:nested.field4");

        // Assert
        Assert.NotNull(result);
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
                .Text(e => e.Name(n => n.Field1).Fields(f => f.Keyword(k => k.Name("keyword"))))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "test", Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other", Field4 = 10 } } }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Parse and examine the result after visitors have run
        var context = new ElasticQueryVisitorContext { QueryType = QueryTypes.Aggregation };
        var parsedNode = await processor.ParseAsync("terms:nested.field1 terms:nested.field4 max:nested.field4", context);
        Assert.NotNull(parsedNode);
        _logger.LogInformation("Parsed node (after visitors): {Node}", await DebugQueryVisitor.RunAsync(parsedNode));

        // Check nested paths on term nodes
        void LogNestedPaths(IQueryNode node, string indent = "")
        {
            if (node is IFieldQueryNode fieldNode)
            {
                _logger.LogInformation("{Indent}FieldNode: Field={Field}, NestedPath={NestedPath}",
                    indent, fieldNode.Field, fieldNode.GetNestedPath() ?? "null");
            }
            if (node is GroupNode groupNode)
            {
                _logger.LogInformation("{Indent}GroupNode: Field={Field}", indent, groupNode.Field ?? "null");
                foreach (var child in groupNode.Children)
                    LogNestedPaths(child, indent + "  ");
            }
        }
        LogNestedPaths(parsedNode);

        // Act
        var result = await processor.BuildAggregationsAsync("terms:nested.field1 terms:nested.field4 max:nested.field4");

        // Assert
        Assert.NotNull(result);
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        // Note: @field_type is "text" because the property lookup uses the original field (nested.field1),
        // not the resolved aggregation field (nested.field1.keyword)
        // Note: The order matches the depth-first bottom-up visitor order (rightmost first)
        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_nested", n => n
                    .Path("nested")
                    .Aggregations(na => na
                        .Terms("terms_nested.field4", t => t
                            .Field("nested.field4")
                            .Meta(m => m.Add("@field_type", "integer")))
                        .Max("max_nested.field4", m => m
                            .Field("nested.field4")
                            .Meta(m2 => m2.Add("@field_type", "integer")))
                        .Terms("terms_nested.field1", t => t
                            .Field("nested.field1.keyword")
                            .Meta(m => m.Add("@field_type", "text")))))));

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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @include:apple @include:banana @include:cherry)");

        // Assert
        Assert.NotNull(result);
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
                            .Meta(m => m.Add("@field_type", "keyword")))))));  // "keyword" because GroupNode uses resolved aggregation field

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedAggregation_WithIncludeExcludeMissingMin_BuildsCorrectTermsAggregation()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1).Fields(f => f.Keyword(k => k.Name("keyword"))))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "apple" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "banana" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "cherry" } } }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @exclude:myexclude @include:myinclude @include:otherinclude @missing:mymissing @exclude:otherexclude @min:1)");

        // Assert
        Assert.NotNull(result);
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
                            .MinimumDocumentCount(1)
                            .Include(["myinclude", "otherinclude"])
                            .Exclude(["myexclude", "otherexclude"])
                            .Missing("mymissing")
                            .Meta(m => m.Add("@field_type", "keyword")))))));  // "keyword" because GroupNode uses resolved aggregation field

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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

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
    public async Task NestedDefaultSearch_WithMultipleNestedFieldsSamePath_CombinesIntoSingleNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Text(e => e.Name(n => n.Field2))
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<MyNestedType>(Client)
            .UseNested()
            .SetDefaultFields(["field1", "nested.field1", "nested.field2"]));

        // Act
        var result = await processor.BuildQueryAsync("findme", new ElasticQueryVisitorContext().UseSearchMode());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        // Expected: regular match for field1, nested with multi_match for nested.field1 and nested.field2
        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Match(m => m.Field("field1").Query("findme"))
                || q.Nested(n => n
                    .Path("nested")
                    .Query(q2 => q2.MultiMatch(mm => mm
                        .Fields(f => f.Fields("nested.field1", "nested.field2"))
                        .Query("findme"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total); // Should match both docs with "findme" in nested fields
    }

    [Fact]
    public async Task NestedDefaultSearch_WithMixedFieldTypes_SplitsIntoAppropriateQueries()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "parent",
                Field4 = 42,
                Nested = { new MyType { Field1 = "child", Field4 = 99 } }
            },
            new MyNestedType
            {
                Field1 = "42", // Field1 contains "42" as text
                Field4 = 0,
                Nested = { new MyType { Field1 = "other", Field4 = 42 } } // nested.field4 = 42
            },
            new MyNestedType
            {
                Field1 = "nomatch",
                Field4 = 100,
                Nested = { new MyType { Field1 = "no", Field4 = 100 } }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<MyNestedType>(Client)
            .UseNested()
            .SetDefaultFields(["field1", "field4", "nested.field1", "nested.field4"]));

        // Act
        var result = await processor.BuildQueryAsync("42", new ElasticQueryVisitorContext().UseSearchMode());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        // Expected: match for text fields, term for integer fields, with nested wrappers
        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Match(m => m.Field("field1").Query("42"))
                || q.Term(t => t.Field("field4").Value(42))
                || q.Nested(n => n
                    .Path("nested")
                    .Query(q2 => q2.Match(m => m.Field("nested.field1").Query("42"))
                        || q2.Term(t => t.Field("nested.field4").Value(42))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(2, actualResponse.Total); // Matches doc with Field1="42"/Field4=42 and doc with nested.Field4=42
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var queryResult = await processor.BuildQueryAsync("nested.field4:>=5", new ElasticQueryVisitorContext { UseScoring = true });
        var aggResult = await processor.BuildAggregationsAsync("terms:nested.field1 max:nested.field4");
        Assert.NotNull(aggResult);

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
        Assert.Equal(2, actualResponse.Total); // Should match high and medium
    }

    [Fact]
    public async Task NestedExistsQuery_WithNestedField_WrapsInNestedQuery()
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
                Nested = { new MyType { Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("_exists_:nested.field1");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
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
                Nested = { new MyType { Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("_missing_:nested.field1", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
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
                Nested = { new MyType { Field1 = "child2", Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("_exists_:nested");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
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
                Nested = { new MyType { Field1 = "child2", Field4 = 10 } }
            },
            new MyNestedType { Field1 = "parent3" }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("_missing_:nested", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
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
                Nested = { new MyType { Field1 = "target", Field4 = 10 } }
            },
            new MyNestedType
            {
                Field1 = "parent3",
                Nested = { new MyType { Field1 = "other", Field4 = 5 } }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("nested.field1:target AND nested.field4:5", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.Match(m => m.Field("nested.field1").Query("target"))
                    && q2.Term(t => t.Field("nested.field4").Value(5))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedMixedFieldQuery_WithNestedAndNonNestedFields_BuildsCorrectQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1).Index())
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "match_parent",
                Nested = { new MyType { Field1 = "match_child", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "match_parent",
                Nested = { new MyType { Field1 = "other", Field4 = 10 } }
            },
            new MyNestedType
            {
                Field1 = "no_match",
                Nested = { new MyType { Field1 = "match_child", Field4 = 5 } }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("field1:match_parent nested.field1:match_child", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Match(m => m.Field("field1").Query("match_parent"))
                && q.Nested(n => n
                    .Path(p => p.Nested)
                    .Query(q2 => q2.Match(m => m.Field("nested.field1").Query("match_child"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
        Assert.Equal(1, actualResponse.Total);
    }

    [Fact]
    public async Task NestedSort_WithNestedField_AddsNestedPathToSort()
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
            new MyNestedType { Field1 = "a", Nested = { new MyType { Field4 = 10 } } },
            new MyNestedType { Field1 = "b", Nested = { new MyType { Field4 = 1 } } },
            new MyNestedType { Field1 = "c", Nested = { new MyType { Field4 = 5 } } }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var sort = await processor.BuildSortAsync("-nested.field4");

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Sort(s => s.Field(f => f
                .Field("nested.field4")
                .Order(SortOrder.Descending)
                .UnmappedType(FieldType.Integer)
                .Nested(n => n.Path("nested")))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValid);
    }

    [Fact]
    public async Task NestedQuery_WithWildcardOnAnalyzedField_WrapsQueryStringInNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "testing" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other" } } }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("nested.field1:test*", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 => q2.QueryString(qs => qs
                    .Fields(f => f.Field("nested.field1"))
                    .AllowLeadingWildcard(false)
                    .AnalyzeWildcard()
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
        // Arrange
        string index = CreateRandomIndex<MyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Field1))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "testing" } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other" } } }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildQueryAsync("nested.field1:test*", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
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
        // Arrange
        string index = CreateRandomIndex<MyDeeplyNestedType>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Field1))
            .Nested<MyMiddleNestedType>(r => r.Name(n => n.Parent.First()).Properties(p1 => p1
                .Text(e => e.Name(n => n.Field1))
                .Nested<MyType>(r2 => r2.Name(n => n.Child.First()).Properties(p2 => p2
                    .Text(e => e.Name(n => n.Field1))
                    .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
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
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyDeeplyNestedType>(Client).UseNested());

        // Act - query a field on the first level of nesting
        var result = await processor.BuildQueryAsync("parent.field1:mid", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyDeeplyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyDeeplyNestedType>(d => d.Index(index)
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
    public async Task NestedQuery_WithSameFieldAtMultipleLevels_BuildsCorrectBooleanStructure()
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
                Nested = { new MyType { Field1 = "excluded", Field4 = 5 } }
            },
            new MyNestedType
            {
                Field1 = "parent2",
                Nested = { new MyType { Field1 = "other", Field4 = 10 } }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act - nested group containing negation and alternative
        var result = await processor.BuildQueryAsync("nested:(-nested:(nested.field1:excluded) OR nested.field4:10)", new ElasticQueryVisitorContext().UseScoring());

        // Assert
        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MyNestedType>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path(p => p.Nested)
                .Query(q2 =>
                    q2.Bool(b => b.MustNot(mn => mn.Nested(n2 => n2
                        .Path(p => p.Nested)
                        .Query(nq => nq.Match(m => m.Field("nested.field1").Query("excluded"))))))
                    || q2.Term(t => t.Field("nested.field4").Value(10))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    public class MyDeeplyNestedType
    {
        public string Field1 { get; set; } = null!;
        public IList<MyMiddleNestedType> Parent { get; set; } = new List<MyMiddleNestedType>();
    }

    public class MyMiddleNestedType
    {
        public string Field1 { get; set; } = null!;
        public IList<MyType> Child { get; set; } = new List<MyType>();
    }

    public class MyNestedType
    {
        public string Field1 { get; set; } = null!;
        public string Field2 { get; set; } = null!;
        public string Field3 { get; set; } = null!;
        public int Field4 { get; set; }
        public string Field5 { get; set; } = null!;
        public string Payload { get; set; } = null!;
        public IList<MyType> Nested { get; set; } = new List<MyType>();
    }

    [Fact]
    public async Task NestedFilterQuery_WithResellerFilter_AddsFilterToNestedInnerQuery()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Keyword(e => e.Name(n => n.Category))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers =
                {
                    new Reseller { Name = "Official", Price = 10.0 },
                    new Reseller { Name = "ThirdParty", Price = 8.0 }
                }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "resellers" ? new TermQuery { Field = "resellers.name", Value = "Official" } : null)
            .UseNested());

        // Act
        var result = await processor.BuildQueryAsync("resellers.price:10", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<Product>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path("resellers")
                .Query(q2 => q2.Term(t => t.Field("resellers.price").Value(10.0))
                    && q2.Term(t => t.Field("resellers.name").Value("Official"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedFilterQuery_WithMultipleFieldsSamePath_AppliesFilterOnceInCoalescedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers =
                {
                    new Reseller { Name = "Official", Price = 10.0 },
                    new Reseller { Name = "ThirdParty", Price = 8.0 }
                }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "resellers" ? new TermQuery { Field = "resellers.name", Value = "Official" } : null)
            .UseNested());

        // Act
        var result = await processor.BuildQueryAsync("resellers.name:Official AND resellers.price:10", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<Product>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path("resellers")
                .Query(q2 => q2.Term(t => t.Field("resellers.name").Value("Official"))
                    && q2.Term(t => t.Field("resellers.price").Value(10.0))
                    && q2.Term(t => t.Field("resellers.name").Value("Official"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedFilterAggregation_WithResellerFilter_WrapsInFilterAggregation()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers =
                {
                    new Reseller { Name = "Official", Price = 10.0 },
                    new Reseller { Name = "ThirdParty", Price = 8.0 }
                }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "resellers" ? new TermQuery { Field = "resellers.name", Value = "Official" } : null)
            .UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("max:resellers.price");

        // Assert
        Assert.NotNull(result);
        var actualResponse = Client.Search<Product>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_resellers", n => n
                    .Path("resellers")
                    .Aggregations(na => na
                        .Filter("filtered_max_resellers.price", f => f
                            .Filter(fq => fq.Term(t => t.Field("resellers.name").Value("Official")))
                            .Aggregations(fa => fa
                                .Max("max_resellers.price", m => m
                                    .Field("resellers.price")
                                    .Meta(m2 => m2.Add("@field_type", "double")))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
    }

    [Fact]
    public async Task NestedFilterSort_WithResellerFilter_SetsNestedSortFilter()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers =
                {
                    new Reseller { Name = "Official", Price = 10.0 },
                    new Reseller { Name = "ThirdParty", Price = 8.0 }
                }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "resellers" ? new TermQuery { Field = "resellers.name", Value = "Official" } : null)
            .UseNested());

        // Act
        var sort = await processor.BuildSortAsync("-resellers.price");

        // Assert
        var actualResponse = Client.Search<Product>(d => d.Index(index).Sort(sort));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Sort(s => s.Field(f => f
                .Field("resellers.price")
                .Order(SortOrder.Descending)
                .UnmappedType(FieldType.Double)
                .Nested(n => n
                    .Path("resellers")
                    .Filter(fq => fq.Term(t => t.Field("resellers.name").Value("Official")))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.True(actualResponse.IsValid);
    }

    [Fact]
    public async Task NestedFilterQuery_WithGroupedNestedFields_AddsFilterToGroupInnerQuery()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers =
                {
                    new Reseller { Name = "Official", Price = 10.0 },
                    new Reseller { Name = "ThirdParty", Price = 8.0 }
                }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "resellers" ? new TermQuery { Field = "resellers.name", Value = "Official" } : null)
            .UseNested());

        // Act
        var result = await processor.BuildQueryAsync("resellers:(resellers.name:Official resellers.price:10)", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<Product>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path("resellers")
                .Query(q2 => q2.Term(t => t.Field("resellers.name").Value("Official"))
                    && q2.Term(t => t.Field("resellers.price").Value(10.0))
                    && q2.Term(t => t.Field("resellers.name").Value("Official"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedFilterQuery_WithNullResolver_ProducesUnfilteredNestedQuery()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers = { new Reseller { Name = "Official", Price = 10.0 } }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) => (QueryContainer?)null)
            .UseNested());

        // Act
        var result = await processor.BuildQueryAsync("resellers.price:10", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<Product>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Query(q => q.Nested(n => n
                .Path("resellers")
                .Query(q2 => q2.Term(t => t.Field("resellers.price").Value(10.0))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedFilterQuery_WithMultiplePaths_AppliesDifferentFiltersPerPath()
    {
        // Arrange
        string index = CreateRandomIndex<MultiNestedProduct>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
            .Nested<Tag>(r => r.Name(n => n.Tags.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Label))
            ))
        ));

        await Client.IndexManyAsync([
            new MultiNestedProduct
            {
                Name = "Widget",
                Resellers = { new Reseller { Name = "Official", Price = 10.0 } },
                Tags = { new Tag { Label = "sale" } }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<MultiNestedProduct>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) => path switch
            {
                "resellers" => new TermQuery { Field = "resellers.name", Value = "Official" },
                "tags" => new TermQuery { Field = "tags.label", Value = "sale" },
                _ => null
            })
            .UseNested());

        // Act
        var result = await processor.BuildQueryAsync("resellers.price:10 AND tags.label:sale", new ElasticQueryVisitorContext { UseScoring = true });

        // Assert
        var actualResponse = Client.Search<MultiNestedProduct>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<MultiNestedProduct>(d => d.Index(index)
            .Query(q =>
                q.Nested(n => n
                    .Path("resellers")
                    .Query(q2 => q2.Term(t => t.Field("resellers.price").Value(10.0))
                        && q2.Term(t => t.Field("resellers.name").Value("Official"))))
                && q.Nested(n => n
                    .Path("tags")
                    .Query(q2 => q2.Term(t => t.Field("tags.label").Value("sale"))
                        && q2.Term(t => t.Field("tags.label").Value("sale"))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
        Assert.Equal(expectedResponse.Total, actualResponse.Total);
    }

    [Fact]
    public async Task NestedFilterAggregation_WithMultipleFields_EachGetsOwnFilterAggregation()
    {
        // Arrange
        string index = CreateRandomIndex<Product>(d => d.Properties(p => p
            .Text(e => e.Name(n => n.Name))
            .Nested<Reseller>(r => r.Name(n => n.Resellers.First()).Properties(p1 => p1
                .Keyword(e => e.Name(n => n.Name))
                .Number(e => e.Name(n => n.Price).Type(NumberType.Double))
            ))
        ));

        await Client.IndexManyAsync([
            new Product
            {
                Name = "Widget",
                Resellers =
                {
                    new Reseller { Name = "Official", Price = 10.0 },
                    new Reseller { Name = "ThirdParty", Price = 8.0 }
                }
            }
        ], cancellationToken: TestCancellationToken);
        await Client.Indices.RefreshAsync(index, ct: TestCancellationToken);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<Product>(Client)
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "resellers" ? new TermQuery { Field = "resellers.name", Value = "Official" } : null)
            .UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("max:resellers.price terms:resellers.name");

        // Assert
        Assert.NotNull(result);
        var actualResponse = Client.Search<Product>(d => d.Index(index).Aggregations(result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        var expectedResponse = Client.Search<Product>(d => d.Index(index)
            .Aggregations(a => a
                .Nested("nested_resellers", n => n
                    .Path("resellers")
                    .Aggregations(na => na
                        .Filter("filtered_max_resellers.price", f => f
                            .Filter(fq => fq.Term(t => t.Field("resellers.name").Value("Official")))
                            .Aggregations(fa => fa
                                .Max("max_resellers.price", m => m
                                    .Field("resellers.price")
                                    .Meta(m2 => m2.Add("@field_type", "double")))))
                        .Filter("filtered_terms_resellers.name", f => f
                            .Filter(fq => fq.Term(t => t.Field("resellers.name").Value("Official")))
                            .Aggregations(fa => fa
                                .Terms("terms_resellers.name", ts => ts
                                    .Field("resellers.name")
                                    .Meta(m => m.Add("@field_type", "keyword")))))))));

        string expectedRequest = expectedResponse.GetRequest();
        _logger.LogInformation("Expected: {Request}", expectedRequest);

        Assert.Equal(expectedRequest, actualRequest);
    }

    public class Product
    {
        public string Name { get; set; } = null!;
        public string Category { get; set; } = null!;
        public IList<Reseller> Resellers { get; set; } = new List<Reseller>();
    }

    public class MultiNestedProduct
    {
        public string Name { get; set; } = null!;
        public IList<Reseller> Resellers { get; set; } = new List<Reseller>();
        public IList<Tag> Tags { get; set; } = new List<Tag>();
    }

    public class Reseller
    {
        public string Name { get; set; } = null!;
        public double Price { get; set; }
    }

    public class Tag
    {
        public string Label { get; set; } = null!;
    }
}
