using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
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
    public async Task NestedQuery_WithFieldMapAndSingleNestedField_BuildsCorrectNestedQuery()
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
    }

    [Fact]
    public async Task NestedQuery_WithFieldMapAndMultipleNestedFields_BuildsCorrectNestedQuery()
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
        var result = await processor.BuildQueryAsync("field1:value1 blah:(blah.field1:value1 blah.field4:4)", new ElasticQueryVisitorContext().UseScoring());

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
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act - search for documents where nested field1 is NOT "excluded_value"
        var result = await processor.BuildQueryAsync("NOT nested:(nested.field1:excluded_value)", new ElasticQueryVisitorContext().UseScoring());

        var actualResponse = Client.Search<MyNestedType>(d => d.Index(index).Query(_ => result));
        string actualRequest = actualResponse.GetRequest();
        _logger.LogInformation("Actual: {Request}", actualRequest);

        // Assert - should exclude documents with nested.field1 = "excluded_value"
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
                .Text(e => e.Name(n => n.Field1).Fields(f => f.Keyword(k => k.Name("keyword"))))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
            ))
        ));

        await Client.IndexManyAsync([
            new MyNestedType { Nested = { new MyType { Field1 = "test", Field4 = 5 } } },
            new MyNestedType { Nested = { new MyType { Field1 = "other", Field4 = 10 } } }
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Parse and examine the result after visitors have run
        var context = new ElasticQueryVisitorContext { QueryType = QueryTypes.Aggregation };
        var parsedNode = await processor.ParseAsync("terms:nested.field1 terms:nested.field4 max:nested.field4", context);
        _logger.LogInformation("Parsed node (after visitors): {Node}", await DebugQueryVisitor.RunAsync(parsedNode));

        // Check nested paths on term nodes
        void LogNestedPaths(Foundatio.Parsers.LuceneQueries.Nodes.IQueryNode node, string indent = "")
        {
            if (node is Foundatio.Parsers.LuceneQueries.Nodes.IFieldQueryNode fieldNode)
            {
                _logger.LogInformation("{Indent}FieldNode: Field={Field}, NestedPath={NestedPath}",
                    indent, fieldNode.Field, fieldNode.GetNestedPath() ?? "null");
            }
            if (node is Foundatio.Parsers.LuceneQueries.Nodes.GroupNode groupNode)
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
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act - multiple @include values should be combined into one list
        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @include:apple @include:banana @include:cherry)");

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
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings<MyNestedType>(Client).UseNested());

        // Act
        var result = await processor.BuildAggregationsAsync("terms:(nested.field1 @exclude:myexclude @include:myinclude @include:otherinclude @missing:mymissing @exclude:otherexclude @min:1)");

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
        // When default fields include nested fields, we can't use multi_match because:
        // 1. Nested fields require a NestedQuery wrapper
        // 2. Multi_match across nested and non-nested fields is invalid
        // We need to split into separate queries: regular match for non-nested, nested(match) for nested

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
    public async Task NestedDefaultSearch_WithMultipleNestedFieldsSamePath_CombinesIntoSingleNestedQuery()
    {
        // When multiple nested fields from the same path are in default fields,
        // they should be combined into a single NestedQuery with multi_match inside

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
        ]);
        await Client.Indices.RefreshAsync(index);

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
        // When default fields include both text and non-text types across nested and non-nested,
        // we need to split into appropriate query types for each field

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
        ]);
        await Client.Indices.RefreshAsync(index);

        var processor = new ElasticQueryParser(c => c
            .SetLoggerFactory(Log)
            .UseMappings<MyNestedType>(Client)
            .UseNested()
            .SetDefaultFields(["field1", "field4", "nested.field1", "nested.field4"]));

        // Act - search for "42"
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
        Assert.Equal(2, actualResponse.Total); // Should match high and medium
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
