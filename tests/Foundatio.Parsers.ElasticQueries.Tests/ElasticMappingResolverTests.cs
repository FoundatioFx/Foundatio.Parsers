using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticMappingResolverTests : ElasticsearchTestBase
{
    public ElasticMappingResolverTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    private ITypeMapping MapMyNestedType(TypeMappingDescriptor<ElasticNestedQueryParserTests.MyNestedType> m)
    {
        return m
            .AutoMap<ElasticNestedQueryParserTests.MyNestedType>()
            .Dynamic()
            .DynamicTemplates(t => t.DynamicTemplate("idx_text", t => t.Match("text*").Mapping(m => m.Text(mp => mp.AddKeywordAndSortFields()))))
            .Properties(p => p
                .Text(p1 => p1.Name(n => n.Field1).AddKeywordAndSortFields())
                .Text(p1 => p1.Name(n => n.Field4).AddKeywordAndSortFields())
                    .FieldAlias(a => a.Path(n => n.Field4).Name("field4alias"))
                .Text(p1 => p1.Name(n => n.Field5).AddKeywordAndSortFields(true))
            );
    }

    [Fact]
    public void CanResolveCodedProperty()
    {
        string index = CreateRandomIndex<ElasticNestedQueryParserTests.MyNestedType>(MapMyNestedType);

        Client.IndexMany([
            new ElasticNestedQueryParserTests.MyNestedType
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
            new ElasticNestedQueryParserTests.MyNestedType { Field1 = "value2", Field2 = "value2" },
            new ElasticNestedQueryParserTests.MyNestedType { Field1 = "value1", Field2 = "value4" }
        ], index);
        Client.Indices.Refresh(index);

        var resolver = ElasticMappingResolver.Create<ElasticNestedQueryParserTests.MyNestedType>(MapMyNestedType, Client, index, _logger);

        var payloadProperty = resolver.GetMappingProperty("payload");
        Assert.IsType<TextProperty>(payloadProperty);
        Assert.NotNull(payloadProperty.Name);
    }

    [Fact]
    public void CanResolveProperties()
    {
        string index = CreateRandomIndex<ElasticNestedQueryParserTests.MyNestedType>(MapMyNestedType);

        Client.IndexMany([
            new ElasticNestedQueryParserTests.MyNestedType
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
            new ElasticNestedQueryParserTests.MyNestedType { Field1 = "value2", Field2 = "value2" },
            new ElasticNestedQueryParserTests.MyNestedType { Field1 = "value1", Field2 = "value4" }
        ], index);
        Client.Indices.Refresh(index);

        var resolver = ElasticMappingResolver.Create<ElasticNestedQueryParserTests.MyNestedType>(MapMyNestedType, Client, index, _logger);

        string dynamicTextAggregation = resolver.GetAggregationsFieldName("nested.data.text-0001");
        Assert.Equal("nested.data.text-0001.keyword", dynamicTextAggregation);

        string dynamicSpacedAggregation = resolver.GetAggregationsFieldName("nested.data.spaced field");
        Assert.Equal("nested.data.spaced field.keyword", dynamicSpacedAggregation);

        string dynamicSpacedSort = resolver.GetSortFieldName("nested.data.spaced field");
        Assert.Equal("nested.data.spaced field.keyword", dynamicSpacedSort);

        string dynamicSpacedField = resolver.GetResolvedField("nested.data.spaced field");
        Assert.Equal("nested.data.spaced field", dynamicSpacedField);

        var field1Property = resolver.GetMappingProperty("Field1");
        Assert.IsType<TextProperty>(field1Property);

        string field5Property = resolver.GetAggregationsFieldName("Field5");
        Assert.Equal("field5.keyword", field5Property);

        var unknownProperty = resolver.GetMappingProperty("UnknowN.test.doesNotExist");
        Assert.Null(unknownProperty);

        string field1 = resolver.GetResolvedField("FielD1");
        Assert.Equal("field1", field1);

        string emptyField = resolver.GetResolvedField(" ");
        Assert.Equal(" ", emptyField);

        string unknownField = resolver.GetResolvedField("UnknowN.test.doesNotExist");
        Assert.Equal("UnknowN.test.doesNotExist", unknownField);

        string unknownField2 = resolver.GetResolvedField("unknown.test.doesnotexist");
        Assert.Equal("unknown.test.doesnotexist", unknownField2);

        string unknownField3 = resolver.GetResolvedField("unknown");
        Assert.Equal("unknown", unknownField3);

        var field4Property = resolver.GetMappingProperty("Field4");
        Assert.IsType<TextProperty>(field4Property);

        var field4ReflectionProperty = resolver.GetMappingProperty(new Field(typeof(ElasticNestedQueryParserTests.MyNestedType).GetProperty("Field4")));
        Assert.IsType<TextProperty>(field4ReflectionProperty);

        var field4ExpressionProperty = resolver.GetMappingProperty(new Field(GetObjectPath(p => p.Field4)));
        Assert.IsType<TextProperty>(field4ExpressionProperty);

        var field4AliasMapping = resolver.GetMapping("Field4Alias", true);
        Assert.IsType<TextProperty>(field4AliasMapping.Property);
        Assert.Same(field4Property, field4AliasMapping.Property);

        string field4sort = resolver.GetSortFieldName("Field4Alias");
        Assert.Equal("field4.sort", field4sort);

        string field4aggs = resolver.GetAggregationsFieldName("Field4Alias");
        Assert.Equal("field4.keyword", field4aggs);

        var nestedIdProperty = resolver.GetMappingProperty("Nested.Id");
        Assert.IsType<TextProperty>(nestedIdProperty);

        string nestedId = resolver.GetResolvedField("Nested.Id");
        Assert.Equal("nested.id", nestedId);

        nestedIdProperty = resolver.GetMappingProperty("nested.id");
        Assert.IsType<TextProperty>(nestedIdProperty);

        var nestedField1Property = resolver.GetMappingProperty("Nested.Field1");
        Assert.IsType<TextProperty>(nestedField1Property);

        nestedField1Property = resolver.GetMappingProperty("nEsted.fieLD1");
        Assert.IsType<TextProperty>(nestedField1Property);

        var nestedField2Property = resolver.GetMappingProperty("Nested.Field4");
        Assert.IsType<NumberProperty>(nestedField2Property);

        var nestedField5Property = resolver.GetMappingProperty("Nested.Field5");
        Assert.IsType<DateProperty>(nestedField5Property);

        var nestedDataProperty = resolver.GetMappingProperty("Nested.Data");
        Assert.IsType<ObjectProperty>(nestedDataProperty);
    }

    private static Expression GetObjectPath(Expression<Func<ElasticNestedQueryParserTests.MyNestedType, object>> objectPath)
    {
        return objectPath;
    }

    #region Unit tests (no Elasticsearch dependency)

    private static Inferrer CreateInferrer()
    {
        var settings = new ConnectionSettings(new Uri("http://localhost:9200"));
        return new Inferrer(settings);
    }

    private static ITypeMapping CreateTextWithKeywordMapping(string fieldName)
    {
        return new TypeMapping
        {
            Properties = new Properties
            {
                {
                    fieldName, new TextProperty
                    {
                        Name = fieldName,
                        Fields = new Properties
                        {
                            { "keyword", new KeywordProperty { Name = "keyword", IgnoreAbove = 256 } }
                        }
                    }
                }
            }
        };
    }

    private static ITypeMapping CreateTextWithKeywordAndSortMapping(string fieldName)
    {
        return new TypeMapping
        {
            Properties = new Properties
            {
                {
                    fieldName, new TextProperty
                    {
                        Name = fieldName,
                        Fields = new Properties
                        {
                            { "keyword", new KeywordProperty { Name = "keyword", IgnoreAbove = 256 } },
                            { "sort", new KeywordProperty { Name = "sort", IgnoreAbove = 256 } }
                        }
                    }
                }
            }
        };
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var codeMapping = CreateTextWithKeywordMapping("title");
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => null, _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("title", "keyword");

        // Assert
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetAggregationsFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var codeMapping = CreateTextWithKeywordMapping("title");
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => null, _logger);

        // Act
        string result = resolver.GetAggregationsFieldName("title");

        // Assert
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetSortFieldName_WithTextPropertyAndSortSubField_ReturnsSortPath()
    {
        // Arrange
        var codeMapping = CreateTextWithKeywordAndSortMapping("title");
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => null, _logger);

        // Act
        string result = resolver.GetSortFieldName("title");

        // Assert
        Assert.Equal("title.sort", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithKeywordProperty_ReturnsBareFieldName()
    {
        // Arrange
        var codeMapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "status", new KeywordProperty { Name = "status" } }
            }
        };
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => null, _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("status", "keyword");

        // Assert
        Assert.Equal("status", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyWithoutSubFields_ReturnsBareFieldName()
    {
        // Arrange
        var codeMapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "body", new TextProperty { Name = "body" } }
            }
        };
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => null, _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("body", "keyword");

        // Assert
        Assert.Equal("body", result);
    }

    [Fact]
    public void RefreshMapping_WhenCalled_ClearsCachedMappings()
    {
        int serverFetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            int n = Interlocked.Increment(ref serverFetchCount);
            if (n <= 1)
            {
                return new TypeMapping
                {
                    Properties = new Properties
                    {
                        { "name", new TextProperty { Name = "name" } }
                    }
                };
            }

            return CreateTextWithKeywordMapping("name");
        }, CreateInferrer(), _logger);

        // First resolution: server returns TextProperty without keyword sub-field.
        string beforeRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal("name", beforeRefresh);

        // Act: refresh should clear cache so next resolution picks up new server mapping.
        resolver.RefreshMapping();

        // Assert: after refresh, server mapping is fetched again and keyword sub-field is found.
        string afterRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal("name.keyword", afterRefresh);
        Assert.True(serverFetchCount >= 2, "Server mapping should have been fetched at least twice");
    }

    [Fact]
    public void RefreshMapping_ClearsFoundCacheEntries_ForcesReResolution()
    {
        // Arrange: prove that a "found" cache entry is evicted on RefreshMapping.
        // Server mapping changes between first and second resolution.
        int version = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            int v = Interlocked.Increment(ref version);
            if (v == 1)
            {
                return new TypeMapping
                {
                    Properties = new Properties
                    {
                        { "name", new TextProperty { Name = "name" } }
                    }
                };
            }

            return CreateTextWithKeywordMapping("name");
        }, CreateInferrer(), _logger);

        // First resolution: server returns TextProperty without keyword sub-field.
        string first = resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal("name", first);

        // Act: refresh to clear cache; next fetch returns mapping with keyword.
        resolver.RefreshMapping();

        // Assert: the stale "found" entry should be gone; new server mapping with keyword resolves.
        string second = resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal("name.keyword", second);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithCodeAndServerMerge_ReturnsKeywordSubField()
    {
        // Arrange: code mapping has keyword sub-fields, server has bare TextProperty.
        // MergeProperties should carry the code sub-fields through.
        var codeMapping = CreateTextWithKeywordMapping("name");
        var serverMapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "name", new TextProperty { Name = "name" } }
            }
        };
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => serverMapping, _logger);

        // Force server mapping fetch via refresh.
        resolver.RefreshMapping();

        // Act
        string result = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal("name.keyword", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_AfterRefreshAndServerMappingChange_ReturnsUpdatedKeywordPath()
    {
        // Arrange: code mapping has no sub-fields. Server mapping starts empty,
        // then after refresh provides keyword sub-field.
        int callCount = 0;
        var codeMapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "name", new TextProperty { Name = "name" } }
            }
        };
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () =>
        {
            int n = Interlocked.Increment(ref callCount);
            if (n <= 1)
                return null;

            return CreateTextWithKeywordMapping("name");
        }, _logger);

        // First call: code mapping only, no sub-fields.
        string initial = resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal("name", initial);

        // Act: refresh and resolve again (server now returns keyword sub-field).
        resolver.RefreshMapping();

        // Assert
        string updated = resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal("name.keyword", updated);
    }

    [Fact]
    public async Task ConcurrentGetMappingAndRefreshMapping_UnderContention_AlwaysReturnsKeywordPath()
    {
        // Arrange
        var codeMapping = CreateTextWithKeywordMapping("name");
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () =>
        {
            Thread.Sleep(1);
            return CreateTextWithKeywordMapping("name");
        }, _logger);

        const int iterations = 200;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var barrier = new Barrier(3);

        // Act & Assert
        var readerTask = Task.Run(() =>
        {
            barrier.SignalAndWait(cts.Token);
            for (int i = 0; i < iterations; i++)
            {
                string result = resolver.GetNonAnalyzedFieldName("name", "keyword");
                Assert.Equal("name.keyword", result);
            }
        }, cts.Token);

        var aggregationReaderTask = Task.Run(() =>
        {
            barrier.SignalAndWait(cts.Token);
            for (int i = 0; i < iterations; i++)
            {
                string result = resolver.GetAggregationsFieldName("name");
                Assert.Equal("name.keyword", result);
            }
        }, cts.Token);

        var refreshTask = Task.Run(() =>
        {
            barrier.SignalAndWait(cts.Token);
            for (int i = 0; i < iterations; i++)
            {
                resolver.RefreshMapping();
                Thread.Sleep(0);
            }
        }, cts.Token);

        await Task.WhenAll(readerTask, aggregationReaderTask, refreshTask);
    }

    #endregion
}
