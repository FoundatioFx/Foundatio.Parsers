using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticMappingResolverUnitTests : TestWithLoggingBase, IDisposable
{
    private readonly ElasticsearchClientSettings _clientSettings;
    private readonly Inferrer _inferrer;

    public ElasticMappingResolverUnitTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        _clientSettings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        _inferrer = new Inferrer(_clientSettings);
    }

    public void Dispose()
    {
        (_clientSettings as IDisposable)?.Dispose();
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), _inferrer, () => null, logger: _logger);

        // Act
        string? result = resolver.GetNonAnalyzedFieldName("title", "keyword");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetAggregationsFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), _inferrer, () => null, logger: _logger);

        // Act
        string? result = resolver.GetAggregationsFieldName("title");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetSortFieldName_WithTextPropertyAndSortSubField_ReturnsSortPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordAndSortMapping("title"), _inferrer, () => null, logger: _logger);

        // Act
        string? result = resolver.GetSortFieldName("title");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("title.sort", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithKeywordProperty_ReturnsBareFieldName()
    {
        // Arrange
        var props = new Properties();
        props.Add("status", new KeywordProperty());
        var codeMapping = new TypeMapping { Properties = props };
        var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => null, logger: _logger);

        // Act
        string? result = resolver.GetNonAnalyzedFieldName("status", "keyword");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("status", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyWithoutSubFields_ReturnsBareFieldName()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("body"), _inferrer, () => null, logger: _logger);

        // Act
        string? result = resolver.GetNonAnalyzedFieldName("body", "keyword");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("body", result);
    }

    [Fact]
    public void RefreshMapping_WhenCalled_ClearsCachedMappings()
    {
        // Arrange
        int serverFetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            int callNumber = Interlocked.Increment(ref serverFetchCount);
            return callNumber <= 1
                ? CreateTextOnlyMapping("name")
                : CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        // Act
        string? beforeRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string? afterRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.NotNull(beforeRefresh);
        Assert.NotNull(afterRefresh);
        Assert.Equal("name", beforeRefresh);
        Assert.Equal("name.keyword", afterRefresh);
        Assert.True(serverFetchCount >= 2, "Server mapping should have been fetched at least twice");
    }

    [Fact]
    public void RefreshMapping_ClearsFoundCacheEntries_ForcesReResolution()
    {
        // Arrange
        int serverFetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            int callNumber = Interlocked.Increment(ref serverFetchCount);
            return callNumber == 1
                ? CreateTextOnlyMapping("name")
                : CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        // Act
        string? first = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string? second = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal("name", first);
        Assert.Equal("name.keyword", second);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithCodeAndServerMerge_ReturnsKeywordSubField()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("name"), _inferrer,
            () => CreateTextOnlyMapping("name"), logger: _logger);
        resolver.RefreshMapping();

        // Act
        string? result = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.NotNull(result);
        Assert.Equal("name.keyword", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_AfterRefreshAndServerMappingChange_ReturnsUpdatedKeywordPath()
    {
        // Arrange
        int callCount = 0;
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("name"), _inferrer, () =>
            {
                int callNumber = Interlocked.Increment(ref callCount);
                return callNumber <= 1 ? null : CreateTextWithKeywordMapping("name");
            }, logger: _logger);

        // Act
        string? initial = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string? updated = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.NotNull(initial);
        Assert.NotNull(updated);
        Assert.Equal("name", initial);
        Assert.Equal("name.keyword", updated);
    }

    [Fact]
    public async Task ConcurrentGetMappingAndRefreshMapping_UnderContention_AlwaysReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("name"), _inferrer, () =>
            {
                Thread.Yield();
                return CreateTextWithKeywordMapping("name");
            }, logger: _logger);
        const int iterations = 200;
        using var barrier = new Barrier(3);

        // Act
        var readerTask = Task.Run(() =>
        {
            barrier.SignalAndWait(TestCancellationToken);
            for (int i = 0; i < iterations; i++)
            {
                string? result = resolver.GetNonAnalyzedFieldName("name", "keyword");
                Assert.NotNull(result);
                Assert.Equal("name.keyword", result);
            }
        }, TestCancellationToken);

        var aggregationReaderTask = Task.Run(() =>
        {
            barrier.SignalAndWait(TestCancellationToken);
            for (int i = 0; i < iterations; i++)
            {
                string? result = resolver.GetAggregationsFieldName("name");
                Assert.NotNull(result);
                Assert.Equal("name.keyword", result);
            }
        }, TestCancellationToken);

        var refreshTask = Task.Run(() =>
        {
            barrier.SignalAndWait(TestCancellationToken);
            for (int i = 0; i < iterations; i++)
            {
                resolver.RefreshMapping();
                Thread.Yield();
            }
        }, TestCancellationToken);

        // Assert
        await Task.WhenAll(readerTask, aggregationReaderTask, refreshTask);
    }

    [Fact]
    public void GetServerMapping_WhenThrottled_DoesNotRefetchWithinOneMinute()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        // Act - first call triggers initial server fetch
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterFirst = fetchCount;
        Assert.True(afterFirst >= 1, "First call should trigger at least one fetch");

        // Act - second call within throttle window should use cache, no new fetch
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal(afterFirst, fetchCount);

        // Act - advance 30s (still within 1-minute window), query unknown field to bypass cache
        timeProvider.Advance(TimeSpan.FromSeconds(30));
        resolver.GetNonAnalyzedFieldName("unknown_field", "keyword");
        Assert.Equal(afterFirst, fetchCount);

        // Act - RefreshMapping bypasses throttle even within window
        resolver.RefreshMapping();
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterRefresh = fetchCount;
        Assert.True(afterRefresh > afterFirst, "Refresh should allow a new fetch");

        // Act - call again without refresh, should be throttled
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal(afterRefresh, fetchCount);

        // Act - advance past the 1-minute throttle window; resolve an uncached field
        // WITHOUT calling RefreshMapping() to prove time-based expiry works on its own.
        timeProvider.Advance(TimeSpan.FromMinutes(2));
        resolver.GetNonAnalyzedFieldName("another_unknown_field", "keyword");
        int afterTimeAdvance = fetchCount;
        Assert.True(afterTimeAdvance > afterRefresh, "Fetch should happen after time advances past throttle without RefreshMapping");
    }

    private static TypeMapping CreateTextWithKeywordMapping(string fieldName)
    {
        var subFields = new Properties();
        subFields.Add("keyword", new KeywordProperty { IgnoreAbove = 256 });

        var props = new Properties();
        props.Add(fieldName, new TextProperty { Fields = subFields });

        return new TypeMapping { Properties = props };
    }

    private static TypeMapping CreateTextWithKeywordAndSortMapping(string fieldName)
    {
        var subFields = new Properties();
        subFields.Add("keyword", new KeywordProperty { IgnoreAbove = 256 });
        subFields.Add("sort", new KeywordProperty { IgnoreAbove = 256 });

        var props = new Properties();
        props.Add(fieldName, new TextProperty { Fields = subFields });

        return new TypeMapping { Properties = props };
    }

    private static TypeMapping CreateTextOnlyMapping(string fieldName)
    {
        var props = new Properties();
        props.Add(fieldName, new TextProperty());

        return new TypeMapping { Properties = props };
    }

    [Fact]
    public void GetFieldType_WithUnsignedLongProperty_ReturnsLong()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "counter", new UnsignedLongNumberProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var result = resolver.GetFieldType("counter");

        Assert.Equal(FieldType.Long, result);
    }

    [Fact]
    public void GetFieldType_WithDateNanosProperty_ReturnsDateNanos()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "timestamp", new DateNanosProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var result = resolver.GetFieldType("timestamp");

        Assert.Equal(FieldType.DateNanos, result);
    }

    [Fact]
    public void GetFieldType_WithSearchAsYouTypeProperty_ReturnsSearchAsYouType()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "suggest", new SearchAsYouTypeProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var result = resolver.GetFieldType("suggest");

        Assert.Equal(FieldType.SearchAsYouType, result);
    }

    [Fact]
    public void GetFieldType_WithConstantKeywordProperty_ReturnsConstantKeyword()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "tenant", new ConstantKeywordProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var result = resolver.GetFieldType("tenant");

        Assert.Equal(FieldType.ConstantKeyword, result);
    }

    [Fact]
    public void GetFieldType_WithFlattenedProperty_ReturnsFlattened()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "labels", new FlattenedProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var result = resolver.GetFieldType("labels");

        Assert.Equal(FieldType.Flattened, result);
    }

    [Fact]
    public void GetFieldType_WithJoinProperty_ReturnsJoin()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "relation", new JoinProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var result = resolver.GetFieldType("relation");

        Assert.Equal(FieldType.Join, result);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMultiLevelNesting_UsesDeepestNestedPath()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync("parent.child.name:test",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNestedFilter_AppliesFilterPerChild()
    {
        var nestedChildProps = new Properties
        {
            { "status", new KeywordProperty() },
            { "priority", new KeywordProperty() },
            { "visible", new BooleanProperty() }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Properties = nestedChildProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        int filterCallCount = 0;
        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, orig, resolved, ctx) =>
            {
                Interlocked.Increment(ref filterCallCount);
                return path is "items"
                    ? (Query)new TermQuery("items.visible", true)
                    : null;
            }));

        var query = await parser.BuildQueryAsync("items.status:active AND items.priority:high",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        Assert.True(filterCallCount >= 2, $"Filter should be called per child, got {filterCallCount} calls");

        string json = SerializeQuery(query);
        Assert.Contains("items.visible", json);
        Assert.Contains("\"path\":\"items\"", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithOrQueryAndDistinctFilters_PreservesPerChildFilters()
    {
        var nestedChildProps = new Properties
        {
            { "status", new KeywordProperty() },
            { "priority", new KeywordProperty() },
            { "status_filter", new KeywordProperty() },
            { "priority_filter", new KeywordProperty() }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Properties = nestedChildProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, orig, resolved, ctx) =>
            {
                if (path is not "items")
                    return null;

                return resolved switch
                {
                    "items.status" => (Query)new TermQuery("items.status_filter", "A"),
                    "items.priority" => (Query)new TermQuery("items.priority_filter", "B"),
                    _ => null
                };
            }));

        var query = await parser.BuildQueryAsync("items.status:active OR items.priority:high",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("items.status_filter", json);
        Assert.Contains("items.priority_filter", json);
        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("should", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMixedNestedLevels_ProducesCorrelatedNestedChain()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync("parent.name:Bob AND parent.child.name:Alice",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNegatedChildInMultiLevel_ProducesCorrelatedNegation()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync("parent.name:Bob AND NOT parent.child.name:Alice",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("must_not", json);
        Assert.Contains("Bob", json);
        Assert.Contains("Alice", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithSiblingNestedPaths_FoldsIntoSharedParent()
    {
        var childAProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childBProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty
                {
                    Properties = new Properties
                    {
                        { "childA", new NestedProperty { Properties = childAProps } },
                        { "childB", new NestedProperty { Properties = childBProps } }
                    }
                }
            }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync("parent.childA.name:X AND parent.childB.name:Y",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.childA\"", json);
        Assert.Contains("\"path\":\"parent.childB\"", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNegatedNestedField_WrapsMustNotOutsideNestedQuery()
    {
        var itemProps = new Properties
        {
            { "status", new KeywordProperty() }
        };
        var rootProps = new Properties
        {
            { "title", new KeywordProperty() },
            { "items", new NestedProperty { Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync("title:Hello AND NOT items.status:archived",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("must_not", json);
        Assert.Contains("items.status", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNegatedNestedFieldAndFilter_AppliesFilterBeforeNegating()
    {
        var itemProps = new Properties
        {
            { "status", new KeywordProperty() },
            { "visible", new BooleanProperty() }
        };
        var rootProps = new Properties
        {
            { "title", new KeywordProperty() },
            { "items", new NestedProperty { Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "items" ? (Query)new TermQuery("items.visible", true) : null));

        var query = await parser.BuildQueryAsync("title:Hello AND NOT items.status:archived",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("must_not", json);
        Assert.Contains("items.status", json);
        Assert.Contains("items.visible", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithOrGroupMixedLevels_PreservesBranchBoundaries()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync(
            "(parent.name:Bob AND parent.child.name:Alice) OR (parent.name:Sue AND parent.child.name:Charlie)",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("Bob", json);
        Assert.Contains("Alice", json);
        Assert.Contains("Sue", json);
        Assert.Contains("Charlie", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithExplicitNestedGroupAndDeeperChild_WrapsChildInNestedQuery()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var query = await parser.BuildQueryAsync("parent:(parent.child.name:Alice)",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("Alice", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithDefaultFieldNestedAndFilter_AppliesNestedWrapperWithFilter()
    {
        var itemProps = new Properties
        {
            { "status", new KeywordProperty() },
            { "visible", new BooleanProperty() }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .SetDefaultFields(["items.status"])
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "items" ? (Query)new TermQuery("items.visible", true) : null));

        var query = await parser.BuildQueryAsync("active",
            new ElasticQueryVisitorContext { UseScoring = true });

        Assert.NotNull(query);
        string json = SerializeQuery(query);

        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("items.status", json);
        Assert.Contains("items.visible", json);
        Assert.Contains("\"filter\"", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMultipleDefaultFieldsAndDistinctFilters_AppliesPerFieldFilter()
    {
        var itemProps = new Properties
        {
            { "status", new KeywordProperty() },
            { "priority", new KeywordProperty() }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .SetDefaultFields(["items.status", "items.priority"])
            .UseNestedFilter((path, field, originalField, ctx) =>
            {
                if (field == "items.status")
                    return Task.FromResult<Query?>(new TermQuery("items.type", "status_filter"));
                if (field == "items.priority")
                    return Task.FromResult<Query?>(new TermQuery("items.type", "priority_filter"));
                return Task.FromResult<Query?>(null);
            }));

        var result = await parser.BuildQueryAsync("active");

        Assert.NotNull(result);
        string json = SerializeQuery(result);

        Assert.Contains("nested", json);
        Assert.Contains("status_filter", json);
        Assert.Contains("priority_filter", json);
        Assert.Contains("items.status", json);
        Assert.Contains("items.priority", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithExplicitNestedGroupAndNegatedDeeperChild_ProducesCorrelatedNegation()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var result = await parser.BuildQueryAsync("parent:(parent.name:Bob AND NOT parent.child.name:Alice)");

        Assert.NotNull(result);
        string json = SerializeQuery(result);

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("Bob", json);
        Assert.Contains("Alice", json);
        Assert.Contains("must_not", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    private string SerializeQuery(Query query)
    {
        var client = new ElasticsearchClient(_clientSettings);
        using var stream = new System.IO.MemoryStream();
        client.RequestResponseSerializer.Serialize(query, stream);
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private string Serialize<T>(T value)
    {
        var client = new ElasticsearchClient(_clientSettings);
        using var stream = new System.IO.MemoryStream();
        client.RequestResponseSerializer.Serialize(value, stream);
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    [Fact]
    public async Task BuildSortAsync_WithMultiLevelNestedField_ProducesHierarchicalNestedSort()
    {
        var grandchildProps = new Properties
        {
            { "score", new IntegerNumberProperty() }
        };
        var childProps = new Properties
        {
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var sorts = await parser.BuildSortAsync("-parent.child.score");

        Assert.NotNull(sorts);
        var sortList = sorts.ToList();
        Assert.Single(sortList);

        string json = Serialize(sortList);
        Assert.Contains("parent.child.score", json);
        Assert.Contains("desc", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildSortAsync_WithUnsignedLongField_UsesLongUnmappedType()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "counter", new UnsignedLongNumberProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c.UseMappings(resolver));

        var sorts = await parser.BuildSortAsync("-counter");

        Assert.NotNull(sorts);
        var sortList = sorts.ToList();
        Assert.Single(sortList);

        string json = Serialize(sortList);
        Assert.Contains("counter", json);
        Assert.Contains("desc", json);
        Assert.Contains("long", json);
    }

    [Fact]
    public async Task BuildSortAsync_WithMultiLevelNestedFieldAndFilter_AppliesFilterOnInnermost()
    {
        var grandchildProps = new Properties
        {
            { "score", new IntegerNumberProperty() }
        };
        var childProps = new Properties
        {
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, field, originalField, ctx) =>
                Task.FromResult<Query?>((Query)new TermQuery($"{path}.active", true))));

        var sorts = await parser.BuildSortAsync("-parent.child.score");

        Assert.NotNull(sorts);
        var sortList = sorts.ToList();
        Assert.Single(sortList);

        string json = Serialize(sortList);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("\"filter\"", json);
    }

    [Fact]
    public async Task BuildAggregationsAsync_WithMultiLevelNestedField_ProducesHierarchicalNestedAggregation()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var aggs = await parser.BuildAggregationsAsync("terms:parent.child.name");

        Assert.NotNull(aggs);
        string json = Serialize(aggs);

        Assert.Contains("nested_parent", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("nested_parent.child", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildAggregationsAsync_WithParentAndChildLevelAggs_PreservesBothUnderSameWrapper()
    {
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        var aggs = await parser.BuildAggregationsAsync("terms:parent.name terms:parent.child.name");

        Assert.NotNull(aggs);
        string json = Serialize(aggs);

        Assert.Contains("nested_parent", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("nested_parent.child", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("terms_parent.name", json);
        Assert.Contains("terms_parent.child.name", json);
    }

    [Fact]
    public async Task BuildAggregationsAsync_WithFilteredParentAndChildAggs_DoesNotOverwrite()
    {
        var grandchildProps = new Properties
        {
            { "status", new KeywordProperty() }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty() },
            { "child", new NestedProperty { Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, field, originalField, ctx) =>
                Task.FromResult<Query?>((Query)new TermQuery($"{path}.active", true))));

        var aggs = await parser.BuildAggregationsAsync("terms:parent.name terms:parent.child.status");

        Assert.NotNull(aggs);
        string json = Serialize(aggs);

        Assert.Contains("terms_parent.name", json);
        Assert.Contains("terms_parent.child.status", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithUnsignedLongValueExceedingInt64Max_PreservesAsString()
    {
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "counter", new UnsignedLongNumberProperty() }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c.UseMappings(resolver));

        var query = await parser.BuildQueryAsync("counter:18446744073709551615");

        Assert.NotNull(query);
        string json = SerializeQuery(query);
        Assert.Contains("counter", json);
        Assert.Contains("18446744073709551615", json);
    }
}
