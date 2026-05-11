using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Time.Testing;
using Nest;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticMappingResolverUnitTests : TestWithLoggingBase, IDisposable
{
    private readonly ConnectionSettings _connectionSettings;
    private readonly Inferrer _inferrer;

    public ElasticMappingResolverUnitTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        _connectionSettings = new ConnectionSettings(new Uri("http://localhost:9200"));
        _inferrer = new Inferrer(_connectionSettings);
    }

    public void Dispose()
    {
        (_connectionSettings as IDisposable)?.Dispose();
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), _inferrer, () => null, logger: _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("title", "keyword")!;

        // Assert
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetAggregationsFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), _inferrer, () => null, logger: _logger);

        // Act
        string result = resolver.GetAggregationsFieldName("title")!;

        // Assert
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetSortFieldName_WithTextPropertyAndSortSubField_ReturnsSortPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordAndSortMapping("title"), _inferrer, () => null, logger: _logger);

        // Act
        string result = resolver.GetSortFieldName("title")!;

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
        var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => null, logger: _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("status", "keyword")!;

        // Assert
        Assert.Equal("status", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyWithoutSubFields_ReturnsBareFieldName()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("body"), _inferrer, () => null, logger: _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("body", "keyword")!;

        // Assert
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
        string beforeRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword")!;
        resolver.RefreshMapping();
        string afterRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword")!;

        // Assert
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
        string first = resolver.GetNonAnalyzedFieldName("name", "keyword")!;
        resolver.RefreshMapping();
        string second = resolver.GetNonAnalyzedFieldName("name", "keyword")!;

        // Assert
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
        string result = resolver.GetNonAnalyzedFieldName("name", "keyword")!;

        // Assert
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
        string initial = resolver.GetNonAnalyzedFieldName("name", "keyword")!;
        resolver.RefreshMapping();
        string updated = resolver.GetNonAnalyzedFieldName("name", "keyword")!;

        // Assert
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
                string result = resolver.GetNonAnalyzedFieldName("name", "keyword")!;
                Assert.Equal("name.keyword", result);
            }
        }, TestCancellationToken);

        var aggregationReaderTask = Task.Run(() =>
        {
            barrier.SignalAndWait(TestCancellationToken);
            for (int i = 0; i < iterations; i++)
            {
                string result = resolver.GetAggregationsFieldName("name")!;
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

    private static ITypeMapping CreateTextOnlyMapping(string fieldName)
    {
        return new TypeMapping
        {
            Properties = new Properties
            {
                { fieldName, new TextProperty { Name = fieldName } }
            }
        };
    }

    [Fact]
    public void GetFieldType_WithUnsignedLongProperty_ReturnsLong()
    {
        // Arrange - NumberProperty with UnsignedLong type
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "counter", new NumberProperty(NumberType.UnsignedLong) { Name = "counter" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Act
        var result = resolver.GetFieldType("counter");

        // Assert
        Assert.Equal(FieldType.Long, result);
    }

    [Fact]
    public void GetFieldType_WithDateNanosProperty_ReturnsDate()
    {
        // Arrange
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "timestamp", new DateNanosProperty { Name = "timestamp" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Act
        var result = resolver.GetFieldType("timestamp");

        // Assert
        Assert.Equal(FieldType.Date, result);
    }

    [Fact]
    public void GetFieldType_WithSearchAsYouTypeProperty_ReturnsSearchAsYouType()
    {
        // Arrange
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "suggest", new SearchAsYouTypeProperty { Name = "suggest" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Act
        var result = resolver.GetFieldType("suggest");

        // Assert
        Assert.Equal(FieldType.SearchAsYouType, result);
    }

    [Fact]
    public void GetFieldType_WithConstantKeywordProperty_ReturnsKeyword()
    {
        // Arrange
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "tenant", new ConstantKeywordProperty { Name = "tenant" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Act
        var result = resolver.GetFieldType("tenant");

        // Assert
        Assert.Equal(FieldType.Keyword, result);
    }

    [Fact]
    public void GetFieldType_WithFlattenedProperty_ReturnsFlattened()
    {
        // Arrange
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "labels", new FlattenedProperty { Name = "labels" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Act
        var result = resolver.GetFieldType("labels");

        // Assert
        Assert.Equal(FieldType.Flattened, result);
    }

    [Fact]
    public void GetFieldType_WithJoinProperty_ReturnsJoin()
    {
        // Arrange
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "relation", new JoinProperty { Name = "relation" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Act
        var result = resolver.GetFieldType("relation");

        // Assert
        Assert.Equal(FieldType.Join, result);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMultiLevelNesting_UsesDeepestNestedPath()
    {
        // Arrange
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty { Name = "name" } }
        };
        var childProps = new Properties
        {
            { "child", new NestedProperty { Name = "child", Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Name = "parent", Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        // Act
        var query = await parser.BuildQueryAsync("parent.child.name:test",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — query should be nested at parent.child (deepest), not parent (shallowest)
        Assert.NotNull(query);
        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("parent.child", container.Nested.Path);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNestedFilter_AppliesFilterPerChild()
    {
        // Arrange
        var nestedChildProps = new Properties
        {
            { "status", new KeywordProperty { Name = "status" } },
            { "priority", new KeywordProperty { Name = "priority" } },
            { "visible", new BooleanProperty { Name = "visible" } }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Name = "items", Properties = nestedChildProps } }
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
                    ? new TermQuery { Field = "items.visible", Value = true }
                    : null;
            }));

        // Act
        var query = await parser.BuildQueryAsync("items.status:active AND items.priority:high",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — filter should be called per child and the generated query should contain nested with bool must
        Assert.NotNull(query);
        Assert.True(filterCallCount >= 2, $"Filter should be called per child, got {filterCallCount} calls");

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("items", container.Nested.Path);
        Assert.NotNull(container.Nested.Query);

        var innerContainer = Assert.IsAssignableFrom<IQueryContainer>(container.Nested.Query);
        Assert.NotNull(innerContainer.Bool);
        Assert.NotNull(innerContainer.Bool.Must);

        var mustClauses = innerContainer.Bool.Must.ToList();
        Assert.True(mustClauses.Count >= 2, $"Expected at least 2 must clauses, got {mustClauses.Count}");

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());
        Assert.Contains("items.visible", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithOrQueryAndDistinctFilters_PreservesPerChildFilters()
    {
        // Arrange
        var nestedChildProps = new Properties
        {
            { "status", new KeywordProperty { Name = "status" } },
            { "priority", new KeywordProperty { Name = "priority" } },
            { "status_filter", new KeywordProperty { Name = "status_filter" } },
            { "priority_filter", new KeywordProperty { Name = "priority_filter" } }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Name = "items", Properties = nestedChildProps } }
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
                    "items.status" => new TermQuery { Field = "items.status_filter", Value = "A" },
                    "items.priority" => new TermQuery { Field = "items.priority_filter", Value = "B" },
                    _ => null
                };
            }));

        // Act
        var query = await parser.BuildQueryAsync("items.status:active OR items.priority:high",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — each branch must have its own distinct filter: (status AND filter_A) OR (priority AND filter_B)
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("items.status_filter", json);
        Assert.Contains("items.priority_filter", json);

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("items", container.Nested.Path);

        var innerContainer = Assert.IsAssignableFrom<IQueryContainer>(container.Nested.Query);
        Assert.NotNull(innerContainer.Bool);
        Assert.NotNull(innerContainer.Bool.Should);
        var shouldClauses = innerContainer.Bool.Should.ToList();
        Assert.Equal(2, shouldClauses.Count);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMixedNestedLevels_DocumentsCurrentNonCorrelatedLimitation()
    {
        // Arrange — parent and parent.child are both nested types
        var grandchildProps = new Properties
        {
            { "name", new KeywordProperty { Name = "name" } }
        };
        var childProps = new Properties
        {
            { "name", new KeywordProperty { Name = "name" } },
            { "child", new NestedProperty { Name = "child", Properties = grandchildProps } }
        };
        var rootProps = new Properties
        {
            { "parent", new NestedProperty { Name = "parent", Properties = childProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        // Act — query mixing parent-level and child-level nested fields
        var query = await parser.BuildQueryAsync("parent.name:Bob AND parent.child.name:Alice",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — KNOWN LIMITATION: produces two independent nested queries instead of
        // the correct correlated structure: nested(path=parent, query=name:Bob AND nested(path=parent.child, query=...))
        // See: https://github.com/FoundatioFx/Foundatio.Parsers/issues/XXX
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Bool);
        Assert.NotNull(container.Bool.Must);
        var mustClauses = container.Bool.Must.ToList();
        Assert.Equal(2, mustClauses.Count);
    }

    [Fact]
    public async Task BuildQueryAsync_WithUnsignedLongValueExceedingInt64Max_PreservesAsString()
    {
        // Arrange
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "counter", new NumberProperty(NumberType.UnsignedLong) { Name = "counter" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c.UseMappings(resolver));

        const string maxUnsignedLong = "18446744073709551615";

        // Act — value exceeds Int64.MaxValue, should not throw
        var query = await parser.BuildQueryAsync($"counter:{maxUnsignedLong}",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — value is preserved as the exact string (Int64.TryParse fails, falls through to string)
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains(maxUnsignedLong, json);
    }
}
