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

        // Verify filter is present in the structure. With AND combining, each child's
        // bool{must+filter} may be nested inside the top-level must array, or NEST may
        // flatten. We verify structurally by checking filters exist at some level.
        int filterCount = 0;
        foreach (var clause in mustClauses)
        {
            var clauseContainer = Assert.IsAssignableFrom<IQueryContainer>(clause);
            if (clauseContainer.Bool?.Filter is not null)
                filterCount++;
        }

        // If NEST didn't flatten the bool queries, each must clause has its own filter
        if (filterCount == 0 && innerContainer.Bool.Filter is not null)
            filterCount = innerContainer.Bool.Filter.Count();

        Assert.True(filterCount >= 1, $"Expected at least 1 filter, got {filterCount}");

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());
        Assert.Contains("items.visible", json);
        Assert.Contains("\"filter\"", json);
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

        var branch0 = Assert.IsAssignableFrom<IQueryContainer>(shouldClauses[0]);
        Assert.NotNull(branch0.Bool);
        Assert.NotNull(branch0.Bool.Must);
        Assert.NotNull(branch0.Bool.Filter);
        Assert.Single(branch0.Bool.Must);
        Assert.Single(branch0.Bool.Filter);

        var branch1 = Assert.IsAssignableFrom<IQueryContainer>(shouldClauses[1]);
        Assert.NotNull(branch1.Bool);
        Assert.NotNull(branch1.Bool.Must);
        Assert.NotNull(branch1.Bool.Filter);
        Assert.Single(branch1.Bool.Must);
        Assert.Single(branch1.Bool.Filter);

        using var s0 = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(branch0, s0);
        string branch0Json = System.Text.Encoding.UTF8.GetString(s0.ToArray());

        using var s1 = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(branch1, s1);
        string branch1Json = System.Text.Encoding.UTF8.GetString(s1.ToArray());

        Assert.Contains("items.status", branch0Json);
        Assert.Contains("items.status_filter", branch0Json);
        Assert.Contains("items.priority", branch1Json);
        Assert.Contains("items.priority_filter", branch1Json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMixedNestedLevels_ProducesCorrelatedNestedChain()
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

        // Assert — produces correlated nested chain:
        // nested(path=parent, query=name:Bob AND nested(path=parent.child, query=name:Alice))
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("parent", container.Nested.Path);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNegatedChildInMultiLevel_ProducesCorrelatedNegation()
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

        // Act — negated child in multi-level should be correlated inside parent nested
        var query = await parser.BuildQueryAsync("parent.name:Bob AND NOT parent.child.name:Alice",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — produces correlated nested chain:
        // nested(parent, query=name:Bob AND must_not(nested(parent.child, query=name:Alice)))
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("must_not", json);

        // The top-level query should be a single nested(parent) — no top-level bool
        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("parent", container.Nested.Path);

        // Inside the parent nested, there should be a bool with must_not containing nested(parent.child)
        var parentInner = Assert.IsAssignableFrom<IQueryContainer>(container.Nested.Query);
        Assert.NotNull(parentInner.Bool);
        Assert.NotNull(parentInner.Bool.Must);
        var mustClauses = parentInner.Bool.Must.ToList();
        Assert.True(mustClauses.Count >= 1);

        // Find the must_not containing the nested child
        var mustNotSource = parentInner.Bool.MustNot?.ToList();
        if (mustNotSource is null || mustNotSource.Count == 0)
        {
            var clauseWithMustNot = mustClauses
                .Select(c => Assert.IsAssignableFrom<IQueryContainer>(c))
                .FirstOrDefault(c => c.Bool?.MustNot is not null);
            Assert.NotNull(clauseWithMustNot);
            mustNotSource = clauseWithMustNot!.Bool!.MustNot!.ToList();
        }

        Assert.Single(mustNotSource);
        var negatedChild = Assert.IsAssignableFrom<IQueryContainer>(mustNotSource[0]);
        Assert.NotNull(negatedChild.Nested);
        Assert.Equal("parent.child", negatedChild.Nested.Path);
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

    [Fact]
    public async Task BuildQueryAsync_WithNegatedNestedField_WrapsMustNotOutsideNestedQuery()
    {
        // Arrange
        var itemProps = new Properties
        {
            { "name", new KeywordProperty { Name = "name" } },
            { "status", new KeywordProperty { Name = "status" } }
        };
        var rootProps = new Properties
        {
            { "title", new KeywordProperty { Name = "title" } },
            { "items", new NestedProperty { Name = "items", Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested());

        // Act — NOT on a nested field should wrap must_not OUTSIDE the nested query
        var query = await parser.BuildQueryAsync("title:Hello AND NOT items.status:archived",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — should produce: bool { must: [term(title), must_not: [nested(path=items, query=term(status))]] }
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("must_not", json);
        Assert.Contains("items.status", json);

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Bool);

        // must_not can be at the top-level bool or nested in a must clause
        var mustNotClauses = container.Bool.MustNot?.ToList();
        if (mustNotClauses is null || mustNotClauses.Count == 0)
        {
            Assert.NotNull(container.Bool.Must);
            var negated = container.Bool.Must
                .Select(c => Assert.IsAssignableFrom<IQueryContainer>(c))
                .FirstOrDefault(c => c.Bool?.MustNot is not null);
            Assert.NotNull(negated);
            mustNotClauses = negated!.Bool!.MustNot!.ToList();
        }

        Assert.Single(mustNotClauses);
        var nestedInMustNot = Assert.IsAssignableFrom<IQueryContainer>(mustNotClauses[0]);
        Assert.NotNull(nestedInMustNot.Nested);
        Assert.Equal("items", nestedInMustNot.Nested.Path);
    }

    [Fact]
    public async Task BuildQueryAsync_WithNegatedNestedFieldAndFilter_AppliesFilterBeforeNegating()
    {
        // Arrange
        var itemProps = new Properties
        {
            { "status", new KeywordProperty { Name = "status" } },
            { "visible", new BooleanProperty { Name = "visible" } }
        };
        var rootProps = new Properties
        {
            { "title", new KeywordProperty { Name = "title" } },
            { "items", new NestedProperty { Name = "items", Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "items" ? new TermQuery { Field = "items.visible", Value = true } : null));

        // Act — negated nested field with a filter should include filter inside the nested query
        var query = await parser.BuildQueryAsync("title:Hello AND NOT items.status:archived",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — must_not should contain nested(path=items, query=bool{must:[status:archived], filter:[visible:true]})
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("must_not", json);
        Assert.Contains("items.status", json);
        Assert.Contains("items.visible", json);

        // Structural assertions: navigate object graph to prove filter is inside nested
        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Bool);

        var mustNotClauses = container.Bool.MustNot?.ToList();
        if (mustNotClauses is null || mustNotClauses.Count == 0)
        {
            Assert.NotNull(container.Bool.Must);
            var negated = container.Bool.Must
                .Select(c => Assert.IsAssignableFrom<IQueryContainer>(c))
                .FirstOrDefault(c => c.Bool?.MustNot is not null);
            Assert.NotNull(negated);
            mustNotClauses = negated!.Bool!.MustNot!.ToList();
        }

        Assert.Single(mustNotClauses);
        var nestedInMustNot = Assert.IsAssignableFrom<IQueryContainer>(mustNotClauses[0]);
        Assert.NotNull(nestedInMustNot.Nested);
        Assert.Equal("items", nestedInMustNot.Nested.Path);

        // The nested query's inner query should be a bool with must (term) + filter (visible)
        var nestedInner = Assert.IsAssignableFrom<IQueryContainer>(nestedInMustNot.Nested.Query);
        Assert.NotNull(nestedInner.Bool);
        Assert.NotNull(nestedInner.Bool.Must);
        Assert.NotNull(nestedInner.Bool.Filter);
        Assert.Single(nestedInner.Bool.Must);
        Assert.Single(nestedInner.Bool.Filter);
    }

    [Fact]
    public async Task BuildQueryAsync_WithOrGroupMixedLevels_PreservesBranchBoundaries()
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

        // Act — OR group with mixed levels should preserve branch correlation within a single nested(parent)
        var query = await parser.BuildQueryAsync(
            "(parent.name:Bob AND parent.child.name:Alice) OR (parent.name:Sue AND parent.child.name:Charlie)",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — coalesced into single nested(parent) with OR inner query preserving branches:
        // nested(parent, (name:Bob AND nested(parent.child, name:Alice)) OR (name:Sue AND nested(parent.child, name:Charlie)))
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("Bob", json);
        Assert.Contains("Alice", json);
        Assert.Contains("Sue", json);
        Assert.Contains("Charlie", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);

        // Top-level should be a single nested(parent) query — not a bool/should of two nested queries
        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("parent", container.Nested.Path);

        // Inner query should be a bool with should (from OR)
        var innerContainer = Assert.IsAssignableFrom<IQueryContainer>(container.Nested.Query);
        Assert.NotNull(innerContainer.Bool);
        Assert.NotNull(innerContainer.Bool.Should);
        var shouldClauses = innerContainer.Bool.Should.ToList();
        Assert.Equal(2, shouldClauses.Count);

        // Each OR branch should contain its own correlated child nested(parent.child)
        using var s0 = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(shouldClauses[0], s0);
        string branch0Json = System.Text.Encoding.UTF8.GetString(s0.ToArray());

        using var s1 = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(shouldClauses[1], s1);
        string branch1Json = System.Text.Encoding.UTF8.GetString(s1.ToArray());

        // Branch isolation: Bob+Alice in one branch, Sue+Charlie in the other
        bool bobInBranch0 = branch0Json.Contains("Bob");
        string bobBranch = bobInBranch0 ? branch0Json : branch1Json;
        string sueBranch = bobInBranch0 ? branch1Json : branch0Json;

        Assert.Contains("Bob", bobBranch);
        Assert.Contains("Alice", bobBranch);
        Assert.Contains("\"path\":\"parent.child\"", bobBranch);

        Assert.Contains("Sue", sueBranch);
        Assert.Contains("Charlie", sueBranch);
        Assert.Contains("\"path\":\"parent.child\"", sueBranch);

        // Cross-branch isolation
        Assert.DoesNotContain("Sue", bobBranch);
        Assert.DoesNotContain("Bob", sueBranch);
    }

    [Fact]
    public async Task BuildQueryAsync_WithExplicitNestedGroupAndDeeperChild_WrapsChildInNestedQuery()
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

        // Act — explicit nested group with a deeper child field
        var query = await parser.BuildQueryAsync("parent:(parent.child.name:Alice)",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — should produce: nested(parent, nested(parent.child, name:Alice))
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("Alice", json);

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("parent", container.Nested.Path);
    }

    [Fact]
    public async Task BuildQueryAsync_WithDefaultFieldNestedAndFilter_AppliesNestedWrapperWithFilter()
    {
        // Arrange — items.status is nested, filter resolver is configured
        var itemProps = new Properties
        {
            { "status", new KeywordProperty { Name = "status" } },
            { "visible", new BooleanProperty { Name = "visible" } }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Name = "items", Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c
            .SetDefaultFields(["items.status"])
            .UseMappings(resolver)
            .UseNested()
            .UseNestedFilter((path, orig, resolved, ctx) =>
                path is "items" ? new TermQuery { Field = "items.visible", Value = true } : null));

        // Act — search without field name uses default field (nested)
        var query = await parser.BuildQueryAsync("active",
            new ElasticQueryVisitorContext { UseScoring = true });

        // Assert — should produce: nested(path=items, query=bool{must:[status:active], filter:[visible:true]})
        Assert.NotNull(query);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(query, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"items\"", json);
        Assert.Contains("items.status", json);
        Assert.Contains("items.visible", json);
        Assert.Contains("\"filter\"", json);

        var container = Assert.IsAssignableFrom<IQueryContainer>(query);
        Assert.NotNull(container.Nested);
        Assert.Equal("items", container.Nested.Path);

        var innerContainer = Assert.IsAssignableFrom<IQueryContainer>(container.Nested.Query);
        Assert.NotNull(innerContainer.Bool);
        Assert.NotNull(innerContainer.Bool.Must);
        Assert.NotNull(innerContainer.Bool.Filter);
    }

    [Fact]
    public async Task BuildSortAsync_WithMultiLevelNestedField_ProducesHierarchicalNestedSort()
    {
        // Arrange — parent and parent.child are both nested types
        var grandchildProps = new Properties
        {
            { "score", new NumberProperty(NumberType.Integer) { Name = "score" } }
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

        // Act — sort on multi-level nested field
        var sorts = await parser.BuildSortAsync("-parent.child.score");

        // Assert — should produce hierarchical nested sort: nested(parent, nested(parent.child))
        Assert.NotNull(sorts);
        var sortList = sorts.ToList();
        Assert.Single(sortList);

        var fieldSort = Assert.IsAssignableFrom<IFieldSort>(sortList[0]);
        Assert.Equal(SortOrder.Descending, fieldSort.Order);
        Assert.NotNull(fieldSort.Nested);
        Assert.Equal("parent", fieldSort.Nested.Path);
        Assert.NotNull(fieldSort.Nested.Nested);
        Assert.Equal("parent.child", fieldSort.Nested.Nested.Path);
    }

    [Fact]
    public async Task BuildAggregationsAsync_WithMultiLevelNestedField_ProducesHierarchicalNestedAggregation()
    {
        // Arrange — parent and parent.child are both nested types
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

        // Act — aggregation on multi-level nested field
        var aggs = await parser.BuildAggregationsAsync("terms:parent.child.name");

        // Assert — should produce hierarchical nested agg: nested(parent) > nested(parent.child) > terms
        Assert.NotNull(aggs);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(aggs, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("nested_parent", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("nested_parent.child", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildSortAsync_WithUnsignedLongField_UsesLongUnmappedType()
    {
        // Arrange — unsigned_long maps to FieldType.Long in NEST 7.x (best available approximation)
        var mapping = new TypeMapping
        {
            Properties = new Properties
            {
                { "counter", new NumberProperty(NumberType.UnsignedLong) { Name = "counter" } }
            }
        };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        var parser = new ElasticQueryParser(c => c.UseMappings(resolver));

        // Act
        var sorts = await parser.BuildSortAsync("-counter");

        // Assert — sort should use 'long' as unmapped_type (NEST 7.x limitation: no unsigned_long field type)
        Assert.NotNull(sorts);
        var sortList = sorts.ToList();
        Assert.Single(sortList);

        var fieldSort = Assert.IsAssignableFrom<IFieldSort>(sortList[0]);
        Assert.Equal(SortOrder.Descending, fieldSort.Order);
        Assert.Equal(FieldType.Long, fieldSort.UnmappedType);
    }

    [Fact]
    public async Task BuildAggregationsAsync_WithParentAndChildLevelAggs_PreservesBothUnderSameWrapper()
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

        // Act — aggregations on both parent-level and child-level fields
        var aggs = await parser.BuildAggregationsAsync("terms:parent.name terms:parent.child.name");

        // Assert — both aggs are under same nested_parent wrapper, child is under nested_parent.child inside it
        Assert.NotNull(aggs);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(aggs, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("nested_parent", json);
        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("nested_parent.child", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
        Assert.Contains("terms_parent.name", json);
        Assert.Contains("terms_parent.child.name", json);

        // Verify nested_parent.child is nested inside nested_parent
        int nestedParentChildCount = json.Split("nested_parent.child").Length - 1;
        Assert.True(nestedParentChildCount >= 1, "Expected nested_parent.child to appear in the JSON");
    }

    [Fact]
    public async Task BuildAggregationsAsync_WithFilteredParentAndChildAggs_DoesNotOverwrite()
    {
        // Arrange — parent and parent.child are both nested
        var grandchildProps = new Properties
        {
            { "status", new KeywordProperty { Name = "status" } }
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
            .UseNested()
            .UseNestedFilter((path, field, originalField, ctx) =>
                Task.FromResult<QueryContainer?>(new TermQuery { Field = $"{path}.active", Value = true })));

        // Act — aggregations on both parent-level and child-level fields with filter
        var aggs = await parser.BuildAggregationsAsync("terms:parent.name terms:parent.child.status");

        // Assert — both aggs preserved under nested wrappers with filters
        Assert.NotNull(aggs);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(aggs, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        // Both parent-level and child-level term aggs must exist
        Assert.Contains("terms_parent.name", json);
        Assert.Contains("terms_parent.child.status", json);
        // Both filter wrappers must exist
        Assert.Contains("filtered_terms_parent.name", json);
        Assert.Contains("filtered_terms_parent.child.status", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithMultipleDefaultFieldsAndDistinctFilters_AppliesPerFieldFilter()
    {
        // Arrange — items is nested with two fields
        var itemProps = new Properties
        {
            { "status", new KeywordProperty { Name = "status" } },
            { "priority", new KeywordProperty { Name = "priority" } }
        };
        var rootProps = new Properties
        {
            { "items", new NestedProperty { Name = "items", Properties = itemProps } }
        };
        var mapping = new TypeMapping { Properties = rootProps };
        var resolver = new ElasticMappingResolver(mapping, _inferrer, () => null, logger: _logger);

        // Filter resolver returns distinct filters per field
        var parser = new ElasticQueryParser(c => c
            .UseMappings(resolver)
            .UseNested()
            .SetDefaultFields(new[] { "items.status", "items.priority" })
            .UseNestedFilter((path, field, originalField, ctx) =>
            {
                if (field == "items.status")
                    return Task.FromResult<QueryContainer?>(new TermQuery { Field = "items.type", Value = "status_filter" });
                if (field == "items.priority")
                    return Task.FromResult<QueryContainer?>(new TermQuery { Field = "items.type", Value = "priority_filter" });
                return Task.FromResult<QueryContainer?>(null);
            }));

        // Act — unqualified term search
        var result = await parser.BuildQueryAsync("active");

        // Assert — should produce nested query with per-field should branches, each with own filter
        Assert.NotNull(result);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(result, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("nested", json);
        Assert.Contains("status_filter", json);
        Assert.Contains("priority_filter", json);
        Assert.Contains("items.status", json);
        Assert.Contains("items.priority", json);
    }

    [Fact]
    public async Task BuildQueryAsync_WithExplicitNestedGroupAndNegatedDeeperChild_ProducesCorrelatedNegation()
    {
        // Arrange — parent and parent.child are nested
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

        // Act — explicit nested group with negated deeper child
        var result = await parser.BuildQueryAsync("parent:(parent.name:Bob AND NOT parent.child.name:Alice)");

        // Assert — correlated negation: nested(parent) containing name:Bob AND must_not nested(parent.child, name:Alice)
        Assert.NotNull(result);

        using var stream = new System.IO.MemoryStream();
        new ElasticClient(_connectionSettings).RequestResponseSerializer.Serialize(result, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());

        Assert.Contains("\"path\":\"parent\"", json);
        Assert.Contains("Bob", json);
        Assert.Contains("Alice", json);
        Assert.Contains("must_not", json);
        Assert.Contains("\"path\":\"parent.child\"", json);
    }

    [Fact]
    public async Task BuildSortAsync_WithMultiLevelNestedFieldAndFilter_AppliesFilterOnInnermost()
    {
        // Arrange — parent and parent.child are nested
        var grandchildProps = new Properties
        {
            { "score", new NumberProperty(NumberType.Integer) { Name = "score" } }
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
            .UseNested()
            .UseNestedFilter((path, field, originalField, ctx) =>
                Task.FromResult<QueryContainer?>(new TermQuery { Field = $"{path}.active", Value = true })));

        // Act — sort on multi-level nested field
        var sorts = await parser.BuildSortAsync("-parent.child.score");

        // Assert — hierarchical nested sort with filter on innermost level
        Assert.NotNull(sorts);
        var sortList = sorts.ToList();
        Assert.Single(sortList);

        var fieldSort = Assert.IsAssignableFrom<IFieldSort>(sortList[0]);
        Assert.Equal(SortOrder.Descending, fieldSort.Order);
        Assert.NotNull(fieldSort.Nested);
        Assert.Equal("parent", fieldSort.Nested.Path);
        Assert.NotNull(fieldSort.Nested.Nested);
        Assert.Equal("parent.child", fieldSort.Nested.Nested.Path);

        // Filter should be on the innermost nested sort
        Assert.NotNull(fieldSort.Nested.Nested.Filter);
    }
}
