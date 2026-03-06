using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Xunit;
using Microsoft.Extensions.Time.Testing;
using Nest;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticMappingResolverUnitTests : TestWithLoggingBase
{
    public ElasticMappingResolverUnitTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    private static Inferrer CreateInferrer()
    {
        var settings = new ConnectionSettings(new Uri("http://localhost:9200"));
        return new Inferrer(settings);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), CreateInferrer(), () => null, logger: _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("title", "keyword");

        // Assert
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetAggregationsFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), CreateInferrer(), () => null, logger: _logger);

        // Act
        string result = resolver.GetAggregationsFieldName("title");

        // Assert
        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetSortFieldName_WithTextPropertyAndSortSubField_ReturnsSortPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordAndSortMapping("title"), CreateInferrer(), () => null, logger: _logger);

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
        var resolver = new ElasticMappingResolver(codeMapping, CreateInferrer(), () => null, logger: _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("status", "keyword");

        // Assert
        Assert.Equal("status", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyWithoutSubFields_ReturnsBareFieldName()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("body"), CreateInferrer(), () => null, logger: _logger);

        // Act
        string result = resolver.GetNonAnalyzedFieldName("body", "keyword");

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
        }, CreateInferrer(), logger: _logger);

        // Act
        string beforeRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string afterRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");

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
        }, CreateInferrer(), logger: _logger);

        // Act
        string first = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string second = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal("name", first);
        Assert.Equal("name.keyword", second);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithCodeAndServerMerge_ReturnsKeywordSubField()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("name"), CreateInferrer(),
            () => CreateTextOnlyMapping("name"), logger: _logger);
        resolver.RefreshMapping();

        // Act
        string result = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal("name.keyword", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_AfterRefreshAndServerMappingChange_ReturnsUpdatedKeywordPath()
    {
        // Arrange
        int callCount = 0;
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("name"), CreateInferrer(), () =>
            {
                int callNumber = Interlocked.Increment(ref callCount);
                return callNumber <= 1 ? null : CreateTextWithKeywordMapping("name");
            }, logger: _logger);

        // Act
        string initial = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string updated = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal("name", initial);
        Assert.Equal("name.keyword", updated);
    }

    [Fact]
    public async Task ConcurrentGetMappingAndRefreshMapping_UnderContention_AlwaysReturnsKeywordPath()
    {
        // Arrange
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("name"), CreateInferrer(), () =>
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
                string result = resolver.GetNonAnalyzedFieldName("name", "keyword");
                Assert.Equal("name.keyword", result);
            }
        }, TestCancellationToken);

        var aggregationReaderTask = Task.Run(() =>
        {
            barrier.SignalAndWait(TestCancellationToken);
            for (int i = 0; i < iterations; i++)
            {
                string result = resolver.GetAggregationsFieldName("name");
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
        }, CreateInferrer(), timeProvider: timeProvider, logger: _logger);

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

        // Act - advance past the 1-minute throttle window, refresh to clear cache
        timeProvider.Advance(TimeSpan.FromMinutes(2));
        resolver.RefreshMapping();
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterTimeAdvance = fetchCount;
        Assert.True(afterTimeAdvance > afterRefresh, "Fetch should happen after time advances past throttle");
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
}
