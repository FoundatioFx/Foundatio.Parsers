using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using MEL = Microsoft.Extensions.Logging;

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
    public void GetMapping_WithAliasMissingPath_ReturnsNullConsistently()
    {
        // Arrange
        var properties = CreateProperties(("alias", new FieldAliasProperty()));
        using var resolver = new ElasticMappingResolver(() => new TypeMapping { Properties = properties }, _inferrer, logger: _logger);

        // Act
        var first = resolver.GetMapping("alias", followAlias: true);
        var cached = resolver.GetMapping("alias", followAlias: true);

        // Assert
        Assert.Null(first);
        Assert.Null(cached);
    }

    [Fact]
    public void FieldMapping_LegacyConstructor_RemainsAvailable()
    {
        // Arrange
        var property = new KeywordProperty();

        // Act
        var mapping = new FieldMapping("name", property, DateTime.UtcNow, 1);

        // Assert
        Assert.Equal("name", mapping.FullPath);
        Assert.Same(property, mapping.Property);
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefreshMapping_DuringInFlightRefresh_DiscardsSupersededResult(bool throwException)
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        TypeMapping serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            int callNumber = Interlocked.Increment(ref fetchCount);
            TypeMapping capturedMapping = serverMapping;
            if (callNumber == 2)
            {
                fetchStarted.Set();
                releaseFetch.Wait(TimeSpan.FromSeconds(30));
                if (throwException)
                    throw new InvalidOperationException("Elasticsearch is unavailable");
            }

            return capturedMapping;
        }, _inferrer, logger: _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        var staleLookup = Task.Run(() => resolver.GetMapping("idx.keyword-000001"));
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));

        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        resolver.RefreshMapping();
        releaseFetch.Set();

        // Act
        var staleResult = await staleLookup;
        var refreshedResult = resolver.GetMapping("idx.keyword-000001");

        // Assert
        Assert.False(staleResult?.Found);
        Assert.True(refreshedResult?.Found);
        Assert.Equal(3, fetchCount);
    }

    [Fact]
    public void GetMapping_WithResolvedField_DoesNotRefetchServerMapping()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        // Act
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterColdStart = fetchCount;

        timeProvider.Advance(TimeSpan.FromSeconds(30));
        resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal(1, afterColdStart);
        Assert.Equal(afterColdStart, fetchCount);
    }

    [Fact]
    public void GetMapping_WithUnknownFieldOnColdStart_FetchesServerMappingOnce()
    {
        // Arrange
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        // Act
        var mapping = resolver.GetMapping("missing");

        // Assert
        Assert.False(mapping?.Found);
        Assert.Equal(1, fetchCount);
    }

    [Fact]
    public void GetMapping_WithChangedResolvedField_RequiresExplicitRefresh()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return serverMapping;
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.IsType<TextProperty>(resolver.GetMappingProperty("name"));
        serverMapping = new TypeMapping { Properties = CreateProperties(("name", new KeywordProperty())) };
        timeProvider.Advance(TimeSpan.FromHours(1));

        // Act
        var staleProperty = resolver.GetMappingProperty("name");
        resolver.RefreshMapping();
        var refreshedProperty = resolver.GetMappingProperty("name");

        // Assert
        Assert.IsType<TextProperty>(staleProperty);
        Assert.IsType<KeywordProperty>(refreshedProperty);
        Assert.Equal(2, fetchCount);
    }

    [Fact]
    public void RefreshMapping_WhenCalled_RefetchesServerMapping()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterColdStart = fetchCount;

        // Act
        resolver.RefreshMapping();
        resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal(afterColdStart + 1, fetchCount);
    }

    [Fact]
    public void IsNestedPropertyType_WithNestedFieldCreatedAfterColdStartFetch_ReturnsTrue()
    {
        // Arrange - a dynamic template creates idx.nested-000001 only after the first document is indexed,
        // which is always after the resolver has already loaded the mapping at least once.
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var serverMapping = CreateTextWithKeywordMapping("field1");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.Equal("field1.keyword", resolver.GetNonAnalyzedFieldName("field1", "keyword"));

        // Act
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        serverMapping = CreateDynamicCustomFieldMapping("field1", "nested-000001", new NestedProperty { Properties = CreateProperties(("value", new KeywordProperty())) });

        // Assert
        Assert.True(resolver.IsNestedPropertyType("idx.nested-000001"));
        Assert.Equal("idx.nested-000001.value", resolver.GetResolvedField("idx.nested-000001.value"));
    }

    [Fact]
    public void GetSortFieldName_WithKeywordSubFieldCreatedAfterColdStartFetch_ReturnsKeywordSubField()
    {
        // Arrange - sorting on a text field without resolving its keyword sub field produces an
        // "Fielddata is disabled" error from Elasticsearch, so a stale mapping is a hard failure here.
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var serverMapping = CreateTextWithKeywordMapping("field1");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, timeProvider: timeProvider, logger: _logger);

        resolver.GetNonAnalyzedFieldName("field1", "keyword");

        // Act
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        serverMapping = CreateDynamicCustomFieldMapping("field1", "string-000001",
            new TextProperty { Fields = CreateProperties(("keyword", new KeywordProperty { IgnoreAbove = 256 })) });

        // Assert
        Assert.Equal("idx.string-000001.keyword", resolver.GetSortFieldName("idx.string-000001"));
    }

    [Fact]
    public void GetMapping_WithUnmappedFieldWithinUnmappedRefreshInterval_DoesNotRefetchServerMapping()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        resolver.GetMapping("name");
        Assert.Equal(1, fetchCount);

        // Act
        resolver.GetMapping("missing_one");
        int afterFirstMiss = fetchCount;

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        resolver.GetMapping("missing_two");

        // Assert
        Assert.Equal(2, afterFirstMiss);
        Assert.Equal(afterFirstMiss, fetchCount);
    }

    [Fact]
    public void GetMapping_WithContinuousDistinctMisses_AttemptsAtMostOncePerRefreshInterval()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.True(resolver.GetMapping("name")?.Found);

        // Act - twelve five-second windows model one minute of continuous attacker-controlled misses.
        for (int interval = 0; interval < 12; interval++)
        {
            for (int miss = 0; miss < 25; miss++)
                Assert.False(resolver.GetMapping($"missing_{interval}_{miss}")?.Found);

            if (interval < 11)
                timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);
        }

        // Assert - one cold-start fetch plus at most twelve automatic reload attempts.
        Assert.Equal(13, fetchCount);
    }

    [Fact]
    public void GetMapping_WhenTimeProviderTimestampStartsAtZero_ThrottlesRepeatedMisses()
    {
        // Arrange
        var timeProvider = new ZeroOriginTimeProvider();
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider, _logger);

        Assert.True(resolver.GetMapping("name")?.Found);

        // Act
        Assert.False(resolver.GetMapping("missing1")?.Found);
        Assert.False(resolver.GetMapping("missing2")?.Found);

        // Assert
        Assert.Equal(2, fetchCount);

        timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);
        Assert.False(resolver.GetMapping("missing3")?.Found);
        Assert.Equal(3, fetchCount);
    }

    [Fact]
    public void GetMapping_WithNewFieldAfterUnrelatedMiss_RefreshesAtUnmappedFieldInterval()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return serverMapping;
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        Assert.False(resolver.GetMapping("typo")?.Found);

        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);

        // Act
        var mapping = resolver.GetMapping("idx.keyword-000001");

        // Assert
        Assert.True(mapping?.Found);
        Assert.Equal(3, fetchCount);
    }

    [Fact]
    public void GetMapping_WithCachedMissAfterFieldIsCreated_ResolvesFieldOnNextLookup()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, timeProvider: timeProvider, logger: _logger);

        var beforeCreate = resolver.GetMapping("idx.keyword-000001");
        Assert.NotNull(beforeCreate);
        Assert.False(beforeCreate.Found);

        // Act
        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        timeProvider.Advance(TimeSpan.FromSeconds(10));
        var afterCreate = resolver.GetMapping("idx.keyword-000001");

        // Assert
        Assert.NotNull(afterCreate);
        Assert.True(afterCreate.Found);
        Assert.Equal("idx.keyword-000001", afterCreate.FullPath);
    }

    [Fact]
    public void RefreshMapping_WithNewlyCreatedField_ResolvesFieldWithoutWaitingForThrottle()
    {
        // Arrange - arm the unmapped field throttle before the field exists.
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.False(resolver.GetMapping("name.missing")!.Found);
        Assert.False(resolver.GetMapping("idx.keyword-000001")!.Found);

        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        Assert.False(resolver.GetMapping("idx.keyword-000001")!.Found);

        // Act
        resolver.RefreshMapping();
        var mapping = resolver.GetMapping("idx.keyword-000001");

        // Assert
        Assert.NotNull(mapping);
        Assert.True(mapping.Found);
    }

    [Fact]
    public void GetMapping_WhenServerMappingFuncThrows_DoesNotRefetchOnEveryLookup()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            throw new InvalidOperationException("Elasticsearch is unavailable");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        // Act
        for (int i = 0; i < 25; i++)
            Assert.False(resolver.GetMapping($"field_{i}")!.Found);

        // Assert - the failed cold start also arms the miss throttle, so one lookup cannot immediately retry.
        Assert.Equal(1, fetchCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GetMapping_WithContinuousFailedRefreshes_AttemptsAtMostOncePerRefreshInterval(bool throwException)
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        bool failRefresh = false;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            if (!failRefresh)
                return CreateTextWithKeywordMapping("name");

            if (throwException)
                throw new InvalidOperationException("Elasticsearch is unavailable");

            return null;
        }, _inferrer, timeProvider, _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        failRefresh = true;

        // Act
        for (int interval = 0; interval < 12; interval++)
        {
            for (int miss = 0; miss < 25; miss++)
                Assert.False(resolver.GetMapping($"missing_{interval}_{miss}")?.Found);

            Assert.True(resolver.GetMapping("name")?.Found);
            if (interval < 11)
                timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);
        }

        // Assert - failures preserve the known mapping and obey the same ceiling as successful reloads.
        Assert.Equal(13, fetchCount);
    }

    [Fact]
    public void RefreshMapping_WithinMissCooldown_PerformsUnthrottledHardRefresh()
    {
        // Arrange
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        Assert.True(resolver.GetMapping("name")?.Found); // cold start
        Assert.False(resolver.GetMapping("missing")?.Found); // automatic miss reload
        resolver.RefreshMapping();
        Assert.True(resolver.GetMapping("name")?.Found); // explicit hard refresh

        // Assert
        Assert.Equal(3, fetchCount);
    }

    [Fact]
    public void RefreshMapping_AfterTargetRecreation_DiscardsLastKnownGoodMapping()
    {
        // Arrange
        TypeMapping? serverMapping = CreateTextOnlyMapping("name");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer,
            new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        Assert.Equal("name", resolver.GetNonAnalyzedFieldName("name", "keyword"));

        // Automatic failure during the deletion gap retains mapping A.
        serverMapping = null;
        Assert.False(resolver.GetMapping("missing")?.Found);
        Assert.Equal("name", resolver.GetNonAnalyzedFieldName("name", "keyword"));

        // Act - deletion invalidation discards A, and recreation invalidation bypasses the failed-miss cooldown.
        resolver.RefreshMapping();
        Assert.False(resolver.GetMapping("name")?.Found);

        serverMapping = CreateTextWithKeywordMapping("name");
        resolver.RefreshMapping();
        string? recreatedField = resolver.GetNonAnalyzedFieldName("name", "keyword");

        // Assert
        Assert.Equal("name.keyword", recreatedField);
    }

    [Fact]
    public void GetMapping_WhenAutomaticRefreshReturnsNull_PreservesLastKnownGoodMapping()
    {
        // Arrange
        int fetchCount = 0;
        TypeMapping? serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return serverMapping;
        }, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        serverMapping = null;

        // Act
        Assert.False(resolver.GetMapping("missing")?.Found);
        var knownMapping = resolver.GetMapping("name");

        // Assert
        Assert.True(knownMapping?.Found);
        Assert.Equal(2, fetchCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GetMapping_AfterFailedMissRefresh_RetriesAfterInterval(bool throwException)
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        bool failRefresh = false;
        TypeMapping serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            if (failRefresh)
            {
                if (throwException)
                    throw new InvalidOperationException("Elasticsearch is unavailable");

                return null;
            }

            return serverMapping;
        }, _inferrer, timeProvider, _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        failRefresh = true;
        Assert.False(resolver.GetMapping("missing")?.Found);

        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        failRefresh = false;

        // Act
        Assert.False(resolver.GetMapping("idx.keyword-000001")?.Found);
        timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);
        var mapping = resolver.GetMapping("idx.keyword-000001");

        // Assert
        Assert.True(mapping?.Found);
        Assert.Equal(3, fetchCount);
    }

    [Fact]
    public void GetMapping_WithFailedMissRefresh_AttemptsOneReload()
    {
        // Arrange
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        bool serverUnavailable = false;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            if (serverUnavailable)
                throw new InvalidOperationException("Elasticsearch is unavailable");

            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        serverUnavailable = true;

        // Act
        var mapping = resolver.GetMapping("missing");

        // Assert
        Assert.False(mapping?.Found);
        Assert.Equal(2, fetchCount);
    }

    [Fact]
    public async Task GetMapping_WithConcurrentUnmappedLookups_FetchesServerMappingOnce()
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            if (Interlocked.Increment(ref fetchCount) > 1)
            {
                fetchStarted.Set();
                releaseFetch.Wait(TimeSpan.FromSeconds(30));
            }

            return CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        resolver.GetMapping("name");
        int afterColdStart = fetchCount;

        // Act
        var lookups = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() => resolver.GetMapping($"missing{i}")))
            .ToArray();
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));
        releaseFetch.Set();
        await Task.WhenAll(lookups);

        // Assert
        Assert.Equal(1, afterColdStart);
        Assert.Equal(afterColdStart + 1, fetchCount);
        Assert.All(lookups, t => Assert.False(t.Result!.Found));
    }

    [Fact]
    public async Task GetMapping_WithCachedKnownFieldDuringBlockedRefresh_ReturnsImmediately()
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            if (Interlocked.Increment(ref fetchCount) > 1)
            {
                fetchStarted.Set();
                releaseFetch.Wait(TimeSpan.FromSeconds(30));
            }

            return CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        Assert.True(resolver.GetMapping("name")?.Found);
        var blockedMiss = Task.Run(() => resolver.GetMapping("missing"));
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));

        // Act
        var cachedLookup = Task.Run(() => resolver.GetMapping("name"));

        // Assert
        try
        {
            var cachedMapping = await cachedLookup.WaitAsync(TimeSpan.FromSeconds(1), TestCancellationToken);
            Assert.True(cachedMapping?.Found);
        }
        finally
        {
            releaseFetch.Set();
            await blockedMiss;
        }
    }

    [Fact]
    public async Task GetMapping_WhenAnotherCallerPublishesNewerMapping_UsesNewerMapping()
    {
        // Arrange
        using var timeProvider = new BlockingTimeProvider();
        TypeMapping serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, timeProvider, _logger);

        Assert.False(resolver.GetMapping("idx.keyword-000001")!.Found);
        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);

        timeProvider.BlockNextTimestampRead();
        var staleLookup = Task.Run(() => resolver.GetMapping("idx.keyword-000001"));
        Assert.True(timeProvider.WaitUntilBlocked(TimeSpan.FromSeconds(10)));

        // Act
        var freshLookup = resolver.GetMapping("idx.keyword-000001");
        timeProvider.ReleaseTimestampRead();
        var result = await staleLookup;

        // Assert
        Assert.True(freshLookup?.Found);
        Assert.True(result?.Found);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetMapping_WithConcurrentFailedMissRefresh_AttemptsOnce(bool throwException)
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            int callNumber = Interlocked.Increment(ref fetchCount);
            if (callNumber == 1)
                return CreateTextWithKeywordMapping("name");

            fetchStarted.Set();
            releaseFetch.Wait(TimeSpan.FromSeconds(30));
            if (throwException)
                throw new InvalidOperationException("Elasticsearch is unavailable");

            return null;
        }, _inferrer, logger: _logger);

        Assert.True(resolver.GetMapping("name")?.Found);

        // Act
        var lookups = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() => resolver.GetMapping($"missing{i}")))
            .ToArray();
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));
        releaseFetch.Set();
        await Task.WhenAll(lookups);

        // Assert
        Assert.Equal(2, fetchCount);
        Assert.All(lookups, task => Assert.False(task.Result?.Found));
        Assert.True(resolver.GetMapping("name")?.Found);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetMapping_WithConcurrentFailedInitialFetch_AttemptsOnce(bool throwException)
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            fetchStarted.Set();
            releaseFetch.Wait(TimeSpan.FromSeconds(30));
            if (throwException)
                throw new InvalidOperationException("Elasticsearch is unavailable");

            return null;
        }, _inferrer, logger: _logger);

        // Act
        var lookups = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() => resolver.GetMapping($"field{i}")))
            .ToArray();
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));
        releaseFetch.Set();
        await Task.WhenAll(lookups);

        // Assert
        Assert.Equal(1, fetchCount);
        Assert.All(lookups, task => Assert.False(task.Result?.Found));
    }

    [Fact]
    public async Task GetMapping_WithConcurrentSuccessfulInitialFetch_AttemptsOnce()
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            fetchStarted.Set();
            releaseFetch.Wait(TimeSpan.FromSeconds(30));
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        // Act
        var lookups = Enumerable.Range(0, 100)
            .Select(_ => Task.Run(() => resolver.GetMapping("name")))
            .ToArray();
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));
        await Task.Delay(200, TestCancellationToken);
        releaseFetch.Set();
        await Task.WhenAll(lookups);

        // Assert
        Assert.Equal(1, fetchCount);
        Assert.All(lookups, task => Assert.True(task.Result?.Found));
    }

    [Fact]
    public async Task GetMapping_WhenInitialFetchOutlastsJoinTimeout_WaitsOnlyOnce()
    {
        // Arrange
        using var fetchStarted = new ManualResetEventSlim(false);
        using var releaseFetch = new ManualResetEventSlim(false);
        using var resolver = new ElasticMappingResolver(() =>
        {
            fetchStarted.Set();
            releaseFetch.Wait(TimeSpan.FromSeconds(30));
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger)
        {
            MappingRefreshWaitTimeout = TimeSpan.FromSeconds(1)
        };

        var initialLookup = Task.Run(() => resolver.GetMapping("name"));
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestCancellationToken));

        // Act
        FieldMapping? timedOutLookup;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            timedOutLookup = resolver.GetMapping("missing");
        }
        finally
        {
            stopwatch.Stop();
            releaseFetch.Set();
            await initialLookup;
        }

        // Assert
        Assert.False(timedOutLookup?.Found);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(1750),
            $"Lookup waited {stopwatch.Elapsed} even though the configured join timeout was {resolver.MappingRefreshWaitTimeout}.");
    }

    [Fact]
    public async Task GetMapping_WithLookupDuringFetchThatOutlastedJoinTimeout_StillJoinsInFlightFetch()
    {
        // Arrange - a lookup that gives up joining an in-flight fetch reports the field as unmapped, but that
        // must not stop later lookups from joining the same fetch and getting the real answer.
        var fetchStarted = new ManualResetEventSlim(false);
        var releaseFetch = new ManualResetEventSlim(false);
        int fetchCount = 0;
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            if (Interlocked.Increment(ref fetchCount) > 1)
            {
                fetchStarted.Set();
                releaseFetch.Wait(TimeSpan.FromSeconds(30));
            }

            return serverMapping;
        }, _inferrer, logger: _logger);

        resolver.GetMapping("name");
        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());

        var inFlightFetch = Task.Run(() => resolver.GetMapping("idx.keyword-000001"));
        Assert.True(fetchStarted.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));

        // Act - this lookup gives up joining the in-flight fetch
        resolver.MappingRefreshWaitTimeout = TimeSpan.FromMilliseconds(50);
        var timedOutLookup = resolver.GetMapping("idx.keyword-000001");

        // Act - a later lookup, still while that same fetch is running, must join it rather than be served
        // the unmapped result the timed out lookup just produced
        resolver.MappingRefreshWaitTimeout = TimeSpan.FromSeconds(30);
        var joiningLookup = Task.Run(() => resolver.GetMapping("idx.keyword-000001"));
        await Task.Delay(200, TestContext.Current.CancellationToken);
        releaseFetch.Set();

        // Assert
        Assert.NotNull(timedOutLookup);
        Assert.False(timedOutLookup.Found);

        var joined = await joiningLookup;
        Assert.NotNull(joined);
        Assert.True(joined.Found);
        Assert.Equal("idx.keyword-000001", joined.FullPath);

        var completed = await inFlightFetch;
        Assert.NotNull(completed);
        Assert.True(completed.Found);
    }

    [Fact]
    public void MappingRefreshWaitTimeout_ByDefault_ExceedsTypicalMappingFetchLatency()
    {
        // Arrange + Act
        using var resolver = new ElasticMappingResolver(() => CreateTextWithKeywordMapping("name"), _inferrer, logger: _logger);

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(30), resolver.MappingRefreshWaitTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), resolver.UnmappedFieldRefreshInterval);
    }

    [Theory]
    [InlineData("keyword")]
    [InlineData("date")]
    [InlineData("long")]
    [InlineData("boolean")]
    [InlineData("ip")]
    public void GetMapping_WithMultiFieldOnNonTextProperty_ResolvesSubField(string propertyType)
    {
        // Arrange - every Elasticsearch property type can carry multi-fields, not just text. Resolving them
        // for text only silently reports "field.subfield" as unmapped for every other type.
        var property = CreateProperty(propertyType);

        var subFields = new Properties();
        subFields.Add("sort", new KeywordProperty());
        SetMultiFields(property, subFields);

        var properties = new Properties();
        properties.Add("code", property);
        using var resolver = new ElasticMappingResolver(() => new TypeMapping { Properties = properties }, _inferrer, logger: _logger);

        // Act
        var parent = resolver.GetMapping("code");
        var subField = resolver.GetMapping("code.sort");

        // Assert
        Assert.NotNull(parent);
        Assert.True(parent.Found);
        Assert.NotNull(subField);
        Assert.True(subField.Found);
        Assert.Equal("code.sort", subField.FullPath);
        Assert.IsType<KeywordProperty>(subField.Property);
    }


    [Theory]
    [InlineData("keyword")]
    [InlineData("date")]
    [InlineData("long")]
    [InlineData("boolean")]
    [InlineData("ip")]
    public void GetMapping_WithCodeDeclaredMultiFieldOnNonTextServerProperty_ResolvesSubField(string propertyType)
    {
        // Arrange - the server reports the property without the multi-field the code mapping declares.
        // Merging has to layer the code declared sub-field on for every property type, not just text.
        IProperty codeProperty = CreateProperty(propertyType);
        var codeSubFields = new Properties();
        codeSubFields.Add("sort", new KeywordProperty());
        SetMultiFields(codeProperty, codeSubFields);

        var codeProperties = new Properties();
        codeProperties.Add("code", codeProperty);

        var serverProperties = new Properties();
        serverProperties.Add("code", CreateProperty(propertyType));

        using var resolver = new ElasticMappingResolver(new TypeMapping { Properties = codeProperties }, _inferrer,
            () => new TypeMapping { Properties = serverProperties }, logger: _logger);

        // Act
        var subField = resolver.GetMapping("code.sort");

        // Assert
        Assert.NotNull(subField);
        Assert.True(subField.Found);
        Assert.Equal("code.sort", subField.FullPath);
        Assert.IsType<KeywordProperty>(subField.Property);
    }

    [Fact]
    public void GetMapping_WithCodeAndServerMappings_DoesNotMutateServerMapping()
    {
        // Arrange - the merged view must not be written back into the mapping the callback returned,
        // otherwise a caller that caches or shares that instance sees it grow sub-fields over time.
        var codeProperty = new KeywordProperty();
        var codeSubFields = new Properties();
        codeSubFields.Add("sort", new KeywordProperty());
        codeProperty.Fields = codeSubFields;

        var codeProperties = new Properties();
        codeProperties.Add("code", codeProperty);

        var serverProperty = new KeywordProperty();
        var serverProperties = new Properties();
        serverProperties.Add("code", serverProperty);

        using var resolver = new ElasticMappingResolver(new TypeMapping { Properties = codeProperties }, _inferrer,
            () => new TypeMapping { Properties = serverProperties }, logger: _logger);

        // Act
        var subField = resolver.GetMapping("code.sort");

        // Assert
        Assert.True(subField?.Found);
        Assert.Null(serverProperty.Fields);
    }

    [Fact]
    public void GetMapping_WithIncompatibleCodeAndServerParentTypes_DoesNotFabricateChildField()
    {
        // Arrange - the server mapping is authoritative about whether a field is a container or a leaf.
        var codeMapping = new TypeMapping
        {
            Properties = CreateProperties(("value", new ObjectProperty
            {
                Properties = CreateProperties(("child", new KeywordProperty()))
            }))
        };
        var serverMapping = new TypeMapping
        {
            Properties = CreateProperties(("value", new KeywordProperty()))
        };
        using var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => serverMapping, logger: _logger);

        // Act
        var mapping = resolver.GetMapping("value.child");

        // Assert
        Assert.False(mapping?.Found);
        Assert.Equal("value.child", mapping?.FullPath);
    }

    [Fact]
    public void GetMapping_WithIncompatibleCodeAndServerScalarTypes_DoesNotFabricateChildField()
    {
        // Arrange - a server scalar with a different type must not inherit code-only multi-fields.
        var codeMapping = new TypeMapping
        {
            Properties = CreateProperties(("value", new TextProperty
            {
                Fields = CreateProperties(("keyword", new KeywordProperty()))
            }))
        };
        var serverMapping = new TypeMapping
        {
            Properties = CreateProperties(("value", new KeywordProperty()))
        };
        using var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => serverMapping, logger: _logger);

        // Act
        var mapping = resolver.GetMapping("value.keyword");

        // Assert
        Assert.False(mapping?.Found);
        Assert.Equal("value.keyword", mapping?.FullPath);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithCodeDeclaredKeywordSubField_UsesCodeDeclaredSubField()
    {
        // Arrange - sorting on an analyzed field fails unless the non analyzed sub-field is found, and the
        // sub-field may only be declared in code.
        var codeProperty = new TextProperty();
        var codeSubFields = new Properties();
        codeSubFields.Add("keyword", new KeywordProperty());
        codeProperty.Fields = codeSubFields;

        var codeProperties = new Properties();
        codeProperties.Add("name", codeProperty);

        var serverProperties = new Properties();
        serverProperties.Add("name", new TextProperty());

        using var resolver = new ElasticMappingResolver(new TypeMapping { Properties = codeProperties }, _inferrer,
            () => new TypeMapping { Properties = serverProperties }, logger: _logger);

        // Act
        string? resolved = resolver.GetNonAnalyzedFieldName("name");

        // Assert
        Assert.Equal("name.keyword", resolved);
    }

    [Fact]
    public void GetMapping_WithSharedServerPropertyInstanceAndDistinctCodeSubFields_DoesNotLeakSubFieldsAcrossFields()
    {
        // Arrange - reusing one IProperty instance for several fields is legal, so merged children must be
        // keyed by field name. Keying them by property instance makes one field's code declared sub-field
        // resolve under the other field's name.
        var alphaCode = new KeywordProperty { Fields = CreateProperties(("alpha_only", new KeywordProperty())) };
        var betaCode = new KeywordProperty { Fields = CreateProperties(("beta_only", new KeywordProperty())) };
        var codeMapping = new TypeMapping { Properties = CreateProperties(("alpha", alphaCode), ("beta", betaCode)) };

        var shared = new KeywordProperty();
        var serverMapping = new TypeMapping { Properties = CreateProperties(("alpha", shared), ("beta", shared)) };

        using var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => serverMapping, logger: _logger);

        // Act
        var alphaOwn = resolver.GetMapping("alpha.alpha_only");
        var betaOwn = resolver.GetMapping("beta.beta_only");
        var alphaLeaked = resolver.GetMapping("alpha.beta_only");
        var betaLeaked = resolver.GetMapping("beta.alpha_only");

        // Assert
        Assert.True(alphaOwn?.Found);
        Assert.True(betaOwn?.Found);
        Assert.False(alphaLeaked?.Found);
        Assert.False(betaLeaked?.Found);
    }

    [Fact]
    public void GetMapping_WithManyDistinctMisses_KeepsKnownFieldsResolvable()
    {
        // Arrange - many caller-controlled misses should share one throttled reload attempt without affecting
        // successful resolutions.
        var properties = new Properties();
        properties.Add("name", new KeywordProperty());
        properties.Add("status", new KeywordProperty());
        properties.Add("created", new DateProperty());
        properties.Add("count", new LongNumberProperty());
        string[] realFields = ["name", "status", "created", "count"];
        int fetchCount = 0;

        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return new TypeMapping { Properties = properties };
        }, _inferrer,
            new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);
        resolver.UnmappedFieldRefreshInterval = TimeSpan.FromHours(1);

        foreach (string realField in realFields)
            Assert.True(resolver.GetMapping(realField)?.Found);
        int afterColdStart = fetchCount;

        // Act
        for (int i = 0; i < 100; i++)
            Assert.False(resolver.GetMapping($"missing{i}")?.Found);

        // Assert
        Assert.Equal(afterColdStart + 1, fetchCount);

        foreach (string realField in realFields)
            Assert.True(resolver.GetMapping(realField)?.Found);
    }

    [Fact]
    public void UnmappedFieldRefreshInterval_WithNegativeValue_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => null, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        var exception = Record.Exception(() => resolver.UnmappedFieldRefreshInterval = TimeSpan.FromSeconds(-1));

        // Assert
        var argumentException = Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", argumentException.ParamName);
    }

    [Fact]
    public void UnmappedFieldRefreshInterval_WithZero_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => null, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        var exception = Record.Exception(() => resolver.UnmappedFieldRefreshInterval = TimeSpan.Zero);

        // Assert
        var argumentException = Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", argumentException.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MappingRefreshWaitTimeout_WithNonPositiveValue_ThrowsArgumentOutOfRangeException(int seconds)
    {
        // Arrange - a zero or negative wait would silently resolve fields as unmapped while a reload that
        // could resolve them is already running.
        using var resolver = new ElasticMappingResolver(() => null, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        var exception = Record.Exception(() => resolver.MappingRefreshWaitTimeout = TimeSpan.FromSeconds(seconds));

        // Assert
        var argumentException = Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", argumentException.ParamName);
    }

    [Fact]
    public void MappingRefreshWaitTimeout_WithInfiniteTimeSpan_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => CreateTextWithKeywordMapping("name"), _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        var exception = Record.Exception(() => resolver.MappingRefreshWaitTimeout = Timeout.InfiniteTimeSpan);

        // Assert
        var argumentException = Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", argumentException.ParamName);
    }

    [Fact]
    public void MappingRefreshWaitTimeout_WithValueExceedingSemaphoreLimit_ThrowsArgumentOutOfRangeException()
    {
        // Arrange - a value a semaphore cannot accept was previously clamped silently on every wait, so the
        // configured timeout and the effective one disagreed.
        using var resolver = new ElasticMappingResolver(() => null, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        var exception = Record.Exception(() => resolver.MappingRefreshWaitTimeout = TimeSpan.FromDays(30));

        // Assert
        var argumentException = Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", argumentException.ParamName);
    }

    [Fact]
    public void GetMapping_AfterFailedColdStart_RetriesOnceIntervalElapses()
    {
        // Arrange - a cold start that failed must not permanently mark the mapping as loaded, otherwise a
        // resolver created while the cluster was unreachable never fetches the mapping again.
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        bool serverUnavailable = true;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            if (serverUnavailable)
                throw new InvalidOperationException("Elasticsearch is unavailable");

            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider, _logger);

        Assert.False(resolver.GetMapping("name")?.Found);
        Assert.Equal(1, fetchCount);

        // Act
        serverUnavailable = false;
        timeProvider.Advance(resolver.UnmappedFieldRefreshInterval);

        // Assert
        Assert.True(resolver.GetMapping("name")?.Found);
    }

    [Fact]
    public void NullInstance_WhenResolvingAnyField_ReportsFieldAsUnmapped()
    {
        // Arrange - the shared null resolver never has a mapping, so every field resolves to its own name
        // and callers fall back to the raw field name instead of failing.
        var resolver = ElasticMappingResolver.NullInstance;

        // Act
        var mapping = resolver.GetMapping("name");

        // Assert
        Assert.NotNull(mapping);
        Assert.False(mapping.Found);
        Assert.Equal("name", mapping.FullPath);
    }

    [Fact]
    public void GetMapping_WithoutAnyMappingSource_ThrowsInvalidOperationException()
    {
        // Arrange - a resolver constructed with neither a code mapping nor a server mapping callback is a
        // configuration error and must fail loudly rather than reporting every field as unmapped.
        using var resolver = new ElasticMappingResolver(null!, _inferrer, logger: _logger);

        // Act
        var exception = Record.Exception(() => resolver.GetMapping("name"));

        // Assert
        Assert.IsType<InvalidOperationException>(exception);
    }

    [Theory]
    [InlineData("text", true)]
    [InlineData("keyword", false)]
    [InlineData("long", false)]
    [InlineData("date", false)]
    public void IsPropertyAnalyzed_ForPropertyType_ReportsWhetherFieldIsAnalyzed(string propertyType, bool expected)
    {
        // Arrange
        var properties = new Properties();
        properties.Add("field", propertyType == "text" ? new TextProperty() : CreateProperty(propertyType));
        using var resolver = new ElasticMappingResolver(() => new TypeMapping { Properties = properties }, _inferrer, logger: _logger);

        // Act
        bool analyzed = resolver.IsPropertyAnalyzed("field");

        // Assert
        Assert.Equal(expected, analyzed);
    }

    [Fact]
    public void PropertyTypePredicates_ForMatchingAndNonMatchingFields_ReportPropertyCategory()
    {
        // Arrange
        var properties = new Properties();
        properties.Add("location", new GeoPointProperty());
        properties.Add("count", new LongNumberProperty());
        properties.Add("enabled", new BooleanProperty());
        properties.Add("created", new DateProperty());
        properties.Add("name", new KeywordProperty());
        using var resolver = new ElasticMappingResolver(() => new TypeMapping { Properties = properties }, _inferrer, logger: _logger);

        // Act & Assert
        Assert.True(resolver.IsGeoPropertyType("location"));
        Assert.False(resolver.IsGeoPropertyType("name"));

        Assert.True(resolver.IsNumericPropertyType("count"));
        Assert.False(resolver.IsNumericPropertyType("name"));

        Assert.True(resolver.IsBooleanPropertyType("enabled"));
        Assert.False(resolver.IsBooleanPropertyType("name"));

        Assert.True(resolver.IsDatePropertyType("created"));
        Assert.False(resolver.IsDatePropertyType("name"));
    }

    private static IProperty CreateProperty(string propertyType)
    {
        return propertyType switch
        {
            "keyword" => new KeywordProperty(),
            "date" => new DateProperty(),
            "long" => new LongNumberProperty(),
            "boolean" => new BooleanProperty(),
            "ip" => new IpProperty(),
            _ => throw new ArgumentOutOfRangeException(nameof(propertyType))
        };
    }

    private static void SetMultiFields(IProperty property, Properties fields)
    {
        switch (property)
        {
            case KeywordProperty p: p.Fields = fields; break;
            case DateProperty p: p.Fields = fields; break;
            case LongNumberProperty p: p.Fields = fields; break;
            case BooleanProperty p: p.Fields = fields; break;
            case IpProperty p: p.Fields = fields; break;
            default: throw new ArgumentOutOfRangeException(nameof(property));
        }
    }

    [Fact]
    public void GetMapping_WithCodeSubPropertyUnderServerNestedProperty_ResolvesCodeSubProperty()
    {
        // Arrange - the server mapping only knows the sub fields that have actually been indexed, so
        // code declared sub properties of a nested property must still be merged in.
        var codeMapping = new TypeMapping
        {
            Properties = CreateProperties(("items", new NestedProperty { Properties = CreateProperties(("code_only", new KeywordProperty())) }))
        };
        var serverMapping = new TypeMapping
        {
            Properties = CreateProperties(("items", new NestedProperty { Properties = CreateProperties(("server_only", new KeywordProperty())) }))
        };
        using var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => serverMapping, logger: _logger);

        // Act
        var codeOnly = resolver.GetMapping("items.code_only");
        var serverOnly = resolver.GetMapping("items.server_only");

        // Assert
        Assert.NotNull(codeOnly);
        Assert.True(codeOnly.Found);
        Assert.IsType<KeywordProperty>(codeOnly.Property);
        Assert.NotNull(serverOnly);
        Assert.True(serverOnly.Found);
        Assert.True(resolver.IsNestedPropertyType("items"));
    }

    [Fact]
    public void GetResolvedField_WithPropertyInstanceSharedByMultipleFields_ResolvesEachFieldName()
    {
        // Arrange - reusing a single IProperty instance for multiple fields is legal and used to make
        // every one of those fields resolve to the name of the first one.
        var shared = new KeywordProperty();
        var serverMapping = new TypeMapping { Properties = CreateProperties(("alpha", shared), ("beta", shared)) };
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, logger: _logger);

        // Act
        string? alpha = resolver.GetResolvedField("alpha");
        string? beta = resolver.GetResolvedField("beta");

        // Assert
        Assert.Equal("alpha", alpha);
        Assert.Equal("beta", beta);
    }

    [Fact]
    public void GetResolvedField_WithMissingTextSubField_PreservesOriginalPath()
    {
        // Arrange
        var serverMapping = new TypeMapping { Properties = CreateProperties(("body", new TextProperty())) };
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, logger: _logger);

        // Act
        string? resolved = resolver.GetResolvedField("body.keyword");

        // Assert
        Assert.Equal("body.keyword", resolved);
    }

    [Fact]
    public void GetMapping_WithDifferentFieldCasing_ResolvesCanonicalNameFromEverySpelling()
    {
        // Arrange - resolution falls back to an ordinal ignore case name comparison, so every spelling of a
        // mapped field must resolve to the same canonical path, including repeat lookups that hit the cache.
        var serverMapping = new TypeMapping { Properties = CreateProperties(("MixedCase", new KeywordProperty())) };
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, logger: _logger);

        // Act
        var exact = resolver.GetMapping("MixedCase");
        var lower = resolver.GetMapping("mixedcase");
        var upper = resolver.GetMapping("MIXEDCASE");

        // Assert
        Assert.True(exact?.Found);
        Assert.True(lower?.Found);
        Assert.True(upper?.Found);
        Assert.Equal("MixedCase", exact!.FullPath);
        Assert.Equal("MixedCase", lower!.FullPath);
        Assert.Equal("MixedCase", upper!.FullPath);
        Assert.IsType<KeywordProperty>(exact.Property);
    }

    [Fact]
    public void GetMapping_WithAlias_FollowsToTargetOnFreshAndCachedLookups()
    {
        // Arrange
        var serverMapping = new TypeMapping
        {
            Properties = CreateProperties(
            ("alias", new FieldAliasProperty { Path = "target" }),
            ("target", new KeywordProperty()))
        };
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, logger: _logger);

        // Act
        var followed = resolver.GetMapping("alias", followAlias: true);
        var cachedFollowed = resolver.GetMapping("alias", followAlias: true);
        var unfollowed = resolver.GetMapping("alias");

        // Assert
        Assert.NotNull(followed);
        Assert.True(followed.Found);
        Assert.Equal("target", followed.FullPath);
        Assert.IsType<KeywordProperty>(followed.Property);

        Assert.NotNull(cachedFollowed);
        Assert.True(cachedFollowed.Found);
        Assert.Equal("target", cachedFollowed.FullPath);
        Assert.IsType<KeywordProperty>(cachedFollowed.Property);

        Assert.NotNull(unfollowed);
        Assert.True(unfollowed.Found);
        Assert.IsType<FieldAliasProperty>(unfollowed.Property);
    }

    [Fact]
    public void Dispose_OnNullInstance_IsNoOpAndInstanceRemainsUsable()
    {
        // Arrange - NullInstance is shared process wide, so disposing it must not poison it for other consumers.
        var resolver = ElasticMappingResolver.NullInstance;
        Assert.False(resolver.GetMapping("name")?.Found);

        // Act
        resolver.Dispose();

        // Assert
        Assert.False(resolver.GetMapping("name")?.Found);
    }

    [Fact]
    public void GetMapping_WithSuppressedReloads_LogsAtMostOneWarningPerWarningInterval()
    {
        // Arrange - a flood of lookups against fields missing from a current mapping is throttled; the
        // resulting warnings must be rate limited so query traffic cannot flood the log.
        var capturingLogger = new CapturingLogger();
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider, capturingLogger);

        // A reload interval longer than the warning rate limit window keeps every later miss suppressed,
        // making the warning cadence deterministic.
        resolver.UnmappedFieldRefreshInterval = TimeSpan.FromMinutes(5);

        Assert.True(resolver.GetMapping("name")?.Found);

        // Act - the first miss reloads and arms the throttle; every later miss is suppressed by it.
        Assert.False(resolver.GetMapping("missing1")?.Found);
        resolver.GetMapping("missing2");
        int warningsAfterFirstSuppression = CountUnresolvedFieldWarnings(capturingLogger);

        resolver.GetMapping("missing3");
        timeProvider.Advance(TimeSpan.FromSeconds(45));
        resolver.GetMapping("missing4");

        timeProvider.Advance(TimeSpan.FromSeconds(16));
        resolver.GetMapping("missing5");

        // Assert
        Assert.Equal(2, fetchCount);
        Assert.Equal(1, warningsAfterFirstSuppression);
        Assert.Equal(2, CountUnresolvedFieldWarnings(capturingLogger));
    }

    private static int CountUnresolvedFieldWarnings(CapturingLogger logger)
    {
        return logger.Entries.Count(e => e.Level == MEL.LogLevel.Warning && e.Message.Contains("Unable to resolve mapping for field"));
    }

    private static Properties CreateProperties(params (string Name, IProperty Property)[] properties)
    {
        var props = new Properties();
        foreach ((string name, var property) in properties)
            props.Add(name, property);

        return props;
    }

    private sealed class ZeroOriginTimeProvider : TimeProvider
    {
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(GetTimestamp());

        public void Advance(TimeSpan value) => Interlocked.Add(ref _timestamp, value.Ticks);
    }

    /// <summary>
    /// Builds a mapping containing the original field plus an <c>idx.&lt;name&gt;</c> custom field of the
    /// kind created at runtime by an <c>idx.*</c> dynamic template.
    /// </summary>
    private static TypeMapping CreateDynamicCustomFieldMapping(string existingFieldName, string customFieldName, IProperty customField)
    {
        var mapping = CreateTextWithKeywordMapping(existingFieldName);
        mapping.Properties!.Add("idx", new ObjectProperty { Properties = CreateProperties((customFieldName, customField)) });

        return mapping;
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

    private sealed class BlockingTimeProvider : TimeProvider, IDisposable
    {
        private readonly ManualResetEventSlim _timestampReadBlocked = new(false);
        private readonly ManualResetEventSlim _releaseTimestampRead = new(false);
        private long _timestamp;
        private int _blockNextTimestampRead;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(Volatile.Read(ref _timestamp));

        public override long GetTimestamp()
        {
            if (Interlocked.Exchange(ref _blockNextTimestampRead, 0) == 1)
            {
                _timestampReadBlocked.Set();
                _releaseTimestampRead.Wait(TimeSpan.FromSeconds(30));
            }

            return Volatile.Read(ref _timestamp);
        }

        public void Advance(TimeSpan value) => Interlocked.Add(ref _timestamp, value.Ticks);

        public void BlockNextTimestampRead()
        {
            _timestampReadBlocked.Reset();
            _releaseTimestampRead.Reset();
            Volatile.Write(ref _blockNextTimestampRead, 1);
        }

        public bool WaitUntilBlocked(TimeSpan timeout) => _timestampReadBlocked.Wait(timeout);

        public void ReleaseTimestampRead() => _releaseTimestampRead.Set();

        public void Dispose()
        {
            _releaseTimestampRead.Set();
            _timestampReadBlocked.Dispose();
            _releaseTimestampRead.Dispose();
        }
    }

    private sealed class CapturingLogger : MEL.ILogger
    {
        public List<(MEL.LogLevel Level, string Message)> Entries { get; } = new();
        private readonly object _lock = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(MEL.LogLevel logLevel) => true;

        public void Log<TState>(MEL.LogLevel logLevel, MEL.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lock)
                Entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
