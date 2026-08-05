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
    public void GetMapping_WithResolvedFieldWithinRefreshInterval_DoesNotRefetchServerMapping()
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
    public void RefreshMapping_WithinRefreshInterval_BypassesThrottleAndRefetchesServerMapping()
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
    public void GetMapping_WithRepeatedlyUnresolvableFields_BacksOffServerMappingReloads()
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
        resolver.GetMapping("missing_1");
        Assert.Equal(2, fetchCount);

        // Act + Assert - a reload that did not resolve the field doubles the interval to 10s
        timeProvider.Advance(TimeSpan.FromSeconds(9));
        resolver.GetMapping("missing_2");
        Assert.Equal(2, fetchCount);

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        resolver.GetMapping("missing_3");
        Assert.Equal(3, fetchCount);

        // Act + Assert - and again to 20s
        timeProvider.Advance(TimeSpan.FromSeconds(19));
        resolver.GetMapping("missing_4");
        Assert.Equal(3, fetchCount);

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        resolver.GetMapping("missing_5");
        Assert.Equal(4, fetchCount);
    }

    [Fact]
    public void GetMapping_WithFieldResolvedByReload_ResetsUnmappedRefreshBackoff()
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

        resolver.GetMapping("name");
        resolver.GetMapping("missing_1");
        resolver.GetMapping("missing_2");
        int afterBackoff = fetchCount;

        // Act - the field now exists, so the reload that finds it must reset the backoff
        timeProvider.Advance(TimeSpan.FromSeconds(10));
        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        var created = resolver.GetMapping("idx.keyword-000001");

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        resolver.GetMapping("missing_3");

        // Assert
        Assert.NotNull(created);
        Assert.True(created.Found);
        Assert.Equal(afterBackoff + 2, fetchCount);
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
    public void InvalidateFieldMapping_WithCachedMissForNewlyCreatedField_ResolvesFieldWithoutWaitingForThrottle()
    {
        // Arrange - arm the unmapped field throttle so the cached miss cannot refresh itself
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() => serverMapping, _inferrer, timeProvider: timeProvider, logger: _logger);

        Assert.False(resolver.GetMapping("name.missing")!.Found);
        Assert.False(resolver.GetMapping("idx.keyword-000001")!.Found);

        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());
        Assert.False(resolver.GetMapping("idx.keyword-000001")!.Found);

        // Act
        resolver.InvalidateFieldMapping("idx.keyword-000001");
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

        // Assert - one cold start attempt plus one miss driven attempt, then throttled
        Assert.Equal(2, fetchCount);
    }

    [Fact]
    public async Task GetMapping_WithConcurrentUnmappedLookups_FetchesServerMappingOnce()
    {
        // Arrange
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            Thread.Sleep(100);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        resolver.GetMapping("name");
        int afterColdStart = fetchCount;

        // Act
        var lookups = Enumerable.Range(0, 16)
            .Select(_ => Task.Run(() => resolver.GetMapping("idx.keyword-000001")))
            .ToArray();
        await Task.WhenAll(lookups);

        // Assert
        Assert.Equal(1, afterColdStart);
        Assert.Equal(afterColdStart + 1, fetchCount);
        Assert.All(lookups, t => Assert.False(t.Result!.Found));
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
    public async Task GetMapping_WithServerMappingFetchSlowerThanFiveSeconds_ResolvesFieldForAllConcurrentLookups()
    {
        // Arrange - the join timeout must comfortably exceed the mapping fetch latency, otherwise concurrent
        // lookups give up on the very fetch that would have resolved their field and silently treat it as
        // unmapped. Six seconds would have exceeded the previously hard coded five second join timeout.
        int fetchCount = 0;
        var serverMapping = CreateTextWithKeywordMapping("name");
        using var resolver = new ElasticMappingResolver(() =>
        {
            if (Interlocked.Increment(ref fetchCount) > 1)
                Thread.Sleep(TimeSpan.FromSeconds(6));

            return serverMapping;
        }, _inferrer, logger: _logger);

        resolver.GetMapping("name");
        serverMapping = CreateDynamicCustomFieldMapping("name", "keyword-000001", new KeywordProperty());

        // Act
        var lookups = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() => resolver.GetMapping("idx.keyword-000001")))
            .ToArray();
        await Task.WhenAll(lookups);

        // Assert - one fetch served them all and every caller got the real mapping
        Assert.Equal(2, fetchCount);
        Assert.All(lookups, t =>
        {
            Assert.NotNull(t.Result);
            Assert.True(t.Result!.Found);
            Assert.Equal("idx.keyword-000001", t.Result.FullPath);
        });
    }

    [Fact]
    public void MappingRefreshWaitTimeout_ByDefault_ExceedsTypicalMappingFetchLatency()
    {
        // Arrange + Act
        using var resolver = new ElasticMappingResolver(() => CreateTextWithKeywordMapping("name"), _inferrer, logger: _logger);

        // Assert - waiting for an in-flight fetch is never more expensive than performing it, so the default
        // must leave plenty of headroom above a normal get mapping round trip
        Assert.Equal(TimeSpan.FromSeconds(30), resolver.MappingRefreshWaitTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), resolver.UnmappedFieldRefreshInterval);
        Assert.Equal(TimeSpan.FromMinutes(1), resolver.MappingRefreshInterval);
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
    public void MaxCachedFields_WhenExceededByUnmappedFields_EvictsMissesAndKeepsResolvedFields()
    {
        // Arrange - field names come from user supplied queries, so a caller probing many non-existent
        // fields must not be able to displace the resolutions real queries depend on.
        var properties = new Properties();
        properties.Add("name", new KeywordProperty());
        properties.Add("status", new KeywordProperty());
        properties.Add("created", new DateProperty());
        properties.Add("count", new LongNumberProperty());
        string[] realFields = ["name", "status", "created", "count"];

        using var resolver = new ElasticMappingResolver(() => new TypeMapping { Properties = properties }, _inferrer,
            new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);
        resolver.MaxCachedFields = 8;
        resolver.UnmappedFieldRefreshInterval = TimeSpan.FromHours(1);

        // Arm both reload throttles before caching anything worth keeping. The first lookup triggers the
        // cold start load and the second arms the separate unmapped field throttle, so the flood below
        // exercises cache eviction rather than snapshot replacement.
        Assert.False(resolver.GetMapping("warmup1")?.Found);
        Assert.False(resolver.GetMapping("warmup2")?.Found);

        foreach (string realField in realFields)
            Assert.True(resolver.GetMapping(realField)?.Found);

        // Act
        for (int i = 0; i < 100; i++)
            resolver.GetMapping($"missing{i}");

        long countAfterFlood = resolver.CachedFieldCount;
        foreach (string realField in realFields)
            Assert.True(resolver.GetMapping(realField)?.Found);

        // Assert - re-resolving the real fields added nothing, so they were still cached.
        Assert.Equal(countAfterFlood, resolver.CachedFieldCount);
        Assert.True(countAfterFlood <= resolver.MaxCachedFields,
            $"Expected cache to stay bounded but it held {countAfterFlood} fields.");
    }

    [Fact]
    public void MaxCachedFields_WhenSetToZero_DisablesCachingWithoutBreakingResolution()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => CreateTextWithKeywordMapping("name"), _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);
        resolver.MaxCachedFields = 0;

        // Act
        var first = resolver.GetMapping("name");
        var second = resolver.GetMapping("name");

        // Assert
        Assert.True(first?.Found);
        Assert.True(second?.Found);
        Assert.Equal(0, resolver.CachedFieldCount);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-60)]
    public void MappingRefreshInterval_WithNegativeValue_ThrowsArgumentOutOfRangeException(int seconds)
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(() => null, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        var exception = Record.Exception(() => resolver.MappingRefreshInterval = TimeSpan.FromSeconds(seconds));

        // Assert
        var argumentException = Assert.IsType<ArgumentOutOfRangeException>(exception);
        Assert.Equal("value", argumentException.ParamName);
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
    public void UnmappedFieldRefreshInterval_WithZero_AlwaysAllowsReload()
    {
        // Arrange - zero means "never throttle", which must be accepted rather than treated as invalid.
        int callCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            callCount++;
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);
        resolver.UnmappedFieldRefreshInterval = TimeSpan.Zero;

        Assert.True(resolver.GetMapping("name")?.Found);
        int callsAfterColdStart = callCount;

        // Act
        resolver.GetMapping("missing1");
        resolver.GetMapping("missing2");

        // Assert
        Assert.True(callCount > callsAfterColdStart + 1,
            $"Expected each unmapped lookup to reload but only {callCount - callsAfterColdStart} reloads occurred.");
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
    public void MappingRefreshWaitTimeout_WithInfiniteTimeSpan_IsAccepted()
    {
        // Arrange - waiting forever is safe when the fetch callback enforces its own timeout, and
        // Timeout.InfiniteTimeSpan is the idiomatic way to express it.
        using var resolver = new ElasticMappingResolver(() => CreateTextWithKeywordMapping("name"), _inferrer, new FakeTimeProvider(DateTimeOffset.UtcNow), _logger);

        // Act
        resolver.MappingRefreshWaitTimeout = Timeout.InfiniteTimeSpan;

        // Assert
        Assert.Equal(Timeout.InfiniteTimeSpan, resolver.MappingRefreshWaitTimeout);
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
    public async Task GetMapping_WithConcurrentLookupsOfSameUnmappedField_BacksOffOncePerReload()
    {
        // Arrange - every one of these lookups adopts the result of a single reload, so the backoff must
        // only advance one step rather than being ratcheted to the ceiling by the burst.
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        using var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        resolver.GetMapping("name");
        int afterColdStart = fetchCount;

        // Act
        await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(() => resolver.GetMapping("idx.keyword-000001"))));
        int afterBurst = fetchCount;

        timeProvider.Advance(TimeSpan.FromSeconds(10));
        resolver.GetMapping("idx.keyword-000002");

        // Assert - one reload for the burst, then one more after a single 10s backoff step
        Assert.Equal(1, afterColdStart);
        Assert.Equal(2, afterBurst);
        Assert.Equal(3, fetchCount);
    }

    [Fact]
    public void GetMapping_WithMoreDistinctFieldsThanMaxCachedFields_BoundsCachedFieldCount()
    {
        // Arrange
        using var resolver = new ElasticMappingResolver(CreateTextWithKeywordMapping("name"), _inferrer, () => null, logger: _logger)
        {
            MaxCachedFields = 16
        };

        // Act
        for (int i = 0; i < 500; i++)
            resolver.GetMapping($"unknown_field_{i}");

        // Assert
        Assert.True(resolver.CachedFieldCount <= 16, $"Expected at most 16 cached fields but found {resolver.CachedFieldCount}");
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

    private static Properties CreateProperties(params (string Name, IProperty Property)[] properties)
    {
        var props = new Properties();
        foreach ((string name, var property) in properties)
            props.Add(name, property);

        return props;
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
}
