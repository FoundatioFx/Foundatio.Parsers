using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
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
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), _inferrer, () => null, logger: _logger);

        string result = resolver.GetNonAnalyzedFieldName("title", "keyword");

        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetAggregationsFieldName_WithTextPropertyAndKeywordSubField_ReturnsKeywordPath()
    {
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("title"), _inferrer, () => null, logger: _logger);

        string result = resolver.GetAggregationsFieldName("title");

        Assert.Equal("title.keyword", result);
    }

    [Fact]
    public void GetSortFieldName_WithTextPropertyAndSortSubField_ReturnsSortPath()
    {
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordAndSortMapping("title"), _inferrer, () => null, logger: _logger);

        string result = resolver.GetSortFieldName("title");

        Assert.Equal("title.sort", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithKeywordProperty_ReturnsBareFieldName()
    {
        var props = new Properties();
        props.Add("status", new KeywordProperty());
        var codeMapping = new TypeMapping { Properties = props };
        var resolver = new ElasticMappingResolver(codeMapping, _inferrer, () => null, logger: _logger);

        string result = resolver.GetNonAnalyzedFieldName("status", "keyword");

        Assert.Equal("status", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithTextPropertyWithoutSubFields_ReturnsBareFieldName()
    {
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("body"), _inferrer, () => null, logger: _logger);

        string result = resolver.GetNonAnalyzedFieldName("body", "keyword");

        Assert.Equal("body", result);
    }

    [Fact]
    public void RefreshMapping_WhenCalled_ClearsCachedMappings()
    {
        int serverFetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            int callNumber = Interlocked.Increment(ref serverFetchCount);
            return callNumber <= 1
                ? CreateTextOnlyMapping("name")
                : CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        string beforeRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string afterRefresh = resolver.GetNonAnalyzedFieldName("name", "keyword");

        Assert.Equal("name", beforeRefresh);
        Assert.Equal("name.keyword", afterRefresh);
        Assert.True(serverFetchCount >= 2, "Server mapping should have been fetched at least twice");
    }

    [Fact]
    public void RefreshMapping_ClearsFoundCacheEntries_ForcesReResolution()
    {
        int serverFetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            int callNumber = Interlocked.Increment(ref serverFetchCount);
            return callNumber == 1
                ? CreateTextOnlyMapping("name")
                : CreateTextWithKeywordMapping("name");
        }, _inferrer, logger: _logger);

        string first = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string second = resolver.GetNonAnalyzedFieldName("name", "keyword");

        Assert.Equal("name", first);
        Assert.Equal("name.keyword", second);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_WithCodeAndServerMerge_ReturnsKeywordSubField()
    {
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("name"), _inferrer,
            () => CreateTextOnlyMapping("name"), logger: _logger);
        resolver.RefreshMapping();

        string result = resolver.GetNonAnalyzedFieldName("name", "keyword");

        Assert.Equal("name.keyword", result);
    }

    [Fact]
    public void GetNonAnalyzedFieldName_AfterRefreshAndServerMappingChange_ReturnsUpdatedKeywordPath()
    {
        int callCount = 0;
        var resolver = new ElasticMappingResolver(
            CreateTextOnlyMapping("name"), _inferrer, () =>
            {
                int callNumber = Interlocked.Increment(ref callCount);
                return callNumber <= 1 ? null : CreateTextWithKeywordMapping("name");
            }, logger: _logger);

        string initial = resolver.GetNonAnalyzedFieldName("name", "keyword");
        resolver.RefreshMapping();
        string updated = resolver.GetNonAnalyzedFieldName("name", "keyword");

        Assert.Equal("name", initial);
        Assert.Equal("name.keyword", updated);
    }

    [Fact]
    public async Task ConcurrentGetMappingAndRefreshMapping_UnderContention_AlwaysReturnsKeywordPath()
    {
        var resolver = new ElasticMappingResolver(
            CreateTextWithKeywordMapping("name"), _inferrer, () =>
            {
                Thread.Yield();
                return CreateTextWithKeywordMapping("name");
            }, logger: _logger);
        const int iterations = 200;
        using var barrier = new Barrier(3);

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

        await Task.WhenAll(readerTask, aggregationReaderTask, refreshTask);
    }

    [Fact]
    public void GetServerMapping_WhenThrottled_DoesNotRefetchWithinOneMinute()
    {
        var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);
        int fetchCount = 0;
        var resolver = new ElasticMappingResolver(() =>
        {
            Interlocked.Increment(ref fetchCount);
            return CreateTextWithKeywordMapping("name");
        }, _inferrer, timeProvider: timeProvider, logger: _logger);

        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterFirst = fetchCount;
        Assert.True(afterFirst >= 1, "First call should trigger at least one fetch");

        resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal(afterFirst, fetchCount);

        timeProvider.Advance(TimeSpan.FromSeconds(30));
        resolver.GetNonAnalyzedFieldName("unknown_field", "keyword");
        Assert.Equal(afterFirst, fetchCount);

        resolver.RefreshMapping();
        resolver.GetNonAnalyzedFieldName("name", "keyword");
        int afterRefresh = fetchCount;
        Assert.True(afterRefresh > afterFirst, "Refresh should allow a new fetch");

        resolver.GetNonAnalyzedFieldName("name", "keyword");
        Assert.Equal(afterRefresh, fetchCount);

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
}
