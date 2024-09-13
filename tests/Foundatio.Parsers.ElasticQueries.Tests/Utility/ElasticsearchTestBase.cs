using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public abstract class ElasticsearchTestBase : ElasticsearchTestBase<ElasticsearchFixture>
{
    protected ElasticsearchTestBase(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture) { }
}

public abstract class ElasticsearchTestBase<T> : TestWithLoggingBase, IAsyncLifetime, IClassFixture<T> where T : ElasticsearchFixture
{
    private readonly T _fixture;

    public ElasticsearchTestBase(ITestOutputHelper output, T fixture) : base(output)
    {
        _fixture = fixture;
        _fixture.Log = Log;
    }

    protected ElasticsearchClient Client => _fixture.Client;

    protected Task CreateNamedIndexAsync<TModel>(string index, Func<TypeMappingDescriptor<TModel>, TypeMappingDescriptor<TModel>> configureMappings = null, Func<IndexSettingsDescriptor, IndexSettingsDescriptor> configureIndex = null) where TModel : class
    {
        return _fixture.CreateNamedIndexAsync(index, configureMappings, configureIndex);
    }

    protected Task<string> CreateRandomIndexAsync<TModel>(Func<TypeMappingDescriptor<TModel>, TypeMappingDescriptor<TModel>> configureMappings = null, Func<IndexSettingsDescriptor, IndexSettingsDescriptor> configureIndex = null) where TModel : class
    {
        return _fixture.CreateRandomIndexAsync(configureMappings, configureIndex);
    }

    protected Task<CreateIndexResponse> CreateIndexAsync(IndexName index, Action<CreateIndexRequestDescriptor> configureIndex = null)
    {
        return _fixture.CreateIndexAsync(index, configureIndex);
    }

    /// <summary>
    /// per test setup
    /// </summary>
    public virtual Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// per test tear down
    /// </summary>
    public virtual Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}

public class ElasticsearchFixture : IAsyncLifetime
{
    private readonly List<IndexName> _createdIndexes = new();
    private static bool _elaticsearchReady;
    protected readonly ILogger _logger;
    private readonly Lazy<ElasticsearchClient> _client;

    public ElasticsearchFixture()
    {
        _client = new Lazy<ElasticsearchClient>(() => GetClient(ConfigureConnectionSettings));
    }

    public TestLogger Log { get; set; }
    public ElasticsearchClient Client => _client.Value;

    protected ElasticsearchClient GetClient(Action<ElasticsearchClientSettings> configure = null)
    {
        string elasticsearchUrl = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL") ?? "http://localhost:9200";
        var settings = new ElasticsearchClientSettings(new Uri(elasticsearchUrl));
        configure?.Invoke(settings);

        var client = new ElasticsearchClient(settings.DisableDirectStreaming().PrettyJson());

        if (!_elaticsearchReady)
        {
            if (!client.WaitForReady(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token, _logger))
                throw new ApplicationException("Unable to connect to Elasticsearch.");

            _elaticsearchReady = true;
        }

        return client;
    }

    protected virtual void ConfigureConnectionSettings(ElasticsearchClientSettings settings) { }

    public async Task CreateNamedIndexAsync<T>(string index, Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> configureMappings = null, Func<IndexSettingsDescriptor, IndexSettingsDescriptor> configureIndex = null) where T : class
    {
        configureMappings ??= m => m.AutoMap<T>().Dynamic(DynamicMapping.True);
        configureIndex ??= i => i.NumberOfReplicas(0).Analysis(a => a.AddSortNormalizer());

        await CreateIndexAsync(index, i => i.Settings(configureIndex(new IndexSettingsDescriptor())).Map<T>(configureMappings));
        Client.ElasticsearchClientSettings.DefaultIndices[typeof(T)] = index;
    }

    public async Task<string> CreateRandomIndexAsync<T>(Func<TypeMappingDescriptor<T>, TypeMappingDescriptor<T>> configureMappings = null, Func<IndexSettingsDescriptor, IndexSettingsDescriptor> configureIndex = null) where T : class
    {
        string index = $"test_{Guid.NewGuid():N}";
        configureMappings ??= m => m.AutoMap<T>().Dynamic(DynamicMapping.True);
        configureIndex ??= i => i.NumberOfReplicas(0).Analysis(a => a.AddSortNormalizer());

        await CreateIndexAsync(index, i => i.Settings(configureIndex(new IndexSettingsDescriptor())).Map<T>(configureMappings));
        Client.ElasticsearchClientSettings.DefaultIndices[typeof(T)] = index;

        return index;
    }

    public async Task<CreateIndexResponse> CreateIndexAsync(IndexName index, Action<CreateIndexRequestDescriptor> configureIndex = null)
    {
        _createdIndexes.Add(index);

        configureIndex ??= d => d.Settings(s => s.NumberOfReplicas(0));
        var result = await Client.Indices.CreateAsync(index, configureIndex);
        if (!result.IsValidResponse)
            throw new ApplicationException($"Unable to create index {index}: " + result.DebugInformation);

        return result;
    }

    protected virtual async Task CleanupTestIndexesAsync(ElasticsearchClient client)
    {
        if (_createdIndexes.Count > 0)
            await client.Indices.DeleteAsync(Indices.Index(_createdIndexes));
    }

    public virtual Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public virtual Task DisposeAsync()
    {
        return CleanupTestIndexesAsync(Client);
    }
}
