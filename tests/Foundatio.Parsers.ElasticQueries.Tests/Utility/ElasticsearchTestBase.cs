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

    protected void CreateNamedIndex<TModel>(string index, Func<TypeMappingDescriptor<TModel>, TypeMapping> configureMappings = null, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> configureIndex = null) where TModel : class
    {
        _fixture.CreateNamedIndex(index, configureMappings, configureIndex);
    }

    protected string CreateRandomIndex<TModel>(Func<TypeMappingDescriptor<TModel>, TypeMapping> configureMappings = null, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> configureIndex = null) where TModel : class
    {
        return _fixture.CreateRandomIndex(configureMappings, configureIndex);
    }

    protected CreateIndexResponse CreateIndex(IndexName index, Func<CreateIndexDescriptor, ICreateIndexRequest> configureIndex = null)
    {
        return _fixture.CreateIndex(index, configureIndex);
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

    public void CreateNamedIndex<T>(string index, Func<TypeMappingDescriptor<T>, TypeMapping> configureMappings = null, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> configureIndex = null) where T : class
    {
        if (configureMappings == null)
            configureMappings = m => m.AutoMap<T>().Dynamic();
        if (configureIndex == null)
            configureIndex = i => i.NumberOfReplicas(0).Analysis(a => a.AddSortNormalizer());

        CreateIndex(index, i => i.Settings(configureIndex).Map<T>(configureMappings));
        Client.ElasticsearchClientSettings.DefaultIndices[typeof(T)] = index;
    }

    public string CreateRandomIndex<T>(Func<TypeMappingDescriptor<T>, TypeMapping> configureMappings = null, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> configureIndex = null) where T : class
    {
        string index = "test_" + Guid.NewGuid().ToString("N");
        if (configureMappings == null)
            configureMappings = m => m.AutoMap<T>().Dynamic();
        if (configureIndex == null)
            configureIndex = i => i.NumberOfReplicas(0).Analysis(a => a.AddSortNormalizer());

        CreateIndex(index, i => i.Settings(configureIndex).Map<T>(configureMappings));
        Client.ElasticsearchClientSettings.DefaultIndices[typeof(T)] = index;

        return index;
    }

    public CreateIndexResponse CreateIndex(IndexName index, Func<CreateIndexDescriptor, ICreateIndexRequest> configureIndex = null)
    {
        _createdIndexes.Add(index);

        if (configureIndex == null)
            configureIndex = d => d.Settings(s => s.NumberOfReplicas(0));

        var result = Client.Indices.Create(index, configureIndex);
        if (!result.IsValid)
            throw new ApplicationException($"Unable to create index {index}: " + result.DebugInformation);

        return result;
    }

    protected virtual void CleanupTestIndexes(ElasticsearchClient client)
    {
        if (_createdIndexes.Count > 0)
            client.Indices.Delete(Indices.Index(_createdIndexes));
    }

    public virtual Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public virtual Task DisposeAsync()
    {
        CleanupTestIndexes(Client);
        return Task.CompletedTask;
    }
}
