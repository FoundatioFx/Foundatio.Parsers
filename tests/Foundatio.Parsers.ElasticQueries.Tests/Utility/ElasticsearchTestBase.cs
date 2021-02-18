using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Xunit;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests {
    public abstract class ElasticsearchTestBase : TestWithLoggingBase, IAsyncLifetime {
        private readonly List<IndexName> _createdIndexes = new List<IndexName>();
        private static bool _elaticsearchReady;

        protected ElasticsearchTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        }

        protected IElasticClient GetClient(Action<ConnectionSettings> configure = null) {
            string elasticsearchUrl = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL") ?? "http://localhost:9200";
            var settings = new ConnectionSettings(new Uri(elasticsearchUrl));
            configure?.Invoke(settings);

            var client = new ElasticClient(settings.DisableDirectStreaming().PrettyJson());

            if (!_elaticsearchReady) {
                if (!client.WaitForReady(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token, _logger))
                    throw new ApplicationException("Unable to connect to Elasticsearch.");

                _elaticsearchReady = true;
            }

            return client;
        }

        protected string CreateRandomIndex<T>(IElasticClient client, Func<TypeMappingDescriptor<T>, ITypeMapping> configureMappings = null, Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> configureIndex = null) where T : class {
            string index = "test_" + Guid.NewGuid().ToString("N");
            if (configureMappings == null)
                configureMappings = m => m.AutoMap<T>().Dynamic();
            if (configureIndex == null)
                configureIndex = i => i.NumberOfReplicas(0).Analysis(a => a.AddSortNormalizer());

            CreateIndex(client, index, i => i.Settings(configureIndex).Map<T>(configureMappings));
            client.ConnectionSettings.DefaultIndices.Add(typeof(T), index);

            return index;
        }

        protected CreateIndexResponse CreateIndex(IElasticClient client, IndexName index, Func<CreateIndexDescriptor, ICreateIndexRequest> configureIndex = null) {
            _createdIndexes.Add(index);

            if (configureIndex == null)
                configureIndex = d => d.Settings(s => s.NumberOfReplicas(0));
            
            var result = client.Indices.Create(index, configureIndex);
            if (!result.IsValid)
                throw new ApplicationException($"Unable to create index {index}");

            return result;
        }

        public virtual async Task InitializeAsync() {
            var client = GetClient();
            var indices = await client.Indices.GetAsync(Indices.All);
            var testIndices = indices.Indices.Where(i => i.Key.Name.StartsWith("test_")).Select(i => i.Key).ToArray();
            if (testIndices.Length > 0)
                await client.Indices.DeleteAsync(Indices.Index(testIndices));
        }

        public virtual async Task DisposeAsync() {
            if (_createdIndexes.Count == 0)
                return;
            
            var client = GetClient();
            await client.Indices.DeleteAsync(Indices.Index(_createdIndexes));
        }
    }
}