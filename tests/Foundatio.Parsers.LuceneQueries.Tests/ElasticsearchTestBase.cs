using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {
    public abstract class ElasticsearchTestBase : TestWithLoggingBase, IAsyncLifetime {
        private readonly List<IndexName> _createdIndexes = new List<IndexName>();
        
        public ElasticsearchTestBase(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        }

        protected IElasticClient GetClient(Action<ConnectionSettings> configure = null) {
            var elasticsearchUrl = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL") ?? "http://localhost:9200";
            var settings = new ConnectionSettings(new Uri(elasticsearchUrl));
            configure?.Invoke(settings);

            var client = new ElasticClient(settings.DisableDirectStreaming().PrettyJson());
            if (!client.WaitForReady(new CancellationTokenSource(TimeSpan.FromMinutes(1)).Token, _logger))
                throw new ApplicationException("Unable to connect to Elasticsearch.");

            return client;
        }

        protected string CreateRandomIndex(Func<CreateIndexDescriptor, ICreateIndexRequest> selector = null) {
            return CreateRandomIndex(GetClient(), selector);
        }

        protected string CreateRandomIndex(IElasticClient client, Func<CreateIndexDescriptor, ICreateIndexRequest> selector = null) {
            var index = "test_" + Guid.NewGuid().ToString("N");
            var result = CreateIndex(client, index, selector);
            if (!result.IsValid)
                throw new ApplicationException("Unable to create index.");

            return index;
        }

        protected ICreateIndexResponse CreateIndex(IndexName index, Func<CreateIndexDescriptor, ICreateIndexRequest> selector = null) {
            var client = GetClient();
            return CreateIndex(client, index, selector);
        }

        protected ICreateIndexResponse CreateIndex(IElasticClient client, IndexName index, Func<CreateIndexDescriptor, ICreateIndexRequest> selector = null) {
            _createdIndexes.Add(index);
            
            // set replicas to 0
            var originalSelector = selector;
            selector = d => {
                originalSelector?.Invoke(d);
                d.Settings(s => s.NumberOfReplicas(0));
                return d;
            };
            var result = client.CreateIndex(index, selector);
            if (!result.IsValid)
                throw new ApplicationException($"Unable to create index {index}");

            return result;
        }

        public virtual async Task InitializeAsync() {
            var client = GetClient();
            var indices = await client.GetIndexAsync(Indices.All);
            var testIndices = indices.Indices.Where(i => i.Key.Name.StartsWith("test_")).Select(i => i.Key).ToArray();
            if (testIndices.Length > 0)
                await client.DeleteIndexAsync(Indices.Index(testIndices));
        }

        public virtual async Task DisposeAsync() {
            if (_createdIndexes.Count == 0)
                return;
            
            var client = GetClient();
            await client.DeleteIndexAsync(Indices.Index(_createdIndexes));
        }
    }
}