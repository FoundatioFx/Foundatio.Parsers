using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class ProductionQueryContractFixture : ElasticsearchFixture
{
    public string Index { get; } = $"production_contract_{Guid.NewGuid():N}";
    public string DateIndex { get; } = $"production_dates_{Guid.NewGuid():N}";
    public Profile Definition { get; private set; } = null!;

    public override async ValueTask InitializeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        string json = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Compatibility", "production-profile.json"), timeout.Token);
        Definition = JsonSerializer.Deserialize<Profile>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;
        Assert.NotNull(Definition);
        Assert.Equal(1, Definition.Version);
        Assert.Equal(6, Definition.Events.Length);
        Assert.Equal(20, Definition.Boundaries.Length);
        Assert.NotEmpty(Definition.DefaultFields);

        using var mappingStream = new MemoryStream(Encoding.UTF8.GetBytes(Definition.Mapping.GetRawText()));
        var mapping = await Client.RequestResponseSerializer.DeserializeAsync<TypeMapping>(mappingStream, timeout.Token);
        using var settingsStream = new MemoryStream(Encoding.UTF8.GetBytes(Definition.Settings.GetRawText()));
        var settings = await Client.RequestResponseSerializer.DeserializeAsync<IndexSettings>(settingsStream, timeout.Token);

        await InitializeIndexAsync(Index, Definition.Events, mapping, settings, timeout.Token);
        await InitializeIndexAsync(DateIndex, Definition.Boundaries, mapping, settings, timeout.Token);
    }

    private async Task InitializeIndexAsync(string index, Document[] documents, TypeMapping mapping, IndexSettings settings, CancellationToken cancellationToken)
    {
        Assert.All(documents, document => Assert.False(String.IsNullOrWhiteSpace(document.Id)));
        Assert.Equal(documents.Length, documents.Select(document => document.Id).Distinct(StringComparer.Ordinal).Count());
        await CreateIndexAsync(index, descriptor => descriptor.Settings(settings).Mappings(mapping));
        var bulk = await Client.IndexManyAsync(documents, index, cancellationToken);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);
        var refresh = await Client.Indices.RefreshAsync(index, cancellationToken: cancellationToken);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);
        var count = await Client.SearchAsync<Document>(descriptor => descriptor.Indices(index).Query(new Elastic.Clients.Elasticsearch.QueryDsl.MatchAllQuery())
            .Size(documents.Length + 1).TrackTotalHits(true).AllowPartialSearchResults(false), cancellationToken);
        Assert.True(count.IsValidResponse, count.DebugInformation);
        Assert.False(count.TimedOut);
        Assert.Equal(0, count.Shards.Failed);
        Assert.Equal(documents.Length, count.Total);
        Assert.Equal(documents.Length, count.Hits.Count);
        Assert.Equal(documents.Select(document => document.Id).Order(StringComparer.Ordinal), count.Hits.Select(hit => hit.Id).Order(StringComparer.Ordinal));
    }

    public sealed class Profile
    {
        public int Version { get; set; }
        public string[] DefaultFields { get; set; } = [];
        public Dictionary<string, string> FieldMap { get; set; } = [];
        public JsonElement Settings { get; set; }
        public JsonElement Mapping { get; set; }
        public Document[] Events { get; set; } = [];
        public Document[] Boundaries { get; set; } = [];
    }

    public sealed class Document
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = String.Empty;

        // Preserve arrays, explicit nulls, omitted values, and nanosecond timestamps without DateTime rounding.
        [JsonExtensionData]
        public Dictionary<string, JsonElement> Fields { get; set; } = [];
    }
}
