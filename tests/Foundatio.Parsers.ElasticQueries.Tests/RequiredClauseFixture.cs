using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class RequiredClauseFixture : ElasticsearchFixture
{
    public string Index { get; } = $"required_{Guid.NewGuid():N}";

    public static TypeMapping Mapping => new()
    {
        Dynamic = DynamicMapping.Strict,
        Properties = new Properties
        {
            { "id", new KeywordProperty() },
            { "tags", new KeywordProperty() },
            { "children", new NestedProperty
                {
                    Properties = new Properties
                    {
                        { "name", new KeywordProperty() },
                        { "value", new KeywordProperty() }
                    }
                }
            }
        }
    };

    public override async ValueTask InitializeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        Child[][] children =
        [
            [],
            [new("a", "x"), new("b", "y")],
            [new("b", "y")],
            [new("a", "y"), new("b", "x")],
            [],
            [new("a", "x")],
            [new("b", "y")],
            [new("a", "y")]
        ];
        var documents = Enumerable.Range(0, 8).Select(bits => new Document
        {
            Id = bits.ToString(CultureInfo.InvariantCulture),
            Tags = new[] { "a", "b", "c" }.Where((_, bit) => (bits & (1 << bit)) != 0).ToArray(),
            Children = children[bits]
        }).ToArray();

        await CreateIndexAsync(Index, descriptor => descriptor
            .Settings(settings => settings.NumberOfShards(1).NumberOfReplicas(0))
            .Mappings(Mapping));
        var bulk = await Client.IndexManyAsync(documents, Index, timeout.Token);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);
        var refresh = await Client.Indices.RefreshAsync(Index, cancellationToken: timeout.Token);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);
    }

    public sealed class Document
    {
        public string Id { get; set; } = String.Empty;
        public string[] Tags { get; set; } = [];
        public Child[] Children { get; set; } = [];
    }

    public sealed record Child(string Name, string Value);
}
