using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class TermTranslationFixture : ElasticsearchFixture
{
    public string Index { get; } = $"term_translation_{Guid.NewGuid():N}";

    public static TypeMapping Mapping => new()
    {
        Dynamic = DynamicMapping.Strict,
        Properties = new Properties
        {
            { "id", new KeywordProperty() },
            { "keyword", new KeywordProperty() },
            { "text", new TextProperty { Analyzer = "standard" } },
            { "otherText", new TextProperty { Analyzer = "standard" } },
            { "children", new NestedProperty
                {
                    Properties = new Properties
                    {
                        { "keyword", new KeywordProperty() },
                        { "text", new TextProperty { Analyzer = "standard" } }
                    }
                }
            }
        }
    };

    public override async ValueTask InitializeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        string?[] values = ["john", "joan", "jo?n", "johnny", "john*", "jo?nny", "JOHN", "foo:bar", "foo:barista", "foo:bar*", "alpha beta", "alpha:beta", "path\\name", "path/name", "", null];
        var documents = values.Select((value, index) => new Document
        {
            Id = ((char)('a' + index)).ToString(CultureInfo.InvariantCulture),
            Keyword = value,
            Text = index == 11 ? "alpha gamma beta" : value,
            OtherText = index == 15 ? "john" : null,
            Children = value is null ? [] : [new Child(value, index == 11 ? "alpha gamma beta" : value)]
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
        public string? Keyword { get; set; }
        public string? Text { get; set; }
        public string? OtherText { get; set; }
        public Child[] Children { get; set; } = [];
    }

    public sealed record Child(string? Keyword, string? Text);
}
