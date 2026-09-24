using System;
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
        TermTranslationDocument[] documents =
        [
            CreateDocument("a", "john"),
            CreateDocument("b", "joan"),
            CreateDocument("c", "jo?n"),
            CreateDocument("d", "johnny"),
            CreateDocument("e", "john*"),
            CreateDocument("f", "jo?nny"),
            CreateDocument("g", "JOHN"),
            CreateDocument("h", "foo:bar"),
            CreateDocument("i", "foo:barista"),
            CreateDocument("j", "foo:bar*"),
            CreateDocument("k", "alpha beta"),
            CreateDocument("l", "alpha:beta", text: "alpha gamma beta"),
            CreateDocument("m", @"path\name"),
            CreateDocument("n", "path/name"),
            CreateDocument("o", String.Empty),
            CreateDocument("p", null, otherText: "john")
        ];

        await CreateIndexAsync(Index, descriptor => descriptor
            .Settings(settings => settings.NumberOfShards(1).NumberOfReplicas(0))
            .Mappings(Mapping));
        var bulk = await Client.IndexManyAsync(documents, Index, timeout.Token);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);

        var refresh = await Client.Indices.RefreshAsync(Index, cancellationToken: timeout.Token);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);
    }

    private static TermTranslationDocument CreateDocument(string id, string? value, string? text = null, string? otherText = null) => new()
    {
        Id = id,
        Keyword = value,
        Text = text ?? value,
        OtherText = otherText,
        Children = value is null ? [] : [new NestedTermValue(value, text ?? value)]
    };

    public sealed record TermTranslationDocument
    {
        public required string Id { get; init; }
        public string? Keyword { get; init; }
        public string? Text { get; init; }
        public string? OtherText { get; init; }
        public NestedTermValue[] Children { get; init; } = [];
    }

    public sealed record NestedTermValue(string? Keyword, string? Text);
}
