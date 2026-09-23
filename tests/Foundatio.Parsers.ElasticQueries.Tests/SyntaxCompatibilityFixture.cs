using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class SyntaxCompatibilityFixture : ElasticsearchFixture
{
    public string Index { get; } = $"compatibility_{Guid.NewGuid():N}";

    public static TypeMapping Mapping => new()
    {
        Dynamic = DynamicMapping.Strict,
        Properties = new Properties
        {
            { "id", new KeywordProperty() },
            { "text", new TextProperty { Analyzer = "standard" } },
            { "otherText", new TextProperty { Analyzer = "standard" } },
            { "keyword", new KeywordProperty() },
            { "number", new IntegerNumberProperty() },
            { "date", new DateProperty() },
            { "dateNanos", new DateNanosProperty() }
        }
    };

    public override async ValueTask InitializeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        Document[] documents =
        [
            new() { Id = "a", Text = "alpha beta", OtherText = "gamma", Keyword = "john", Number = 1, Date = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "b", Text = "alpha gamma beta", OtherText = "beta", Keyword = "joan", Number = 3, Date = new DateTimeOffset(2024, 1, 1, 5, 59, 59, TimeSpan.Zero) },
            new() { Id = "c", Text = "beta gamma", OtherText = "alpha", Keyword = "jo?n", Number = 5, Date = new DateTimeOffset(2024, 1, 1, 6, 0, 0, TimeSpan.Zero) },
            new() { Id = "d", Text = "alphabet soup", OtherText = "beta", Keyword = "johnny", Number = 9, Date = new DateTimeOffset(2024, 1, 2, 6, 0, 0, TimeSpan.Zero) },
            new() { Id = "e", Text = "foo bar", OtherText = "delta", Keyword = "1..5", Number = 7, Date = null },
            new() { Id = "f", Text = "foobar", OtherText = "delta", Keyword = "val.test", Number = 2, Date = new DateTimeOffset(2023, 12, 31, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "g", Text = "brown fox", OtherText = "delta", Keyword = "value", Number = 4, Date = new DateTimeOffset(2024, 1, 3, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "h", Text = "quick brown fox", OtherText = "alpha", Keyword = "[0-9]+", Number = 6, Date = new DateTimeOffset(2024, 1, 4, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "i", Text = null, OtherText = null, Keyword = null, Number = 8, Date = null },
            new() { Id = "j", Text = "literal", OtherText = "gamma", Keyword = "john*", Number = 10, Date = new DateTimeOffset(2024, 1, 5, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "k", Text = "value", OtherText = "gamma", Keyword = "jo?nny", Number = 0, Date = new DateTimeOffset(2024, 1, 6, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "l", Text = "alpha", OtherText = "gamma", Keyword = "ALPHA", Number = -1, Date = new DateTimeOffset(2024, 1, 7, 0, 0, 0, TimeSpan.Zero) },
        ];

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
        public string? Text { get; set; }
        public string? OtherText { get; set; }
        public string? Keyword { get; set; }
        public int Number { get; set; }
        public DateTimeOffset? Date { get; set; }
        public DateTimeOffset? DateNanos => Date;
    }
}
