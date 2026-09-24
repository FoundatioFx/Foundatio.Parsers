using System;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public sealed class SyntaxCompatibilityFixture : ElasticsearchFixture
{
    public string Index { get; } = $"compatibility_{Guid.NewGuid():N}";
    public string DateIndex { get; } = $"compatibility_dates_{Guid.NewGuid():N}";

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
            { "dateNanos", new DateNanosProperty() },
            { "source", new TextProperty { Analyzer = "components", SearchAnalyzer = "whitespace_lower" } },
            { "tags", new TextProperty { Analyzer = "lowerkeyword" } },
            { "tag", new FieldAliasProperty { Path = "tags" } },
            { "folded", new KeywordProperty { Normalizer = "sort" } },
            { "ignored", new KeywordProperty { IgnoreAbove = 5 } },
            { "nullable", new KeywordProperty { NullValue = "MISSING" } }
        }
    };

    public override async ValueTask InitializeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        CompatibilityDocument[] documents =
        [
            new() { Id = "a", Text = "alpha beta", OtherText = "gamma", Keyword = "john", Number = 1, Date = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero), Source = "App.Services.Checkout", Tags = ["VIP", "alpha"], Folded = "Café", Ignored = [""], Nullable = [null] },
            new() { Id = "b", Text = "alpha gamma beta", OtherText = "beta", Keyword = "joan", Number = 3, Date = new DateTimeOffset(2024, 1, 1, 5, 59, 59, TimeSpan.Zero), Source = "App.Services.Payments", Tags = ["vip", "VIP Member"], Folded = "CAFE" },
            new() { Id = "c", Text = "beta gamma", OtherText = "alpha", Keyword = "jo?n", Number = 5, Date = new DateTimeOffset(2024, 1, 1, 6, 0, 0, TimeSpan.Zero), Source = "Other.Worker", Tags = ["beta"], Folded = "Tea", Ignored = ["123456"], Nullable = ["present"] },
            new() { Id = "d", Text = "alphabet soup", OtherText = "beta", Keyword = "johnny", Number = 9, Date = new DateTimeOffset(2024, 1, 2, 6, 0, 0, TimeSpan.Zero), Ignored = ["ok"], Nullable = [""] },
            new() { Id = "e", Text = "foo bar", OtherText = "delta", Keyword = "1..5", Number = 7, Date = null, Nullable = [] },
            new() { Id = "f", Text = "foobar", OtherText = "delta", Keyword = "val.test", Number = 2, Date = new DateTimeOffset(2023, 12, 31, 0, 0, 0, TimeSpan.Zero), Ignored = ["", null], Nullable = ["x", null] },
            new() { Id = "g", Text = "brown fox", OtherText = "delta", Keyword = "value", Number = 4, Date = new DateTimeOffset(2024, 1, 3, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "h", Text = "quick brown fox", OtherText = "alpha", Keyword = "[0-9]+", Number = 6, Date = new DateTimeOffset(2024, 1, 4, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "i", Text = null, OtherText = null, Keyword = null, Number = 8, Date = null },
            new() { Id = "j", Text = "literal", OtherText = "gamma", Keyword = "john*", Number = 10, Date = new DateTimeOffset(2024, 1, 5, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "k", Text = "value", OtherText = "gamma", Keyword = "jo?nny", Number = 0, Date = new DateTimeOffset(2024, 1, 6, 0, 0, 0, TimeSpan.Zero) },
            new() { Id = "l", Text = "alpha", OtherText = "gamma", Keyword = "ALPHA", Number = -1, Date = new DateTimeOffset(2024, 1, 7, 0, 0, 0, TimeSpan.Zero) },
        ];

        await CreateIndexAsync(Index, descriptor => descriptor
            .Settings(settings => settings.NumberOfShards(1).NumberOfReplicas(0).Analysis(analysis => analysis
                .AddSortNormalizer()
                .Analyzers(analyzers => analyzers
                    .Custom("lowerkeyword", analyzer => analyzer.Tokenizer("keyword").Filter("lowercase"))
                    .Custom("whitespace_lower", analyzer => analyzer.Tokenizer("comma_whitespace").Filter("lowercase"))
                    .Custom("components", analyzer => analyzer.Tokenizer("comma_whitespace").Filter("components", "lowercase", "unique")))
                .Tokenizers(tokenizers => tokenizers.CharGroup("comma_whitespace", tokenizer => tokenizer.TokenizeOnChars(",", "whitespace")))
                .TokenFilters(filters => filters.PatternCapture("components", filter => filter.PreserveOriginal(true).Patterns(@"([^.]+)")))))
            .Mappings(Mapping));
        var bulk = await Client.IndexManyAsync(documents, Index, timeout.Token);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);

        var refresh = await Client.Indices.RefreshAsync(Index, cancellationToken: timeout.Token);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);

        await InitializeDateIndexAsync(timeout.Token);
    }

    public sealed record CompatibilityDocument
    {
        public required string Id { get; init; }
        public string? Text { get; init; }
        public string? OtherText { get; init; }
        public string? Keyword { get; init; }
        public int Number { get; init; }
        public DateTimeOffset? Date { get; init; }
        public DateTimeOffset? DateNanos => Date;
        public string? Source { get; init; }
        public string[]? Tags { get; init; }
        public string? Folded { get; init; }
        public string?[]? Ignored { get; init; }

        // Omit absent values; an explicit null array element exercises the mapping's null_value.
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public string?[]? Nullable { get; init; }
    }

    // Strings preserve nanoseconds that DateTimeOffset cannot represent.
    public sealed record DateBoundaryDocument(string Id, string Date)
    {
        public string DateNanos => Date;
    }

    private async Task InitializeDateIndexAsync(CancellationToken cancellationToken)
    {
        DateBoundaryDocument[] documents =
        [
            new("s0", "2024-03-10T05:59:59.999Z"),
            new("s1", "2024-03-10T06:00:00Z"),
            new("s2", "2024-03-10T07:59:59.999Z"),
            new("s3", "2024-03-10T08:00:00Z"),
            new("s4", "2024-03-11T04:59:59.999Z"),
            new("s5", "2024-03-11T05:00:00Z"),
            new("f0", "2024-11-03T04:59:59.999Z"),
            new("f1", "2024-11-03T05:00:00Z"),
            new("f2", "2024-11-03T06:30:00Z"),
            new("f3", "2024-11-03T07:30:00Z"),
            new("f4", "2024-11-04T05:59:59.999Z"),
            new("f5", "2024-11-04T06:00:00Z"),
            new("l0", "2024-02-28T23:59:59.999Z"),
            new("l1", "2024-02-29T00:00:00Z"),
            new("l2", "2024-03-01T00:00:00Z"),
            new("y0", "2024-12-31T23:59:59.999Z"),
            new("y1", "2025-01-01T00:00:00Z"),
            new("n0", "2024-01-01T00:00:00.000000000Z"),
            new("n1", "2024-01-01T00:00:00.000000001Z"),
            new("n2", "2024-01-01T00:00:00.000000002Z")
        ];

        await CreateIndexAsync(DateIndex, descriptor => descriptor
            .Settings(settings => settings.NumberOfShards(1).NumberOfReplicas(0))
            .Mappings(mapping => mapping.Properties(properties => properties
                .Keyword("id").Date("date").DateNanos("dateNanos"))));
        var bulk = await Client.IndexManyAsync(documents, DateIndex, cancellationToken);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);

        var refresh = await Client.Indices.RefreshAsync(DateIndex, cancellationToken: cancellationToken);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);
    }
}
