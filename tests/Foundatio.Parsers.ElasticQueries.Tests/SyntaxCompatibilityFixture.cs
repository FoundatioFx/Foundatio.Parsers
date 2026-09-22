using System;
using System.Globalization;
using System.IO;
using System.Linq;
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
        var lines = await File.ReadAllLinesAsync(CorpusPath("documents.tsv"), timeout.Token);
        var documents = lines.Where(line => !line.StartsWith('#') && !String.IsNullOrWhiteSpace(line))
            .Select(line =>
            {
                var values = line.Split('\t');
                Assert.Equal(6, values.Length);
                DateTimeOffset? date = values[5] == "-" ? null : DateTimeOffset.Parse(values[5], CultureInfo.InvariantCulture);
                return new Document
                {
                    Id = values[0].Trim(),
                    Text = values[1] == "-" ? null : values[1],
                    OtherText = values[2] == "-" ? null : values[2],
                    Keyword = values[3] == "-" ? null : values[3],
                    Number = Int32.Parse(values[4], CultureInfo.InvariantCulture),
                    Date = date,
                    DateNanos = date
                };
            }).ToArray();
        Assert.Equal(12, documents.Length);
        Assert.Equal(documents.Length, documents.Select(document => document.Id).Distinct(StringComparer.Ordinal).Count());

        await CreateIndexAsync(Index, descriptor => descriptor
            .Settings(settings => settings.NumberOfShards(1).NumberOfReplicas(0))
            .Mappings(Mapping));
        var bulk = await Client.IndexManyAsync(documents, Index, timeout.Token);
        Assert.True(bulk.IsValidResponse && !bulk.Errors, bulk.DebugInformation);
        var refresh = await Client.Indices.RefreshAsync(Index, cancellationToken: timeout.Token);
        Assert.True(refresh.IsValidResponse, refresh.DebugInformation);
    }

    public static string CorpusPath(string name) => Path.Combine(AppContext.BaseDirectory, "Compatibility", name);

    public sealed class Document
    {
        public string Id { get; set; } = String.Empty;
        public string? Text { get; set; }
        public string? OtherText { get; set; }
        public string? Keyword { get; set; }
        public int Number { get; set; }
        public DateTimeOffset? Date { get; set; }
        public DateTimeOffset? DateNanos { get; set; }
    }
}
