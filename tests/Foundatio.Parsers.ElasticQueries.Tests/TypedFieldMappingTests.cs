using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class TypedFieldMappingTests
{
    [Theory]
    [InlineData("sort", false)]
    [InlineData("sort", true)]
    [InlineData("aggregation", false)]
    [InlineData("aggregation", true)]
    [InlineData("non-analyzed", false)]
    [InlineData("non-analyzed", true)]
    public async Task GetFieldName_WithColdMissingTypedField_FetchesOnce(string helper, bool asynchronous)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        int loads = 0;
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ =>
        {
            Interlocked.Increment(ref loads);
            return Task.FromResult<TypeMapping?>(new TypeMapping { Properties = new Properties() });
        }, new Inferrer(settings));

        string result = asynchronous
            ? await GetFieldNameAsync(resolver, new Field("missing"), helper, TestContext.Current.CancellationToken)
            : GetFieldName(resolver, new Field("missing"), helper);

        Assert.Equal("missing", result);
        Assert.Equal(1, loads);

        int stringLoads = 0;
        using var stringResolver = ElasticMappingResolver.CreateWithAsyncLoader(_ =>
        {
            stringLoads++;
            return Task.FromResult<TypeMapping?>(new TypeMapping { Properties = new Properties() });
        });
        string? stringResult = asynchronous
            ? await GetFieldNameAsync(stringResolver, "missing", helper, TestContext.Current.CancellationToken)
            : GetFieldName(stringResolver, "missing", helper);
        Assert.Equal(result, stringResult);
        Assert.Equal(loads, stringLoads);
    }

    [Theory]
    [InlineData("title", "sort", "title.sort", "title.sort")]
    [InlineData("TITLE", "aggregation", "title.keyword", "title.keyword")]
    [InlineData("alias", "non-analyzed", "title.keyword", "title.keyword")]
    [InlineData("status-alias", "sort", "status", "status-alias")]
    [InlineData("STATUS", "aggregation", "status", "STATUS")]
    [InlineData("title.missing", "non-analyzed", "title.missing", "title.missing")]
    public async Task GetFieldName_WithTypedField_PreservesCanonicalNamesAndSubfields(string field, string helper, string expected, string stringExpected)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        using var resolver = new ElasticMappingResolver(CreateMapping, new Inferrer(settings));

        Assert.Equal(expected, GetFieldName(resolver, new Field(field), helper));
        Assert.Equal(expected, await GetFieldNameAsync(resolver, new Field(field), helper, TestContext.Current.CancellationToken));
        Assert.Equal(stringExpected, GetFieldName(resolver, field, helper));
        Assert.Equal(stringExpected, await GetFieldNameAsync(resolver, field, helper, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("sort")]
    [InlineData("aggregation")]
    [InlineData("non-analyzed")]
    public async Task GetFieldNameAsync_WithCancelledCaller_DoesNotStartLoad(string helper)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        int loads = 0;
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ => { loads++; return Task.FromResult<TypeMapping?>(CreateMapping()); }, new Inferrer(settings));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => GetFieldNameAsync(resolver, new Field("missing"), helper, cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => GetFieldNameAsync(resolver, "missing", helper, cancellation.Token));

        Assert.Equal(0, loads);
    }

    [Fact]
    public async Task GetFieldNameAsync_WithExpressionField_UsesConfiguredInferrer()
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        settings.DefaultFieldNameInferrer(name => name.ToUpperInvariant());
        int loads = 0;
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ =>
        {
            loads++;
            return Task.FromResult<TypeMapping?>(new TypeMapping
            {
                Properties = new Properties { { "TITLE", new TextProperty { Fields = new Properties { { "keyword", new KeywordProperty() } } } } }
            });
        }, new Inferrer(settings));

        string field = await resolver.GetSortFieldNameAsync(Infer.Field<Document>(d => d.Title), TestContext.Current.CancellationToken);

        Assert.Equal("TITLE.keyword", field);
        Assert.Equal(1, loads);
    }

    [Theory]
    [InlineData("sort")]
    [InlineData("aggregation")]
    [InlineData("non-analyzed")]
    public async Task GetFieldNameAsync_WithConcurrentMissingFields_JoinsOneLoad(string helper)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var release = new TaskCompletionSource<TypeMapping?>(TaskCreationOptions.RunContinuationsAsynchronously);
        int loads = 0;
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ => { Interlocked.Increment(ref loads); return release.Task; }, new Inferrer(settings));

        var first = GetFieldNameAsync(resolver, new Field("missing"), helper, TestContext.Current.CancellationToken);
        var second = GetFieldNameAsync(resolver, "other", helper, TestContext.Current.CancellationToken);
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);
        release.SetResult(CreateMapping());
        await Task.WhenAll((Task)first, second).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal("missing", await first);
        Assert.Equal("other", await second);
        Assert.Equal(1, loads);
    }

    private static string GetFieldName(ElasticMappingResolver resolver, Field field, string helper) => helper switch
    {
        "sort" => resolver.GetSortFieldName(field),
        "aggregation" => resolver.GetAggregationsFieldName(field),
        _ => resolver.GetNonAnalyzedFieldName(field)
    };

    private static async Task<string> GetFieldNameAsync(ElasticMappingResolver resolver, Field field, string helper, CancellationToken cancellationToken = default) => helper switch
    {
        "sort" => await resolver.GetSortFieldNameAsync(field, cancellationToken),
        "aggregation" => await resolver.GetAggregationsFieldNameAsync(field, cancellationToken),
        _ => await resolver.GetNonAnalyzedFieldNameAsync(field, cancellationToken: cancellationToken)
    };

    private static string? GetFieldName(ElasticMappingResolver resolver, string field, string helper) => helper switch
    {
        "sort" => resolver.GetSortFieldName(field),
        "aggregation" => resolver.GetAggregationsFieldName(field),
        _ => resolver.GetNonAnalyzedFieldName(field)
    };

    private static async Task<string?> GetFieldNameAsync(ElasticMappingResolver resolver, string field, string helper, CancellationToken cancellationToken) => helper switch
    {
        "sort" => await resolver.GetSortFieldNameAsync(field, cancellationToken),
        "aggregation" => await resolver.GetAggregationsFieldNameAsync(field, cancellationToken),
        _ => await resolver.GetNonAnalyzedFieldNameAsync(field, cancellationToken: cancellationToken)
    };

    private static TypeMapping CreateMapping() => new()
    {
        Properties = new Properties
        {
            { "title", new TextProperty { Fields = new Properties { { "keyword", new KeywordProperty() }, { "sort", new KeywordProperty() } } } },
            { "status", new KeywordProperty() },
            { "alias", new FieldAliasProperty { Path = "title" } },
            { "status-alias", new FieldAliasProperty { Path = "status" } }
        }
    };

    private sealed class Document
    {
        public string? Title { get; set; }
    }
}
