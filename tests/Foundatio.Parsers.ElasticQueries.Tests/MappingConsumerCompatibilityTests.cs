using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class MappingConsumerCompatibilityTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void GetMappingProperty_WithMergedChildren_PreservesPublicShape(bool text)
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        IProperty code = text ? new TextProperty { Fields = new Properties { { "local", new KeywordProperty() } } } : new ObjectProperty { Properties = new Properties { { "local", new KeywordProperty() } } };
        IProperty server = text ? new TextProperty { Fields = new Properties { { "remote", new KeywordProperty() } } } : new ObjectProperty { Properties = new Properties { { "remote", new KeywordProperty() } } };
        using var resolver = new ElasticMappingResolver(new TypeMapping { Properties = new Properties { { "name", code } } }, new Inferrer(settings), () => new TypeMapping { Properties = new Properties { { "name", server } } });
        var property = resolver.GetMappingProperty("name");
        var children = property is TextProperty t ? t.Fields : ((ObjectProperty)property!).Properties;
        Assert.True(resolver.GetMapping("name.local")?.Found);
        Assert.Contains(children!, child => child.Key.Name == "local");
        Assert.Contains(children!, child => child.Key.Name == "remote");
    }

    public static TheoryData<string> PropertyTypes => new(typeof(IProperty).Assembly.GetExportedTypes()
        .Where(type => !type.IsAbstract && type != typeof(FieldAliasProperty) && typeof(IProperty).IsAssignableFrom(type) && type.GetConstructor(Type.EmptyTypes) is not null)
        .Select(type => type.FullName!));

    [Theory]
    [MemberData(nameof(PropertyTypes))]
    public void GetMappingProperty_WithCodeChildren_PreservesClientPropertyTypes(string typeName)
    {
        var type = typeof(IProperty).Assembly.GetType(typeName)!;
        var code = (IProperty)Activator.CreateInstance(type)!;
        var server = (IProperty)Activator.CreateInstance(type)!;
        string childPropertyName = code is ObjectProperty or NestedProperty ? "Properties" : "Fields";
        var childProperty = type.GetProperty(childPropertyName)!;
        childProperty.SetValue(code, new Properties { { "local", new KeywordProperty() } });
        childProperty.SetValue(server, new Properties { { "remote", new KeywordProperty() } });
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        using var resolver = new ElasticMappingResolver(new TypeMapping { Properties = new Properties { { "name", code } } }, new Inferrer(settings), () => new TypeMapping { Properties = new Properties { { "name", server } } });
        resolver.SetPropertyMetadataValue(code, "custom", "value");

        var merged = resolver.GetMappingProperty("name", followAlias: false)!;
        var children = (Properties)childProperty.GetValue(merged)!;

        Assert.Equal(type, merged.GetType());
        Assert.NotSame(server, merged);
        Assert.Contains(children, child => child.Key.Name == "local");
        Assert.Contains(children, child => child.Key.Name == "remote");
        Assert.Single((Properties)childProperty.GetValue(server)!);
        Assert.Single((Properties)childProperty.GetValue(code)!);
        Assert.Same(children.Single(child => child.Key.Name == "local").Value, resolver.GetMappingProperty("name.local"));
        Assert.Equal("value", resolver.GetPropertyMetadataValue<string>(merged, "custom"));
    }

    [Fact]
    public void GetMappingProperty_WithNestedMerges_PreservesSettingsAndSerialization()
    {
        var local = new TextProperty { Fields = new Properties { { "local", new KeywordProperty() } } };
        var remote = new TextProperty { Analyzer = "english", SearchAnalyzer = "standard", Index = false, Norms = false, Fields = new Properties { { "remote", new KeywordProperty { IgnoreAbove = 256 } } } };
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var code = new TypeMapping { Properties = new Properties { { "parent", new ObjectProperty { Properties = new Properties { { "name", local } } } } } };
        var server = new TypeMapping { Properties = new Properties { { "parent", new ObjectProperty { Dynamic = DynamicMapping.Strict, Properties = new Properties { { "name", remote } } } } } };
        using var resolver = new ElasticMappingResolver(code, new Inferrer(settings), () => server);
        var parent = Assert.IsType<ObjectProperty>(resolver.GetMappingProperty("parent"));
        var merged = Assert.IsType<TextProperty>(parent.Properties!.Single().Value);
        Assert.Equal(DynamicMapping.Strict, parent.Dynamic);
        Assert.Equal("english", merged.Analyzer);
        Assert.Equal("standard", merged.SearchAnalyzer);
        Assert.False(merged.Index);
        Assert.False(merged.Norms);
        Assert.Equal(2, merged.Fields!.Count());
        var client = new ElasticsearchClient(settings);
        using var stream = new System.IO.MemoryStream();
        client.RequestResponseSerializer.Serialize<IProperty>(parent, stream);
        string json = System.Text.Encoding.UTF8.GetString(stream.ToArray());
        Assert.Contains("\"local\"", json);
        Assert.Contains("\"remote\"", json);
        Assert.Contains("\"ignore_above\":256", json);
        Assert.Single(remote.Fields!);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GetMappingProperty_WithExpressionChild_PreservesInferredName(bool server)
    {
        var name = new PropertyName((System.Linq.Expressions.Expression<Func<string, object>>)(value => value.Length));
        var child = new KeywordProperty();
        var parent = new ObjectProperty { Properties = new Properties { { name, child } } };
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        var mapping = new TypeMapping { Properties = new Properties { { "parent", parent } } };
        using var resolver = new ElasticMappingResolver(server ? new TypeMapping() : mapping, new Inferrer(settings), () => server ? mapping : null);

        var merged = Assert.IsType<ObjectProperty>(resolver.GetMappingProperty("parent"));

        Assert.Same(child, resolver.GetMappingProperty("parent.length"));
        Assert.Equal("length", Assert.Single(merged.Properties!).Key.Name);
        Assert.Same(name, Assert.Single(parent.Properties!).Key);
    }

    [Fact]
    public void Parse_WithSynchronousOverride_PreservesCallerThread()
    {
        var previous = SynchronizationContext.Current;
        var parser = new ThreadBoundParser();
        try
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            Assert.NotNull(parser.Parse("value", new QueryVisitorContext()));
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    [Fact]
    public async Task ValidateQueryAsync_WithMissingAlias_CallsRuntimeResolverOnce()
    {
        int calls = 0;
        using var resolver = new ElasticMappingResolver(() => new TypeMapping { Properties = new Properties() });
        var parser = new ElasticQueryParser(c => c.UseMappings(resolver).UseFieldMap(new FieldMap { { "alias", "missing" } }).UseRuntimeFieldResolver(_ => { calls++; return Task.FromResult<ElasticRuntimeField?>(null); }));
        var result = await parser.ValidateQueryAsync("alias:value", new QueryValidationOptions { AllowUnresolvedFields = false });
        Assert.False(result.IsValid);
        Assert.Equal(1, calls);
    }

    private sealed class ThreadBoundParser : LuceneQueryParser
    {
        private readonly int _owner = Environment.CurrentManagedThreadId;
        public override Task<IQueryNode?> ParseAsync(string query, IQueryVisitorContext? context = null)
        {
            if (Environment.CurrentManagedThreadId != _owner)
                throw new InvalidOperationException("Synchronous override moved off owner thread");
            return Task.FromResult<IQueryNode?>(new TermNode { Term = query });
        }
    }
}
