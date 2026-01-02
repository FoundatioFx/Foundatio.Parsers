using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticMappingResolverTests : ElasticsearchTestBase
{
    public ElasticMappingResolverTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    private void MapMyNestedType(TypeMappingDescriptor<MyNestedType> m)
    {
        m.Dynamic(DynamicMapping.True)
            .DynamicTemplates(dt => dt.Add("idx_text", d => d.Mapping(dm => dm.Text(o => o.AddKeywordAndSortFields())).Match("text*")))
            .Properties(p => p
                .Text(p1 => p1.Field1, o => o.AddKeywordAndSortFields())
                .Text(p1 => p1.Field4, o => o.AddKeywordAndSortFields())
                .FieldAlias("field4alias", o => o.Path("field4"))
                .Text(p1 => p1.Field5, o => o.AddKeywordAndSortFields(true))
            );
    }

    [Fact]
    public async Task CanResolveCodedProperty()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(MapMyNestedType);

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
                Payload = "test payload",
                Nested =
            [
                new MyType
                {
                    Field1 = "banana",
                    Data = {
                        { "number-0001", 23 },
                        { "text-0001", "Hey" },
                        { "spaced field", "hey" }
                    }
                }
            ]
            },
            new MyNestedType { Field1 = "value2", Field2 = "value2", Payload = "another payload" },
            new MyNestedType { Field1 = "value1", Field2 = "value4", Payload = "third payload" }
        ], index);

        await Client.Indices.RefreshAsync(index);

        var resolver = ElasticMappingResolver.Create<MyNestedType>(MapMyNestedType, Client, index, _logger);

        var payloadProperty = resolver.GetMappingProperty("payload");
        Assert.IsType<TextProperty>(payloadProperty);
        // TryGetName() returns null in the new Elastic client as property names are stored in dictionary keys
    }

    [Fact]
    public async Task CanResolveProperties()
    {
        string index = await CreateRandomIndexAsync<MyNestedType>(MapMyNestedType);

        await Client.IndexManyAsync([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
                Nested =
            [
                new MyType
                {
                    Id = "nested-id-1",
                    Field1 = "banana",
                    Data = {
                        { "number-0001", 23 },
                        { "text-0001", "Hey" },
                        { "spaced field", "hey" }
                    }
                }
            ]
            },
            new MyNestedType { Field1 = "value2", Field2 = "value2" },
            new MyNestedType { Field1 = "value1", Field2 = "value4" }
        ], index);
         await Client.Indices.RefreshAsync(index);

        var resolver = ElasticMappingResolver.Create<MyNestedType>(MapMyNestedType, Client, index, _logger);

        string dynamicTextAggregation = resolver.GetAggregationsFieldName("nested.data.text-0001");
        Assert.Equal("nested.data.text-0001.keyword", dynamicTextAggregation);

        string dynamicSpacedAggregation = resolver.GetAggregationsFieldName("nested.data.spaced field");
        Assert.Equal("nested.data.spaced field.keyword", dynamicSpacedAggregation);

        string dynamicSpacedSort = resolver.GetSortFieldName("nested.data.spaced field");
        Assert.Equal("nested.data.spaced field.keyword", dynamicSpacedSort);

        string dynamicSpacedField = resolver.GetResolvedField("nested.data.spaced field");
        Assert.Equal("nested.data.spaced field", dynamicSpacedField);

        var field1Property = resolver.GetMappingProperty("Field1");
        Assert.IsType<TextProperty>(field1Property);

        string field5Property = resolver.GetAggregationsFieldName("Field5");
        Assert.Equal("field5.keyword", field5Property);

        var unknownProperty = resolver.GetMappingProperty("UnknowN.test.doesNotExist");
        Assert.Null(unknownProperty);

        string field1 = resolver.GetResolvedField("FielD1");
        Assert.Equal("field1", field1);

        string emptyField = resolver.GetResolvedField(" ");
        Assert.Equal(" ", emptyField);

        string unknownField = resolver.GetResolvedField("UnknowN.test.doesNotExist");
        Assert.Equal("UnknowN.test.doesNotExist", unknownField);

        string unknownField2 = resolver.GetResolvedField("unknown.test.doesnotexist");
        Assert.Equal("unknown.test.doesnotexist", unknownField2);

        string unknownField3 = resolver.GetResolvedField("unknown");
        Assert.Equal("unknown", unknownField3);

        var field4Property = resolver.GetMappingProperty("Field4");
        Assert.IsType<TextProperty>(field4Property);

        var field4ReflectionProperty = resolver.GetMappingProperty(new Field(typeof(MyNestedType).GetProperty("Field4")));
        Assert.IsType<TextProperty>(field4ReflectionProperty);

        var field4ExpressionProperty = resolver.GetMappingProperty(new Field(GetObjectPath(p => p.Field4)));
        Assert.IsType<TextProperty>(field4ExpressionProperty);

        var field4AliasMapping = resolver.GetMapping("Field4Alias", true);
        Assert.IsType<TextProperty>(field4AliasMapping.Property);
        Assert.Same(field4Property, field4AliasMapping.Property);

        string field4Sort = resolver.GetSortFieldName("Field4Alias");
        Assert.Equal("field4.sort", field4Sort);

        string field4Aggs = resolver.GetAggregationsFieldName("Field4Alias");
        Assert.Equal("field4.keyword", field4Aggs);

        var nestedIdProperty = resolver.GetMappingProperty("Nested.Id");
        Assert.IsType<TextProperty>(nestedIdProperty);

        string nestedId = resolver.GetResolvedField("Nested.Id");
        Assert.Equal("nested.id", nestedId);

        nestedIdProperty = resolver.GetMappingProperty("nested.id");
        Assert.IsType<TextProperty>(nestedIdProperty);

        var nestedField1Property = resolver.GetMappingProperty("Nested.Field1");
        Assert.IsType<TextProperty>(nestedField1Property);

        nestedField1Property = resolver.GetMappingProperty("nEsted.fieLD1");
        Assert.IsType<TextProperty>(nestedField1Property);

        var nestedField2Property = resolver.GetMappingProperty("Nested.Field4");
        Assert.IsType<LongNumberProperty>(nestedField2Property);

        var nestedField5Property = resolver.GetMappingProperty("Nested.Field5");
        Assert.IsType<DateProperty>(nestedField5Property);

        var nestedDataProperty = resolver.GetMappingProperty("Nested.Data");
        Assert.IsType<ObjectProperty>(nestedDataProperty);
    }

    private static Expression GetObjectPath(Expression<Func<MyNestedType, object>> objectPath)
    {
        return objectPath;
    }

    [Fact]
    public async Task PropertyMetadataIsThreadSafe()
    {
        var resolver = new ElasticMappingResolver(() => null);

        // Create multiple properties and set metadata in parallel
        var properties = Enumerable.Range(0, 100)
            .Select(_ => new KeywordProperty())
            .ToArray();

        // Set metadata in parallel
        var setTasks = properties.Select((p, i) => Task.Run(() =>
        {
            resolver.SetPropertyMetadataValue(p, "index", i);
            resolver.SetPropertyMetadataValue(p, "name", $"property_{i}");
        }));
        await Task.WhenAll(setTasks);

        // Read metadata in parallel
        var readTasks = properties.Select((p, i) => Task.Run(() =>
        {
            var index = resolver.GetPropertyMetadataValue<int>(p, "index");
            var name = resolver.GetPropertyMetadataValue<string>(p, "name");
            return (ExpectedIndex: i, ActualIndex: index, ExpectedName: $"property_{i}", ActualName: name);
        }));
        var results = await Task.WhenAll(readTasks);

        // Verify all values are correct
        foreach (var result in results)
        {
            Assert.Equal(result.ExpectedIndex, result.ActualIndex);
            Assert.Equal(result.ExpectedName, result.ActualName);
        }
    }

    [Fact]
    public void PropertyMetadataCopyPreservesValues()
    {
        var resolver = new ElasticMappingResolver(() => null);

        var source = new KeywordProperty();
        resolver.SetPropertyMetadataValue(source, "key1", "value1");
        resolver.SetPropertyMetadataValue(source, "key2", 42);

        var target = new TextProperty();
        resolver.CopyPropertyMetadata(source, target);

        Assert.Equal("value1", resolver.GetPropertyMetadataValue<string>(target, "key1"));
        Assert.Equal(42, resolver.GetPropertyMetadataValue<int>(target, "key2"));
    }
}
