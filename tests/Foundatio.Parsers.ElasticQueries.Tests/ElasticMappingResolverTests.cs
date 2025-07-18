using System;
using System.Linq.Expressions;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class ElasticMappingResolverTests : ElasticsearchTestBase
{
    public ElasticMappingResolverTests(ITestOutputHelper output, ElasticsearchFixture fixture) : base(output, fixture)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    private ITypeMapping MapMyNestedType(TypeMappingDescriptor<MyNestedType> m)
    {
        return m
            .AutoMap<MyNestedType>()
            .Dynamic()
            .DynamicTemplates(t => t.DynamicTemplate("idx_text", t => t.Match("text*").Mapping(m => m.Text(mp => mp.AddKeywordAndSortFields()))))
            .Properties(p => p
                .Text(p1 => p1.Name(n => n.Field1).AddKeywordAndSortFields())
                .Text(p1 => p1.Name(n => n.Field4).AddKeywordAndSortFields())
                    .FieldAlias(a => a.Path(n => n.Field4).Name("field4alias"))
                .Text(p1 => p1.Name(n => n.Field5).AddKeywordAndSortFields(true))
            );
    }

    [Fact]
    public void CanResolveCodedProperty()
    {
        string index = CreateRandomIndex<MyNestedType>(MapMyNestedType);

        Client.IndexMany([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
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
            new MyNestedType { Field1 = "value2", Field2 = "value2" },
            new MyNestedType { Field1 = "value1", Field2 = "value4" }
        ], index);
        Client.Indices.Refresh(index);

        var resolver = ElasticMappingResolver.Create<MyNestedType>(MapMyNestedType, Client, index, _logger);

        var payloadProperty = resolver.GetMappingProperty("payload");
        Assert.IsType<TextProperty>(payloadProperty);
        Assert.NotNull(payloadProperty.Name);
    }

    [Fact]
    public void CanResolveProperties()
    {
        string index = CreateRandomIndex<MyNestedType>(MapMyNestedType);

        Client.IndexMany([
            new MyNestedType
            {
                Field1 = "value1",
                Field2 = "value2",
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
            new MyNestedType { Field1 = "value2", Field2 = "value2" },
            new MyNestedType { Field1 = "value1", Field2 = "value4" }
        ], index);
        Client.Indices.Refresh(index);

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

        string field4sort = resolver.GetSortFieldName("Field4Alias");
        Assert.Equal("field4.sort", field4sort);

        string field4aggs = resolver.GetAggregationsFieldName("Field4Alias");
        Assert.Equal("field4.keyword", field4aggs);

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
        Assert.IsType<NumberProperty>(nestedField2Property);

        var nestedField5Property = resolver.GetMappingProperty("Nested.Field5");
        Assert.IsType<DateProperty>(nestedField5Property);

        var nestedDataProperty = resolver.GetMappingProperty("Nested.Data");
        Assert.IsType<ObjectProperty>(nestedDataProperty);
    }

    private static Expression GetObjectPath(Expression<Func<MyNestedType, object>> objectPath)
    {
        return objectPath;
    }
}
