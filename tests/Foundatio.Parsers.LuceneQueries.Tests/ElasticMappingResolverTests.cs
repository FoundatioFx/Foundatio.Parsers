using System;
using System.Linq.Expressions;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {

    public class ElasticMappingResolverTests : ElasticsearchTestBase {

        public ElasticMappingResolverTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        }

        [Fact]
        public void CanResolveProperties() {
            var client = GetClient();
            var index = CreateRandomIndex<MyNestedType>(client, m => m
                .AutoMap<MyNestedType>()
                .Properties(p => p
                    .Text(p1 => p1.Name(n => n.Field1).AddKeywordAndSortFields())
                    .Text(p1 => p1.Name(n => n.Field4).AddKeywordAndSortFields())
                        .FieldAlias(a => a.Path(n => n.Field4).Name("field4alias"))
                ));

            client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field2 = "value2" },
                new MyType { Field1 = "value2", Field2 = "value2" },
                new MyType { Field1 = "value1", Field2 = "value4" }
            }, index);
            client.Indices.Refresh(index);

            var resolver = ElasticMappingResolver.Create<MyNestedType>(client, _logger);

            var field1Property = resolver.GetMappingProperty("Field1");
            Assert.IsType<TextProperty>(field1Property);

            var field4Property = resolver.GetMappingProperty("Field4");
            Assert.IsType<TextProperty>(field4Property);

            var field4ReflectionProperty = resolver.GetMappingProperty(new Field(typeof(MyNestedType).GetProperty("Field4")));
            Assert.IsType<TextProperty>(field4ReflectionProperty);

            var field4ExpressionProperty = resolver.GetMappingProperty(new Field(GetObjectPath(p => p.Field4)));
            Assert.IsType<TextProperty>(field4ExpressionProperty);

            var field4AliasProperty = resolver.GetResolvedMappingProperty("Field4Alias");
            Assert.IsType<TextProperty>(field4AliasProperty.Mapping);
            Assert.Same(field4Property, field4AliasProperty.Mapping);

            var field4sort = resolver.GetSortFieldName("Field4Alias");
            Assert.Equal("field4.sort", field4sort);

            var field4aggs = resolver.GetAggregationsFieldName("Field4Alias");
            Assert.Equal("field4.keyword", field4aggs);

            var nestedIdProperty = resolver.GetMappingProperty("Nested.Id");
            Assert.IsType<TextProperty>(nestedIdProperty);

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

        private Expression GetObjectPath(Expression<Func<MyNestedType, object>> objectPath) {
            return objectPath;
        }

        // resolve from Field instance
    }
}