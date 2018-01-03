using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {
    public class AggregationParserTests : TestWithLoggingBase {
        public AggregationParserTests(ITestOutputHelper output) : base(output) { }

        private IElasticClient GetClient(ConnectionSettings settings = null) {
            if (settings == null)
                settings = new ConnectionSettings();

            return new ElasticClient(settings.DisableDirectStreaming().PrettyJson());
        }

        [Fact]
        public async Task ProcessAggregationsAsync() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.IndexMany(new List<MyType> {
                new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)), Field2 = "field2" },
                new MyType { Field1 = "value2", Field4 = 2, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4)) },
                new MyType { Field1 = "value3", Field4 = 3, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3)) },
                new MyType { Field1 = "value4", Field4 = 4, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2)) },
                new MyType { Field1 = "value5", Field4 = 5, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1)) }
            }, "stuff");
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff").UseGeo(l => "51.5032520,-0.1278990"));
            var aggregations = await processor.BuildAggregationsAsync("min:field4 max:field4 avg:field4 sum:field4 percentiles:field4~50,100 cardinality:field4 missing:field2 date:field5 histogram:field4 geogrid:field3 terms:field1");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .GeoHash("geogrid_field3", h => h.Field("field3").GeoHashPrecision(GeoHashPrecision.Precision1)
                    .Aggregations(a1 => a1.Average("avg_lat", s => s.Script(ss => ss.Source("doc['field3'].lat"))).Average("avg_lon", s => s.Script(ss => ss.Source("doc['field3'].lon")))))
                .Terms("terms_field1", t => t.Field("field1.keyword").Meta(m => m.Add("@field_type", "keyword")))
                .Histogram("histogram_field4", h => h.Field("field4").Interval(50).MinimumDocumentCount(0))
                .DateHistogram("date_field5", d1 => d1.Field("field5").Interval("1d").Format("date_optional_time").MinimumDocumentCount(0))
                .Missing("missing_field2", t => t.Field("field2.keyword"))
                .Cardinality("cardinality_field4", c => c.Field("field4"))
                .Percentiles("percentiles_field4", c => c.Field("field4").Percents(50,100))
                .Sum("sum_field4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))
                .Average("avg_field4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))
                .Max("max_field4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))
                .Min("min_field4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessNestedAggregationsWithAliasesAsync() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff").Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field3))
                .Object<Dictionary<string, object>>(o1 => o1.Name(f1 => f1.Data).Properties(p1 => p1
                    .Object<object>(o2 => o2.Name("@user").Properties(p2 => p2
                        .Text(f3 => f3.Name("identity").Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256))))))))));
            var res = client.IndexMany(new List<MyType> { new MyType { Field1 = "value1" } }, "stuff");
            client.Refresh("stuff");

            var aliasMap = new AliasMap { { "user", "data.@user.identity" }, { "alias1", "field1" } };
            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff").UseAliases(aliasMap));
            var aggregations = await processor.BuildAggregationsAsync("terms:(alias1 cardinality:user)");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .Terms("terms_alias1", t => t.Field("field1.keyword").Meta(m => m.Add("@field_type", "keyword"))
                    .Aggregations(a1 => a1.Cardinality("cardinality_user", c => c.Field("data.@user.identity.keyword"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessAggregationsWithAliasesAsync() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff").Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field3))
                .Object<Dictionary<string, object>>(o1 => o1.Name(f1 => f1.Data).Properties(p1 => p1
                    .Object<object>(o2 => o2.Name("@user").Properties(p2 => p2
                        .Text(f3 => f3.Name("identity").Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256))))))))));
            var res = client.IndexMany(new List<MyType> {
                new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)), Field2 = "field2" }
            }, "stuff");
            client.Refresh("stuff");

            var aliasMap = new AliasMap { { "user", "data.@user.identity" }, { "alias1", "field1" }, { "alias2", "field2" }, { "alias3", "field3" }, { "alias4", "field4" }, { "alias5", "field5" } };
            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff").UseGeo(l => "51.5032520,-0.1278990").UseAliases(aliasMap));
            var aggregations = await processor.BuildAggregationsAsync("min:alias4 max:alias4 avg:alias4 sum:alias4 percentiles:alias4 cardinality:user missing:alias2 date:alias5 histogram:alias4 geogrid:alias3 terms:alias1");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .GeoHash("geogrid_alias3", h => h.Field("field3").GeoHashPrecision(GeoHashPrecision.Precision1)
                    .Aggregations(a1 => a1.Average("avg_lat", s => s.Script(ss => ss.Source("doc['field3'].lat"))).Average("avg_lon", s => s.Script(ss => ss.Source("doc['field3'].lon")))))
                .Terms("terms_alias1", t => t.Field("field1.keyword").Meta(m => m.Add("@field_type", "keyword")))
                .Histogram("histogram_alias4", h => h.Field("field4").Interval(50).MinimumDocumentCount(0))
                .DateHistogram("date_alias5", d1 => d1.Field("field5").Interval("1d").Format("date_optional_time").MinimumDocumentCount(0))
                .Missing("missing_alias2", t => t.Field("field2.keyword"))
                .Cardinality("cardinality_user", c => c.Field("data.@user.identity.keyword"))
                .Percentiles("percentiles_alias4", c => c.Field("field4"))
                .Sum("sum_alias4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))
                .Average("avg_alias4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))
                .Max("max_alias4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))
                .Min("min_alias4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessTermAggregations() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.IndexMany(new List<MyType> { new MyType { Field1 = "value1" } }, "stuff");
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var aggregations = processor.BuildAggregationsAsync("terms:(field1 @exclude:myexclude @include:myinclude @missing:mymissing @min:1)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .Terms("terms_field1", t => t
                    .Field("field1.keyword")
                    .MinimumDocumentCount(1)
                    .Include("myinclude")
                    .Exclude("myexclude")
                    .Missing("mymissing")
                    .Meta(m => m.Add("@field_type", "keyword")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessSortedTermAggregations() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.IndexMany(new List<MyType> { new MyType { Field1 = "value1" } }, "stuff");
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var aggregations = processor.BuildAggregationsAsync("terms:(field1 -cardinality:field4)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .Terms("terms_field1", t => t
                    .Field("field1.keyword")
                    .Order(o => o.Descending("cardinality_field4"))
                    .Aggregations(a2 => a2
                        .Cardinality("cardinality_field4", c => c.Field("field4")))
                    .Meta(m => m.Add("@field_type", "keyword")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessDateHistogramAggregations() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.IndexMany(new List<MyType> { new MyType { Field5 = SystemClock.UtcNow } }, "stuff");
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var aggregations = processor.BuildAggregationsAsync("date:(field5^1h @missing:\"0001-01-01T00:00:00\" min:field5^1h max:field5^1h)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .DateHistogram("date_field5", d1 => d1
                    .Field("field5").Meta(m => m.Add("@timezone", "1h"))
                    .Interval("1d")
                    .Format("date_optional_time")
                    .MinimumDocumentCount(0)
                    .TimeZone("+01:00")
                    .Missing(DateTime.MinValue)
                    .Aggregations(a1 => a1
                        .Min("min_field5", s => s.Field(f => f.Field5).Meta(m => m.Add("@field_type", "date").Add("@timezone", "1h")))
                        .Max("max_field5", s => s.Field(f => f.Field5).Meta(m => m.Add("@field_type", "date").Add("@timezone", "1h")))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid, actualResponse.DebugInformation);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanSpecifyDefaultValuesAggregations() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.IndexMany(new List<MyType> { new MyType { Field1 = "test" }, new MyType { Field4 = 1 } }, "stuff");
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var aggregations = processor.BuildAggregationsAsync("min:field4~0 max:field4~0 avg:field4~0 sum:field4~0 cardinality:field4~0").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .Sum("sum_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))
                .Cardinality("cardinality_field4", c => c.Field("field4").Missing(0))
                .Average("avg_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))
                .Max("max_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))
                .Min("min_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
        
        [Fact]
        public async Task GeoGridDoesNotResolveLocationForAggregation() {
            string index = nameof(MyType).ToLower();

            var client = GetClient();
            await client.DeleteIndexAsync(index);
            await client.RefreshAsync(index);
            
            var mapping = new TypeMappingDescriptor<MyType>()
                .Properties(p => p.GeoPoint(g => g.Name(f => f.Field1).RootAlias("geo")));

            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(mapping);

            var processor = new ElasticQueryParser(
                c => c
                    .UseGeo(l => "someinvalidvaluehere")
                    .UseAliases(visitor.RootAliasMap)
                    .UseMappings<MyType>(m => mapping, () => client.GetMapping(new GetMappingRequest(index, index)).Indices[index][index]));
            
            await processor.BuildAggregationsAsync("geogrid:geo~3");

        }
    }
}