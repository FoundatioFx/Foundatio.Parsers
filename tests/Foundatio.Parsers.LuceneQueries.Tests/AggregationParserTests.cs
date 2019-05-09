using System;
using System.Collections.Generic;
using System.Threading;
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
    public class AggregationParserTests : ElasticsearchTestBase {
        public AggregationParserTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public async Task ProcessSingleAggregationAsync() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic().Properties(p => p.GeoPoint(g => g.Name(f => f.Field3)))));
            client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)), Field2 = "field2" },
                new MyType { Field1 = "value2", Field4 = 2, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4)) },
                new MyType { Field1 = "value3", Field4 = 3, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3)) },
                new MyType { Field1 = "value4", Field4 = 4, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2)) },
                new MyType { Field1 = "value5", Field4 = 5, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1)) }
            }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index).UseGeo(l => "51.5032520,-0.1278990"));
            var aggregations = await processor.BuildAggregationsAsync("min:field4");

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Min("min_field4", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessSingleAggregationWithAliasAsync() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic().Properties(p => p.GeoPoint(g => g.Name(f => f.Field3)))));
            client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)), Field2 = "field2" },
                new MyType { Field1 = "value2", Field4 = 2, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4)) },
                new MyType { Field1 = "value3", Field4 = 3, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3)) },
                new MyType { Field1 = "value4", Field4 = 4, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2)) },
                new MyType { Field1 = "value5", Field4 = 5, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1)) }
            }, index);
            client.Refresh(index);

            var fieldMap = new FieldMap { { "heynow", "field4" } };
            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index).UseFieldMap(fieldMap).UseGeo(l => "51.5032520,-0.1278990"));
            var aggregations = await processor.BuildAggregationsAsync("min:heynow");

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Min("min_heynow", c => c.Field("field4").Meta(m => m.Add("@field_type", "long")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessAggregationsAsync() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic().Properties(p => p.GeoPoint(g => g.Name(f => f.Field3)))));
            client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)), Field2 = "field2" },
                new MyType { Field1 = "value2", Field4 = 2, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(4)) },
                new MyType { Field1 = "value3", Field4 = 3, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(3)) },
                new MyType { Field1 = "value4", Field4 = 4, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(2)) },
                new MyType { Field1 = "value5", Field4 = 5, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(1)) }
            }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index).UseGeo(l => "51.5032520,-0.1278990"));
            var aggregations = await processor.BuildAggregationsAsync("min:field4 max:field4 avg:field4 sum:field4 percentiles:field4~50,100 cardinality:field4 missing:field2 date:field5 histogram:field4 geogrid:field3 terms:field1");

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
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
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessNestedAggregationsWithAliasesAsync() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic().Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field3))
                .Object<Dictionary<string, object>>(o1 => o1.Name(f1 => f1.Data).Properties(p1 => p1
                    .Object<object>(o2 => o2.Name("@user").Properties(p2 => p2
                        .Text(f3 => f3.Name("identity")
                            .Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256)))))))))));

            client.IndexMany(new[] { new MyType { Field1 = "value1" } }, index);
            client.Refresh(index);

            var aliasMap = new FieldMap { { "user", "data.@user.identity" }, { "alias1", "field1" } };
            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index).UseFieldMap(aliasMap));
            var aggregations = await processor.BuildAggregationsAsync("terms:(alias1 cardinality:user)");

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Terms("terms_alias1", t => t.Field("field1.keyword").Meta(m => m.Add("@field_type", "keyword"))
                    .Aggregations(a1 => a1.Cardinality("cardinality_user", c => c.Field("data.@user.identity.keyword"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessSingleAggregationWithAlias() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            
            client.IndexMany(new[] {
                new MyType { Field2 = "field2" }
            }, index);
            client.Refresh(index);

            var aliasMap = new FieldMap { { "alias2", "field2" } };
            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index).UseFieldMap(aliasMap));
            var aggregations = await processor.BuildAggregationsAsync("missing:alias2");

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Missing("missing_alias2", t => t.Field("field2.keyword"))));
            
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public async Task ProcessAggregationsWithAliasesAsync() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic().Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field3))
                .Object<Dictionary<string, object>>(o1 => o1.Name(f1 => f1.Data).Properties(p1 => p1
                    .Object<object>(o2 => o2.Name("@user").Properties(p2 => p2
                        .Text(f3 => f3.Name("identity").Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256)))))))))));
            
            client.IndexMany(new[] {
                new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990", Field5 = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(5)), Field2 = "field2" }
            }, index);
            client.Refresh(index);

            var aliasMap = new FieldMap { { "user", "data.@user.identity" }, { "alias1", "field1" }, { "alias2", "field2" }, { "alias3", "field3" }, { "alias4", "field4" }, { "alias5", "field5" } };
            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index).UseGeo(l => "51.5032520,-0.1278990").UseFieldMap(aliasMap));
            var aggregations = await processor.BuildAggregationsAsync("min:alias4 max:alias4 avg:alias4 sum:alias4 percentiles:alias4 cardinality:user missing:alias2 date:alias5 histogram:alias4 geogrid:alias3 terms:alias1");

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
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
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessTermAggregations() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            client.IndexMany(new[] { new MyType { Field1 = "value1" } }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index));
            var aggregations = processor.BuildAggregationsAsync("terms:(field1 @exclude:myexclude @include:myinclude @include:otherinclude @missing:mymissing @exclude:otherexclude @min:1)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Terms("terms_field1", t => t
                    .Field("field1.keyword")
                    .MinimumDocumentCount(1)
                    .Include(new[] { "otherinclude", "myinclude" })
                    .Exclude(new[] { "otherexclude", "myexclude" })
                    .Missing("mymissing")
                    .Meta(m => m.Add("@field_type", "keyword")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessHistogramIntervalAggregations() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            client.IndexMany(new[] { new MyType { Field1 = "value1" } }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index));
            var aggregations = processor.BuildAggregationsAsync("histogram:(field1~0.1)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Histogram("histogram_field1", t => t
                    .Field("field1.keyword")
                    .Interval(0.1)
                    .MinimumDocumentCount(0)
                    )));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessTermTopHitsAggregations() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            client.IndexMany(new[] { new MyType { Field1 = "value1" } }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index));
            var aggregations = processor.BuildAggregationsAsync("terms:(field1~1000^2 tophits:(_~1000 @include:myinclude))").Result;

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Terms("terms_field1", t => t
                    .Field("field1.keyword")
                    .Size(1000)
                    .MinimumDocumentCount(2)
                    .Aggregations(a1 => a1.TopHits("tophits", t2 => t2.Size(1000).Source(s => s.Includes(i => i.Field("myinclude")))))
                    .Meta(m => m.Add("@field_type", "keyword")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessSortedTermAggregations() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            client.IndexMany(new[] { new MyType { Field1 = "value1" } }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index));
            var aggregations = processor.BuildAggregationsAsync("terms:(field1 -cardinality:field4)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Terms("terms_field1", t => t
                    .Field("field1.keyword")
                    .Order(o => o.Descending("cardinality_field4"))
                    .Aggregations(a2 => a2
                        .Cardinality("cardinality_field4", c => c.Field("field4")))
                    .Meta(m => m.Add("@field_type", "keyword")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ProcessDateHistogramAggregations() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            client.IndexMany(new[] { new MyType { Field5 = SystemClock.UtcNow } }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index));
            var aggregations = processor.BuildAggregationsAsync("date:(field5^1h @missing:\"0001-01-01T00:00:00\" min:field5^1h max:field5^1h)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
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
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid, actualResponse.DebugInformation);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanSpecifyDefaultValuesAggregations() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Dynamic()));
            client.IndexMany(new[] { new MyType { Field1 = "test" }, new MyType { Field4 = 1 } }, index);
            client.Refresh(index);

            var processor = new ElasticQueryParser(c => c.SetLoggerFactory(Log).UseMappings(client, index));
            var aggregations = processor.BuildAggregationsAsync("min:field4~0 max:field4~0 avg:field4~0 sum:field4~0 cardinality:field4~0").Result;

            var actualResponse = client.Search<MyType>(d => d.Index(index).Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualRequest);

            var expectedResponse = client.Search<MyType>(d => d.Index(index).Aggregations(a => a
                .Sum("sum_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))
                .Cardinality("cardinality_field4", c => c.Field("field4").Missing(0))
                .Average("avg_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))
                .Max("max_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))
                .Min("min_field4", c => c.Field("field4").Missing(0).Meta(m => m.Add("@field_type", "long")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
        
        [Fact]
        public async Task GeoGridDoesNotResolveLocationForAggregation() {
            var client = GetClient();
            var index = CreateRandomIndex(client, i => i.Map<MyType>(d => d.Properties(p => p
                .GeoPoint(g => g.Name(f => f.Field1))
                .FieldAlias(a => a.Name("geo").Path(f => f.Field1)))));

            var processor = new ElasticQueryParser(
                c => c
                    .UseGeo(l => "someinvalidvaluehere")
                    .UseMappings(client, index));
            
            await processor.BuildAggregationsAsync("geogrid:geo~3");

        }
    }
}