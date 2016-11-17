using System;
using System.Collections.Generic;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
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
        public void ProcessAggregations() {
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
            var aggregations = processor.BuildAggregationsAsync("min:field4 max:field4 avg:field4 sum:field4 percentiles:field4 cardinality:field4 missing:field2 date:field5 geogrid:field3 terms:field1").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(aggregations));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(a => a
                .GeoHash("geogrid_field3", h => h.Field("field3").GeoHashPrecision(GeoHashPrecision.Precision1)
                    .Aggregations(a1 => a1.Average("avg_lat", s => s.Script(ss => ss.Inline("doc['field3'].lat"))).Average("avg_lon", s => s.Script(ss => ss.Inline("doc['field3'].lon")))))
                .Terms("terms_field1", t => t.Field("field1.keyword"))
                .DateHistogram("date_field5", d1 => d1.Field("field5").Interval("1d").Format("date_optional_time"))
                .Missing("missing_field2", t => t.Field("field2.keyword"))
                .Cardinality("cardinality_field4", c => c.Field("field4"))
                .Percentiles("percentiles_field4", c => c.Field("field4"))
                .Sum("sum_field4", c => c.Field("field4"))
                .Average("avg_field4", c => c.Field("field4"))
                .Max("max_field4", c => c.Field("field4"))
                .Min("min_field4", c => c.Field("field4"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.True(actualResponse.IsValid);
            Assert.True(expectedResponse.IsValid);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
    }
}