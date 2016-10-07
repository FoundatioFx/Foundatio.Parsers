using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {
    public class QueryParserTests : TestWithLoggingBase {
        public QueryParserTests(ITestOutputHelper output) : base(output) { }

        private IElasticClient GetClient(ConnectionSettings settings = null) {
            if (settings == null)
                settings = new ConnectionSettings();

            return new ElasticClient(settings.DisableDirectStreaming().PrettyJson());
        }

        [Fact]
        public void CanParseQuery() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("criteria");
            Assert.NotNull(result);
            Assert.NotNull(result.Left);
            Assert.IsType<TermNode>(result.Left);
            Assert.Equal("criteria", ((TermNode)result.Left).Term);
        }

        [Fact]
        public void SimpleFilterProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:value1");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field1, "value1")))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ExistsFilterProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery($"_exists_:{nameof(MyType.Field2)}");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MissingFilterProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery($"_missing_:{nameof(MyType.Field2)}", scoreResults: true);
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.MustNot(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");

            client.CreateIndex("stuff", i => i.Mappings(m => m.Map<MyType>(t => t
                .Properties(p => p
                    .Text(e => e.Name(n => n.Field3).Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256))))))));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4", Field3 = "hey now" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyType))).Mapping));
            var result = processor.BuildQuery("field1:value1", Operator.Or, true);
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(e => e.Field(m => m.Field1).Query("value1"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQuery("field3:hey", Operator.Or, true);
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");
            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(m => m
                .Field(f => f.Field3)
                .Query("hey")
            )));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NegativeQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value3" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:value1 AND -field2:value2", Operator.Or, true);
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildQuery("field1:value1 AND NOT field2:value2", Operator.Or, true);
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildQuery("field1:value1 OR NOT field2:value2", Operator.Or, true);
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildQuery("field1:value1 OR -field2:value2", Operator.Or, true);
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:value1 (field2:value2 OR field3:value3)", Operator.Or, true);

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedQuery() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:value1 (field2:value2 OR field3:value3)");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MixedCaseTermFilterQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Testing.Casing" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:Testing.Casing", scoreResults: true);
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "Testing.Casing")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MultipleWordsTermFilterQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Blake Niemyjski" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:\"Blake Niemyjski\"", scoreResults: true);
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(p => p.Field1, "Blake Niemyjski")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GroupedOrFilterProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:value1 (field2:value2 OR field3:value3)", scoreResults: true);

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedFilterProcessor() {
            var client = GetClient(new ConnectionSettings()
                .MapDefaultTypeNames(t => t
                    .Add(typeof(MyNestedType), "things"))
                .MapDefaultTypeIndices(d => d
                    .Add(typeof(MyNestedType), "stuff")));
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff", i => i.Mappings(m => m.Map<MyNestedType>(d => d.Properties(p => p
                .Text(e => e.Name(n => n.Field1).Index())
                .Text(e => e.Name(n => n.Field2).Index())
                .Text(e => e.Name(n => n.Field3).Index())
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                    .Text(e => e.Name(n => n.Field1).Index())
                    .Text(e => e.Name(n => n.Field2).Index())
                    .Text(e => e.Name(n => n.Field3).Index())
                    .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                ))
            ))));

            var res = client.IndexManyAsync(new[] {
                new MyNestedType { Field1 = "value1", Field2 = "value2", Nested = { new MyType { Field1 = "value1", Field4 = 4 } }},
                new MyNestedType { Field1 = "value2", Field2 = "value2" },
                new MyNestedType { Field1 = "value1", Field2 = "value4" }
            });
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyNestedType))).Mapping).UseNested());
            var result = processor.BuildQuery("field1:value1 nested.field1:value1", scoreResults: true);

            var actualResponse = client.Search<MyNestedType>(d => d.Query(f => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n.Path(p => p.Nested).Query(q2 => q2.Match(m => m.Field("nested.field1").Query("value1"))))));

            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQuery("field1:value1 nested:(field1:value1 field4:4)", scoreResults: true);

            actualResponse = client.Search<MyNestedType>(d => d.Query(q => result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n.Path(p => p.Nested).Query(q2 => q2.Match(m => m.Field("nested.field1").Query("value1"))
                    && q2.Term("nested.field4", "4")))));

            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedFilterProcessor2() {
            var client = GetClient(new ConnectionSettings()
                .MapDefaultTypeNames(t => t
                    .Add(typeof(MyNestedType), "things"))
                .MapDefaultTypeIndices(d => d
                    .Add(typeof(MyNestedType), "stuff")));
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff", i => i.Mappings(m => m.Map<MyNestedType>(d => d.Properties(p => p
                .Text(e => e.Name(n => n.Field1).Index())
                .Text(e => e.Name(n => n.Field2).Index())
                .Text(e => e.Name(n => n.Field3).Index())
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                .Nested<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                    .Text(e => e.Name(n => n.Field1).Index())
                    .Text(e => e.Name(n => n.Field2).Index())
                    .Text(e => e.Name(n => n.Field3).Index())
                    .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                ))
            ))));

            var res = client.IndexMany(new[] {
                new MyNestedType { Field1 = "value1", Field2 = "value2", Nested = { new MyType { Field1 = "value1", Field4 = 4 } }},
                new MyNestedType { Field1 = "value2", Field2 = "value2" },
                new MyNestedType { Field1 = "value1", Field2 = "value4", Field3 = "value3" }
            });
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyNestedType))).Mapping).UseNested());
            var result = processor.BuildQuery("field1:value1 nested:(field1:value1 field4:4 field3:value3)", scoreResults: true);

            var actualResponse = client.Search<MyNestedType>(d => d.Query(q => result));
            var actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n.Path(p => p.Nested).Query(q2 =>
                    q2.Match(m => m.Field("nested.field1").Query("value1"))
                    && q2.Term("nested.field4", "4")
                    && q2.Match(m => m.Field("nested.field3").Query("value3"))))));

            var expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }


        [Fact]
        public void RangeQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1 }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field4:[1 TO 2} OR field1:value1", scoreResults: true);

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.TermRange(m => m.Field(f2 => f2.Field4).GreaterThanOrEquals("1").LessThan("2")) || f.Term(m => m.Field1, "value1")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleGeoRangeQuery() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c
                .UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyType))).Mapping)
                .UseGeo(l => "51.5032520,-0.1278990"));
            var result = processor.BuildQuery("field3:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990]", scoreResults: true);

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q =>
                q.GeoBoundingBox(m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GeoRangeQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyType))).Mapping)
                .UseGeo(l => "51.5032520,-0.1278990")
                .UseAliases(aliasMap));
            var result = processor.BuildQuery("geo:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi", scoreResults: true);

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q =>
                q.GeoBoundingBox(m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))
                || q.Match(y => y.Field(e => e.Field1).Query("value1"))
                || q.Range(m => m.Field(g => g.Field2).GreaterThanOrEquals(1).LessThanOrEquals(4))
                || !q.GeoDistance(m => m.Field(p => p.Field3).Location("51.5032520,-0.1278990").Distance("75mi"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        public static string GetRequest(IResponse response) {
            return response.ApiCall.RequestBodyInBytes != null ?
                $"{response.ApiCall.HttpMethod} {response.ApiCall.Uri.PathAndQuery}\r\n{Encoding.UTF8.GetString(response.ApiCall.RequestBodyInBytes)}\r\n"
                : $"{response.ApiCall.HttpMethod} {response.ApiCall.Uri.PathAndQuery}\r\n";
        }
    }

    public class MyType {
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        public string Field3 { get; set; }
        public int Field4 { get; set; }
    }

    public class MyNestedType {
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        public string Field3 { get; set; }
        public int Field4 { get; set; }
        public IList<MyType> Nested { get; set; } = new List<MyType>();
    }
}
