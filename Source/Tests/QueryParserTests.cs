using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
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
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter("field1:value1");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void EscapeFilterProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter("field1:\"value1\"");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "\"value1\"")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ExistsFilterProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter($"_exists_:{nameof(MyType.Field2)}");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Exists(nameof(MyType.Field2))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MissingFilterProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter($"_missing_:{nameof(MyType.Field2)}");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Missing(nameof(MyType.Field2))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleQueryProcessor() {
            var client = new ElasticClient();
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyType>(f => f
                .Properties(p => p
                    .String(e => e.Name(m => m.Field3).Index(FieldIndexOption.Analyzed)))));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4", Field3 = "hey now"}, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyType))).Mapping));
            var result = processor.BuildQuery("field1:value1");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(e => e.OnField(m => m.Field1).Query("value1"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQuery("field3:hey");
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");
            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(m => m
                .OnField(f => f.Field3)
                .Query("hey")
            )));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NegativeQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value3" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.SetDefaultFilterOperator(Operator.Or));
            var result = processor.BuildFilter("field1:value1 AND -field2:value2");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser(c => c.SetDefaultFilterOperator(Operator.Or));
            result = processor.BuildFilter("field1:value1 AND NOT field2:value2");
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser(c => c.SetDefaultFilterOperator(Operator.Or));
            result = processor.BuildFilter("field1:value1 OR NOT field2:value2");
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser(c => c.SetDefaultFilterOperator(Operator.Or));
            result = processor.BuildFilter("field1:value1 OR -field2:value2");
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildQuery("field1:value1 (field2:value2 OR field3:value3)");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MixedCaseTermFilterQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Testing.Casing" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter("field1:Testing.Casing");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "Testing.Casing")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MultipleWordsTermFilterQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Blake Niemyjski" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter("field1:\"Blake Niemyjski\"");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(p => p.Field1, "Blake Niemyjski")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GroupedOrFilterProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter("field1:value1 (field2:value2 OR field3:value3)");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedFilterProcessor() {
            var client = new ElasticClient(new ConnectionSettings()
                .MapDefaultTypeNames(t => t
                    .Add(typeof(MyNestedType), "things"))
                .MapDefaultTypeIndices(d => d
                    .Add(typeof(MyNestedType), "stuff")));
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyNestedType>(d => d.Index("stuff").Properties(p => p
                .String(e => e.Name(n => n.Field1).Index(FieldIndexOption.Analyzed))
                .String(e => e.Name(n => n.Field2).Index(FieldIndexOption.Analyzed))
                .String(e => e.Name(n => n.Field3).Index(FieldIndexOption.Analyzed))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                .NestedObject<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                    .String(e => e.Name(n => n.Field1).Index(FieldIndexOption.Analyzed))
                    .String(e => e.Name(n => n.Field2).Index(FieldIndexOption.Analyzed))
                    .String(e => e.Name(n => n.Field3).Index(FieldIndexOption.Analyzed))
                    .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                ))
            )));

            var res = client.IndexMany(new[] {
                new MyNestedType { Field1 = "value1", Field2 = "value2", Nested = { new MyType { Field1 = "value1", Field4 = 4 } }},
                new MyNestedType { Field1 = "value2", Field2 = "value2" },
                new MyNestedType { Field1 = "value1", Field2 = "value4" }
            });
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyNestedType))).Mapping).UseNested());
            var result = processor.BuildFilter("field1:value1 nested.field1:value1");

            var actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField(e => e.Field1).Query("value1")))
                && f.Nested(n => n.Path(p => p.Nested).Filter(f1 => f1
                    .Query(q => q.Match(m => m.OnField("nested.field1").Query("value1")))))));

            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildFilter("field1:value1 nested:(field1:value1 field4:4)");

            actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField(e => e.Field1).Query("value1")))
                && f.Nested(n => n.Path(p => p.Nested).Filter(f1 => f1
                    .Query(q => q.Match(m => m.OnField("nested.field1").Query("value1")))
                    && f1.Term("nested.field4", "4")))));

            expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedFilterProcessor2() {
            var client = new ElasticClient(new ConnectionSettings()
                .MapDefaultTypeNames(t => t
                    .Add(typeof(MyNestedType), "things"))
                .MapDefaultTypeIndices(d => d
                    .Add(typeof(MyNestedType), "stuff")));
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyNestedType>(d => d.Index("stuff").Properties(p => p
                .String(e => e.Name(n => n.Field1).Index(FieldIndexOption.Analyzed))
                .String(e => e.Name(n => n.Field2).Index(FieldIndexOption.Analyzed))
                .String(e => e.Name(n => n.Field3).Index(FieldIndexOption.Analyzed))
                .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                .NestedObject<MyType>(r => r.Name(n => n.Nested.First()).Properties(p1 => p1
                    .String(e => e.Name(n => n.Field1).Index(FieldIndexOption.Analyzed))
                    .String(e => e.Name(n => n.Field2).Index(FieldIndexOption.Analyzed))
                    .String(e => e.Name(n => n.Field3).Index(FieldIndexOption.Analyzed))
                    .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                ))
            )));

            var res = client.IndexMany(new[] {
                new MyNestedType { Field1 = "value1", Field2 = "value2", Nested = { new MyType { Field1 = "value1", Field4 = 4 } }},
                new MyNestedType { Field1 = "value2", Field2 = "value2" },
                new MyNestedType { Field1 = "value1", Field2 = "value4", Field3 = "value3" }
            });
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyNestedType))).Mapping).UseNested());
            var result = processor.BuildFilter("field1:value1 nested:(field1:value1 field4:4 field3:value3)");

            var actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            var actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField(e => e.Field1).Query("value1")))
                && f.Nested(n => n.Path(p => p.Nested).Filter(f1 => f1
                    .Query(q => q.Match(m => m.OnField("nested.field1").Query("value1")))
                    && f1.Term("nested.field4", "4")
                    && f1.Query(q => q.Match(m => m.OnField("nested.field3").Query("value3")))))));

            var expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void RangeQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1 }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilter("field4:[1 TO 2} OR field1:value1");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Range(m => m.OnField(f2 => f2.Field4).GreaterOrEquals(1).Lower(2)) || f.Term(m => m.Field1, "value1")));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GeoRangeQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh();

            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings(() => client.GetMapping(new GetMappingRequest("stuff", typeof(MyType))).Mapping)
                .UseGeo(l => "d")
                .UseAliases(aliasMap));
            var result = processor.BuildFilter("geo:[9 TO d] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = GetRequest(actualResponse);
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f =>
                f.GeoBoundingBox(m => m.Field3, "9", "d")
                || f.Query(m => m.Match(y => y.OnField(e => e.Field1).Query("value1")))
                || f.Range(m => m.OnField(g => g.Field2).GreaterOrEquals(1).LowerOrEquals(4))
                || !f.GeoDistance(m => m.Field3, e => e.Location("d").Distance("75mi"))));
            string expectedRequest = GetRequest(expectedResponse);
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        public static string GetRequest(IResponseWithRequestInformation response) {
            var requestUrl = new Uri(response.RequestInformation.RequestUrl);
            return $"{response.RequestInformation.RequestMethod.ToUpper()} {requestUrl.PathAndQuery}\r\n{Encoding.UTF8.GetString(response.RequestInformation.Request)}\r\n";
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
