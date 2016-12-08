using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elasticsearch.Net;
using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
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
            var result = processor.BuildFilterAsync("field1:value1").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void IncludeProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var includes = new Dictionary<string, string> {
                { "stuff", "field2:value2" }
            };

            var processor = new ElasticQueryParser(c => c.UseIncludes(includes));
            var result = processor.BuildFilterAsync("field1:value1 @include:stuff").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f =>
                f.Term(m => m.Field1, "value1")
                && f.Term(m => m.Field2, "value2")));

            string expectedRequest = expectedResponse.GetRequest();
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
            var response = client.Index(new MyType { Field1 = "hey \"you there\"", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client));
            var result = processor.BuildFilterAsync("field1:\"hey \\\"you there\\\"\"").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Query(q => q.MatchPhrase(m => m.OnField(w => w.Field1).Query("hey \"you there\"")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
            Assert.Equal(1, actualResponse.Total);
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
            var result = processor.BuildFilterAsync($"_exists_:{nameof(MyType.Field2)}").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Exists(nameof(MyType.Field2))));
            string expectedRequest = expectedResponse.GetRequest();
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
            var result = processor.BuildFilterAsync($"_missing_:{nameof(MyType.Field2)}").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Missing(nameof(MyType.Field2))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleAggregation() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyType>(f => f.Dynamic()
                .Properties(p => p
                    .String(e => e.Name(m => m.Field2).Index(FieldIndexOption.Analyzed)
                        .Fields(f1 => f1.String(e1 => e1.Name("keyword").Index(FieldIndexOption.NotAnalyzed)))))));

            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client));
            var result = processor.BuildAggregationsAsync("min:field2 max:field2 date:(field5~1d^-3h min:field2 max:field2 min:field1)").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(i => i.Index("stuff").Aggregations(f => f
                .Max("max_field2", m => m.Field("field2.keyword"))
                .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).Interval("1d").Offset("-3h").Aggregations(l => l
                    .Max("max_field2", m => m.Field("field2.keyword"))
                    .Min("min_field1", m => m.Field(m2 => m2.Field1))
                    .Min("min_field2", m => m.Field("field2.keyword"))
                ))
                .Min("min_field2", m => m.Field("field2.keyword"))
            ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void DateAggregation() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser();
            var result = processor.BuildAggregationsAsync("date:field5").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(i => i.Index("stuff").Aggregations(f => f
                .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).Interval("1d"))
            ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }


        [Fact]
        public void SimpleQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyType>(f => f
                .Properties(p => p
                    .String(e => e.Name(m => m.Field3).Index(FieldIndexOption.Analyzed)))));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4", Field3 = "hey now" }, i => i.Index("stuff"));
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client));
            var result = processor.BuildQueryAsync("field1:value1").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(e => e.OnField(m => m.Field1).Query("value1"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQueryAsync("field3:hey").Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");
            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(m => m
                .OnField(f => f.Field3)
                .Query("hey")
            )));
            expectedRequest = expectedResponse.GetRequest();
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

            var processor = new ElasticQueryParser();
            var result = processor.BuildFilterAsync("field1:value1 AND -field2:value2", new ElasticQueryVisitorContext().SetDefaultOperator(Operator.Or)).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildFilterAsync("field1:value1 AND NOT field2:value2", new ElasticQueryVisitorContext().SetDefaultOperator(Operator.Or)).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildFilterAsync("field1:value1 OR NOT field2:value2", new ElasticQueryVisitorContext().SetDefaultOperator(Operator.Or)).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildFilterAsync("field1:value1 OR -field2:value2", new ElasticQueryVisitorContext().SetDefaultOperator(Operator.Or)).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
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
            var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = expectedResponse.GetRequest();
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
            var result = processor.BuildFilterAsync("field1:Testing.Casing").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "Testing.Casing")));
            string expectedRequest = expectedResponse.GetRequest();
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
            var result = processor.BuildFilterAsync("field1:\"Blake Niemyjski\"").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(p => p.Field1, "Blake Niemyjski")));
            string expectedRequest = expectedResponse.GetRequest();
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
            var result = processor.BuildFilterAsync("field1:value1 (field2:value2 OR field3:value3)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = expectedResponse.GetRequest();
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

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result = processor.BuildFilterAsync("field1:value1 nested.field1:value1").Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField(e => e.Field1).Query("value1")))
                && f.Nested(n => n.Path(p => p.Nested).Filter(f1 => f1
                    .Query(q => q.Match(m => m.OnField("nested.field1").Query("value1")))))));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildFilterAsync("field1:value1 nested:(field1:value1 field4:4)").Result;

            actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField(e => e.Field1).Query("value1")))
                && f.Nested(n => n.Path(p => p.Nested).Filter(f1 => f1
                    .Query(q => q.Match(m => m.OnField("nested.field1").Query("value1")))
                    && f1.Term("nested.field4", "4")))));

            expectedRequest = expectedResponse.GetRequest();
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

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result = processor.BuildFilterAsync("field1:value1 nested:(field1:value1 field4:4 field3:value3)").Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            var actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField(e => e.Field1).Query("value1")))
                && f.Nested(n => n.Path(p => p.Nested).Filter(f1 => f1
                    .Query(q => q.Match(m => m.OnField("nested.field1").Query("value1")))
                    && f1.Term("nested.field4", "4")
                    && f1.Query(q => q.Match(m => m.OnField("nested.field3").Query("value3")))))));

            var expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanHandleJustName() {
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
                .Object<MyType>(r => r.Name(n => n.Nested.First()).Path("just_name").Properties(p1 => p1
                    .String(e => e.Name(n => n.Field1).Index(FieldIndexOption.Analyzed))
                    .String(e => e.Name(n => n.Field2).Index(FieldIndexOption.Analyzed))
                    .String(e => e.Name(n => n.Field3).Index(FieldIndexOption.Analyzed).IndexName("moved"))
                    .Number(e => e.Name(n => n.Field4).Type(NumberType.Integer))
                ))
            )));

            var res = client.IndexMany(new[] {
                new MyNestedType { Field1 = "value1", Field2 = "value2", Nested = { new MyType { Field1 = "value1", Field4 = 4, Field3 = "Hey"} }},
                new MyNestedType { Field1 = "value2", Field2 = "value2" },
                new MyNestedType { Field1 = "value1", Field2 = "value4", Field3 = "value3" }
            });
            client.Refresh();

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result = processor.BuildFilterAsync("moved:hey").Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Filter(result));
            var actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Filter(f => f
                .Query(q => q.Match(m => m.OnField("moved").Query("hey")))));

            var expectedRequest = expectedResponse.GetRequest();
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
            var result = processor.BuildFilterAsync("field4:[1 TO 2} OR field1:value1").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Range(m => m.OnField(f2 => f2.Field4).GreaterOrEquals(1).Lower(2)) || f.Term(m => m.Field1, "value1")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanParseSort() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyType>(f => f.Dynamic()
                .Properties(p => p
                    .GeoPoint(g => g.Name(gp => gp.Field3))
                    .String(e => e.Name(m => m.Field1).Index(FieldIndexOption.Analyzed)
                        .Fields(f1 => f1.String(e1 => e1.Name("keyword").Index(FieldIndexOption.NotAnalyzed)))))));

            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh();

            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseAliases(aliasMap));
            var sort = processor.BuildSortAsync("geo -field1 -(field2 field3 +field4)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Sort(sort));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff")
                .SortMulti(
                    s => s.OnField("field3"),
                    s => s.OnField("field1.keyword").Descending(),
                    s => s.OnField("field2").Descending(),
                    s => s.OnField("field3").Descending(),
                    s => s.OnField("field4")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GeoRangeQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff").AddMapping<MyType>(f => f.Dynamic()
                .Properties(p => p
                    .GeoPoint(g => g.Name(gp => gp.Field3))
                    .String(e => e.Name(m => m.Field1).Index(FieldIndexOption.Analyzed)
                        .Fields(f1 => f1.String(e1 => e1.Name("keyword").Index(FieldIndexOption.NotAnalyzed)))))));

            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh();

            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseGeo(l => "d")
                .UseAliases(aliasMap));
            var result = processor.BuildFilterAsync("geo:[9 TO d] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi").Result;
            var geogridAgg = processor.BuildAggregationsAsync("geogrid:geo").Result;
            var sort = processor.BuildSortAsync("geo -field1").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(result).Sort(sort));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff")
                .SortMulti(s => s.OnField("field3"), s => s.OnField("field1.keyword").Descending())
                .Filter(f =>
                    f.GeoBoundingBox(m => m.Field3, "9", "d")
                    || f.Query(m => m.Match(y => y.OnField(e => e.Field1).Query("value1")))
                    || f.Range(m => m.OnField(g => g.Field2).GreaterOrEquals(1).LowerOrEquals(4))
                    || !f.GeoDistance(m => m.Field3, e => e.Location("d").Distance("75mi"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
    }

    public class MyType {
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        public string Field3 { get; set; }
        public int Field4 { get; set; }
        public DateTime Field5 { get; set; }
    }

    public class MyNestedType {
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        public string Field3 { get; set; }
        public int Field4 { get; set; }
        public IList<MyType> Nested { get; set; } = new List<MyType>();
    }
}
