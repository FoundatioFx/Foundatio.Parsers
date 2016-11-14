using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:value1").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field1, "value1")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void EscapeFilterProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var response = client.Index(new MyType { Field1 = "hey \"you there\"", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var result = processor.BuildQueryAsync("field1:\"hey \\\"you there\\\"\"").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.MatchPhrase(m => m.Field(w => w.Field1).Query("hey \"you there\""))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
            Assert.Equal(1, actualResponse.Total);
        }

        [Fact]
        public void ExistsFilterProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync($"_exists_:{nameof(MyType.Field2)}").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync($"_missing_:{nameof(MyType.Field2)}", new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.MustNot(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleAggregation() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var result = processor.BuildAggregationsAsync("min:field2 max:field2 date:(field5~1d^-3h min:field2 max:field2 min:field1)").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(i => i.Index("stuff").Aggregations(f => f
                .Max("max_field2", m => m.Field("field2.keyword"))
                .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).Interval("1d").Format("date_optional_time").Offset("-3h").Aggregations(l => l
                    .Max("max_field2", m => m.Field("field2.keyword"))
                    .Min("min_field1", m => m.Field("field1.keyword"))
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
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildAggregationsAsync("date:field5").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(i => i.Index("stuff").Aggregations(f => f
                .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).Interval("1d").Format("date_optional_time"))
            ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff", i => i.Mappings(m => m.Map<MyType>(t => t
                .Properties(p => p
                    .Text(e => e.Name(n => n.Field3).Fields(f => f.Keyword(k => k.Name("keyword").IgnoreAbove(256))))))));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4", Field3 = "hey now" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var result = processor.BuildQueryAsync("field1:value1", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(e => e.Field(m => m.Field1).Query("value1"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQueryAsync("field3:hey", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");
            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(m => m
                .Field(f => f.Field3)
                .Query("hey")
            )));
            expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value3" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:value1 AND -field2:value2", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildQueryAsync("field1:value1 AND NOT field2:value2", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildQueryAsync("field1:value1 OR NOT field2:value2", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result = processor.BuildQueryAsync("field1:value1 OR -field2:value2", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)", new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)").Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))))));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Testing.Casing" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:Testing.Casing", new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "Testing.Casing")));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Blake Niemyjski" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:\"Blake Niemyjski\"", new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(p => p.Field1, "Blake Niemyjski")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanTranslateTermQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var response = client.Index(new MyType { Field1 = "Testing.Casing" }, i => i.Index("stuff"));

            var processor = new ElasticQueryParser(c => c.AddVisitor(new UpdateFixedTermFieldToDateFixedExistsQueryVisitor()));
            var result = processor.BuildQueryAsync("fixed:true").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Bool(b => b.Filter(filter => filter.Exists(m => m.Field("date_fixed"))))));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)", new ElasticQueryVisitorContext().SetDefaultOperator(Operator.And).UseScoring()).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = expectedResponse.GetRequest();
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

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result = processor.BuildQueryAsync("field1:value1 nested.field1:value1", new ElasticQueryVisitorContext().UseScoring()).Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Query(f => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n.Path(p => p.Nested).Query(q2 => q2.Match(m => m.Field("nested.field1").Query("value1"))))));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQueryAsync("field1:value1 nested:(field1:value1 field4:4)", new ElasticQueryVisitorContext { UseScoring = true }).Result;

            actualResponse = client.Search<MyNestedType>(d => d.Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            expectedResponse = client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n.Path(p => p.Nested).Query(q2 => q2.Match(m => m.Field("nested.field1").Query("value1"))
                    && q2.Term("nested.field4", "4")))));

            expectedRequest = expectedResponse.GetRequest();
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

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result = processor.BuildQueryAsync("field1:value1 nested:(field1:value1 field4:4 field3:value3)", new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Query(q => result));
            var actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                && q.Nested(n => n.Path(p => p.Nested).Query(q2 =>
                    q2.Match(m => m.Field("nested.field1").Query("value1"))
                    && q2.Term("nested.field4", "4")
                    && q2.Match(m => m.Field("nested.field3").Query("value3"))))));

            var expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1 }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildQueryAsync("field4:[1 TO 2} OR field1:value1", new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.TermRange(m => m.Field(f2 => f2.Field4).GreaterThanOrEquals("1").LessThan("2")) || f.Term(m => m.Field1, "value1")));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseGeo(l => "51.5032520,-0.1278990"));
            var result = processor.BuildQueryAsync("field3:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990]", new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q =>
                q.GeoBoundingBox(m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))));
            string expectedRequest = expectedResponse.GetRequest();
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
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseGeo(l => "51.5032520,-0.1278990")
                .UseAliases(aliasMap));
            var result = processor.BuildQueryAsync("geo:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi", new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q =>
                q.GeoBoundingBox(m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))
                || q.Match(y => y.Field(e => e.Field1).Query("value1"))
                || q.TermRange(m => m.Field(g => g.Field2).GreaterThanOrEquals("1").LessThanOrEquals("4"))
                || !q.GeoDistance(m => m.Field(p => p.Field3).Location("51.5032520,-0.1278990").Distance("75mi"))));
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

    public class UpdateFixedTermFieldToDateFixedExistsQueryVisitor : ChainableQueryVisitor {
        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (!String.Equals(node.Field, "fixed", StringComparison.OrdinalIgnoreCase))
                return;

            bool isFixed;
            if (!Boolean.TryParse(node.Term, out isFixed))
                return;

            var query = new ExistsQuery { Field = "date_fixed" };
            node.SetQuery(isFixed ? query : !query);
        }
    }
}
