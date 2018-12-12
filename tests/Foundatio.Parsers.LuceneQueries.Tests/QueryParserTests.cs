using Foundatio.Logging;
using Foundatio.Logging.Xunit;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {

    public class QueryParserTests : TestWithLoggingBase {

        public QueryParserTests(ITestOutputHelper output) : base(output) {
        }

        private IElasticClient GetClient(ConnectionSettings settings = null) {
            if (settings == null)
                settings = new ConnectionSettings();

            return new ElasticClient(settings.DisableDirectStreaming().DefaultTypeName("_doc").PrettyJson());
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
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d => d.Index("stuff").Query(q => q.Bool(b => b.Filter(f => f.Term(m => m.Field1, "value1")))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void IncludeProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var includes = new Dictionary<string, string> {
                {"stuff", "field2:value2"}
            };

            var processor = new ElasticQueryParser(c => c.UseIncludes(includes));
            var result =
                processor.BuildQueryAsync("field1:value1 @include:stuff",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f =>
                f.Term(m => m.Field1, "value1")
                && f.Term(m => m.Field2, "value2")));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ShouldGenerateORedTermsQuery() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field3 = "value3" },
                i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result =
                processor.BuildQueryAsync("field1:value1 field2:value2 field3:value3",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f =>
                f.Term(m => m.Field1, "value1") || f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3")));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void ShouldMergeTermsIntoMatchQueryForAnalyzedFields() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");
            client.CreateIndex("stuff");
            client.Map<MyType>(
                d => d.Dynamic(true).Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))
                    .Text(e => e.Name(m => m.Field1).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))
                    .Keyword(e => e.Name(m => m.Field2))
                ));
            var response = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field3 = "value3" },
                i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.SetDefaultFields(new[] { "field1" }).UseMappings<MyType>(client, "stuff"));

            var result = processor.BuildQueryAsync("field1:(value1 abc def ghi) field2:(value2 jhk)",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f =>
                f.Match(m => m.Field(mf => mf.Field1).Query("value1 abc def ghi")) || f.Term(m => m.Field2, "value2") || f.Term(m => m.Field2, "jhk")));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result = processor.BuildQueryAsync("value1 abc def ghi",
                    new ElasticQueryVisitorContext {  DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f =>
                f.Match(m => m.Field(mf => mf.Field1).Query("value1 abc def ghi"))));

            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            //multi-match on multiple default fields
            processor = new ElasticQueryParser(c => c.SetDefaultFields(new[] { "field1", "field2" }).UseMappings<MyType>(client, "stuff"));
            result = processor.BuildQueryAsync("value1 abc def ghi",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f =>
                f.MultiMatch(m => m.Fields(mf => mf.Fields("field1", "field2")).Query("value1 abc def ghi").Type(TextQueryType.BestFields))));

            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var response = client.Index(new MyType { Field1 = "hey \"you there\"", Field2 = "value2" },
                i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var result = processor.BuildQueryAsync("field1:\"hey \\\"you there\\\"\"").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest(true);
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(
                                q =>
                                    q.Bool(
                                        b =>
                                            b.Filter(
                                                f =>
                                                    f.MatchPhrase(m => m.Query("hey \"you there\"").Field(w => w.Field1))))));
            string expectedRequest = expectedResponse.GetRequest(true);
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(q => q.Bool(b => b.Filter(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync($"_missing_:{nameof(MyType.Field2)}",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(q => q.Bool(b => b.MustNot(f => f.Exists(e => e.Field(nameof(MyType.Field2)))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void MinMaxWithDateHistogramAggregation() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now },
                i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var result =
                processor.BuildAggregationsAsync(
                    "min:field2 max:field2 date:(field5~1d^\"America/Chicago\" min:field2 max:field2 min:field1 @offset:-6h)").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(i => i.Index("stuff").Aggregations(f => f
                .Max("max_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "keyword")))
                .DateHistogram("date_field5",
                    d =>
                        d.Field(d2 => d2.Field5)
                            .Interval("1d")
                            .Format("date_optional_time")
                            .MinimumDocumentCount(0)
                            .TimeZone("America/Chicago")
                            .Offset("-6h")
                            .Meta(m2 => m2.Add("@timezone", "America/Chicago"))
                            .Aggregations(l => l
                                .Min("min_field1", m => m.Field("field1.keyword").Meta(m2 => m2.Add("@field_type", "keyword")))
                                .Max("max_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "keyword")))
                                .Min("min_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "keyword")))
                            ))
                .Min("min_field2", m => m.Field("field2.keyword").Meta(m2 => m2.Add("@field_type", "keyword")))
            ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2", Field5 = DateTime.Now },
                i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Index(new MyType { Field2 = "value4", Field5 = DateTime.Now }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser();
            var result = processor.BuildAggregationsAsync("date:field5").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Aggregations(result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(i => i.Index("stuff").Aggregations(f => f
                .DateHistogram("date_field5", d => d.Field(d2 => d2.Field5).Interval("1d").Format("date_optional_time").MinimumDocumentCount(0))
            ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field1:value1",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d => d.Index("stuff").Query(q => q.Match(e => e.Field(m => m.Field1).Query("value1"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result =
                processor.BuildQueryAsync("field3:hey",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);
            expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => q.Match(m => m
                .Field(f => f.Field3)
                .Query("hey")
            )));
            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field1:value1 AND -field2:value2",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result =
                processor.BuildQueryAsync("field1:value1 AND NOT field2:value2",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") && !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result =
                processor.BuildQueryAsync("field1:value1 OR NOT field2:value2",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            processor = new ElasticQueryParser();
            result =
                processor.BuildQueryAsync("field1:value1 OR -field2:value2",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;
            actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff").Query(f => f.Term(m => m.Field1, "value1") || !f.Term(m => m.Field2, "value2")));
            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)",
                    new ElasticQueryVisitorContext { DefaultOperator = Operator.Or, UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(
                                f =>
                                    f.Term(m => m.Field1, "value1") ||
                                    (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(
                                q =>
                                    q.Bool(
                                        b =>
                                            b.Filter(
                                                f =>
                                                    f.Term(m => m.Field1, "value1") &&
                                                    (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field1:Testing.Casing", new ElasticQueryVisitorContext { UseScoring = true })
                    .Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(m => m.Field1, "Testing.Casing")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field1:\"Blake Niemyjski\"",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term(p => p.Field1, "Blake Niemyjski")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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

            var processor =
                new ElasticQueryParser(c => c.AddVisitor(new UpdateFixedTermFieldToDateFixedExistsQueryVisitor()));
            var result = processor.BuildQueryAsync("fixed:true").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(f => f.Bool(b => b.Filter(filter => filter.Exists(m => m.Field("date_fixed"))))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field1:value1 (field2:value2 OR field3:value3)",
                    new ElasticQueryVisitorContext().SetDefaultOperator(Operator.And).UseScoring()).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(
                                f =>
                                    f.Term(m => m.Field1, "value1") &&
                                    (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedFilterProcessor() {
            var client = GetClient(new ConnectionSettings().DefaultMappingFor<MyNestedType>(t => t.IndexName("stuff")));
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
                new MyNestedType {
                    Field1 = "value1",
                    Field2 = "value2",
                    Nested = {new MyType {Field1 = "value1", Field4 = 4}}
                },
                new MyNestedType {Field1 = "value2", Field2 = "value2"},
                new MyNestedType {Field1 = "value1", Field2 = "value4"}
            });
            client.Refresh(Indices.Index<MyNestedType>());

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result =
                processor.BuildQueryAsync("field1:value1 nested.field1:value1",
                    new ElasticQueryVisitorContext().UseScoring()).Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Query(f => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                                                              &&
                                                              q.Nested(
                                                                  n =>
                                                                      n.Path(p => p.Nested)
                                                                          .Query(
                                                                              q2 =>
                                                                                  q2.Match(
                                                                                      m =>
                                                                                          m.Field("nested.field1")
                                                                                              .Query("value1"))))));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Expected: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);

            result =
                processor.BuildQueryAsync("field1:value1 nested:(field1:value1 field4:4)",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;

            actualResponse = client.Search<MyNestedType>(d => d.Query(q => result));
            actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            expectedResponse =
                client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                                                              &&
                                                              q.Nested(
                                                                  n =>
                                                                      n.Path(p => p.Nested)
                                                                          .Query(
                                                                              q2 =>
                                                                                  q2.Match(
                                                                                      m =>
                                                                                          m.Field("nested.field1")
                                                                                              .Query("value1"))
                                                                                  && q2.Term("nested.field4", "4")))));

            expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void NestedFilterProcessor2() {
            var client = GetClient(new ConnectionSettings().DefaultMappingFor<MyNestedType>(t => t.IndexName("stuff")));
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
                new MyNestedType {
                    Field1 = "value1",
                    Field2 = "value2",
                    Nested = {new MyType {Field1 = "value1", Field4 = 4}}
                },
                new MyNestedType {Field1 = "value2", Field2 = "value2"},
                new MyNestedType {Field1 = "value1", Field2 = "value4", Field3 = "value3"}
            });
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyNestedType>(client).UseNested());
            var result =
                processor.BuildQueryAsync("field1:value1 nested:(field1:value1 field4:4 field3:value3)",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyNestedType>(d => d.Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyNestedType>(d => d.Query(q => q.Match(m => m.Field(e => e.Field1).Query("value1"))
                                                              && q.Nested(n => n.Path(p => p.Nested).Query(q2 =>
                                                                  q2.Match(m => m.Field("nested.field1").Query("value1"))
                                                                  && q2.Term("nested.field4", "4")
                                                                  &&
                                                                  q2.Match(m => m.Field("nested.field3").Query("value3"))))));

            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanGenerateMatchQuery() {
            var client = GetClient();
            client.DeleteIndex("stuff");

            var mapping = new TypeMappingDescriptor<MyType>().Properties(p => p
                .Text(f => f.Name(e => e.Field1).Fields(f1 => f1.Keyword(k => k.Name("keyword").IgnoreAbove(256)))));

            client.CreateIndex("stuff", i => i.Mappings(m => m.Map("mytype", d => mapping)));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var result = processor.BuildQueryAsync("field1:test", new ElasticQueryVisitorContext().UseScoring()).GetAwaiter().GetResult();

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d => d.Index("stuff").Query(q => q.Match(m => m.Field(e => e.Field1).Query("test"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanBuildAliasQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");

            var mapping = new TypeMappingDescriptor<MyType>().Properties(p => p
                .Object<Dictionary<string, object>>(f => f.Name(e => e.Data).Properties(p2 => p2
                    .Text(e => e.Name("@browser").RootAlias("browser"))
                    .Text(e => e.Name("@browser_version").RootAlias("browser.version")))));

            client.CreateIndex("stuff", i => i.Mappings(m => m.Map("mytype", d => mapping)));
            client.Refresh("stuff");

            var visitor = new AliasMappingVisitor(client.Infer);
            var walker = new MappingWalker(visitor);
            walker.Accept(mapping);

            var processor = new ElasticQueryParser(c => c.UseAliases(visitor.RootAliasMap));
            var result =
                processor.BuildQueryAsync("browser.version:1", new ElasticQueryVisitorContext().UseScoring()).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d => d.Index("stuff").Query(q => q.Term(m => m.Field(e => e.Data["@browser_version"]).Value("1"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

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
            var result =
                processor.BuildQueryAsync("field4:[1 TO 2} OR field1:value1",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(
                    d =>
                        d.Index("stuff")
                            .Query(
                                f =>
                                    f.TermRange(m => m.Field(f2 => f2.Field4).GreaterThanOrEquals("1").LessThan("2")) ||
                                    f.Term(m => m.Field1, "value1")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void DateRangeQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(d => d.Dynamic(true).Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field5 = DateTime.UtcNow }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3, Field5 = DateTime.UtcNow }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var ctx = new ElasticQueryVisitorContext { UseScoring = true };
            ctx.Data["TimeZone"] = "America/Chicago";

            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff").SetLoggerFactory(Log));
            var result = processor.BuildQueryAsync("field5:[2017-01-01 TO 2017-01-31} OR field1:value1", ctx).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse =
                client.Search<MyType>(d => d
                    .Index("stuff")
                    .Query(f => f
                        .DateRange(m => m
                            .Field(f2 => f2.Field5).GreaterThanOrEquals("2017-01-01").LessThan("2017-01-31").TimeZone("America/Chicago"))
                                || f.Match(e => e.Field(m => m.Field1).Query("value1"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void SimpleGeoRangeQuery() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");

            client.CreateIndex("stuff");
            client.Map<MyType>(
                d => d.Dynamic(true).Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" },
                i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");

            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseGeo(l => "51.5032520,-0.1278990"));
            var result =
                processor.BuildQueryAsync("field3:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990]",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(q =>
                q.GeoBoundingBox(
                    m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanParseSort() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");
            client.CreateIndex("stuff");
            client.Map<MyType>(
                d => d.Dynamic(true).Index("stuff").Properties(p => p.GeoPoint(g => g.Name(f => f.Field3))
                    .Text(e => e.Name(m => m.Field1).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))
                    .Text(e => e.Name(m => m.Field2).Fields(f2 => f2.Keyword(e1 => e1.Name("keyword")).Keyword(e2 => e2.Name("sort"))))
                ));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" },
                i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");
            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseAliases(aliasMap));
            var sort = processor.BuildSortAsync("geo -field1 -(field2 field3 +field4) (field5 field3)").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Sort(sort));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff")
                .Sort(s => s
                    .Ascending(new Field("field3"))
                    .Descending(new Field("field1.keyword"))
                    .Descending(new Field("field2.sort"))
                    .Descending(new Field("field3"))
                    .Ascending(new Field("field4"))
                    .Ascending(new Field("field5"))
                    .Ascending(new Field("field3"))
                ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);
            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact(Skip = "This currently isn't supported")]
        public void CanParseMixedCaseSort() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");
            client.CreateIndex("stuff");
            client.Map<MyType>(
                d => d.Dynamic(true).Index("stuff").Properties(p => p
                    .Text(e => e.Name(m => m.MultiWord).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))
                ));

            var res = client.Index(new MyType { MultiWord = "value1" }, i => i.Index("stuff"));
            client.Refresh("stuff");
            var processor = new ElasticQueryParser(c => c.UseMappings<MyType>(client, "stuff"));
            var sort = processor.BuildSortAsync("multiWord -multiword").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Sort(sort));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff")
                .Sort(
                    s => s.Ascending(new Field("multiWord.keyword")).Descending(new Field("multiWord.keyword"))
                ));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);
            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GeoRangeQueryProcessor() {
            var client = GetClient();
            client.DeleteIndex("stuff");
            client.Refresh("stuff");
            client.CreateIndex("stuff");
            client.Map<MyType>(
                d =>
                    d.Dynamic(true)
                        .Index("stuff")
                        .Properties(
                            p =>
                                p.GeoPoint(g => g.Name(f => f.Field3))
                                    .Text(e => e.Name(m => m.Field1).Fields(f1 => f1.Keyword(e1 => e1.Name("keyword"))))));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1, Field3 = "51.5032520,-0.1278990" },
                i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));
            client.Refresh("stuff");
            var aliasMap = new AliasMap { { "geo", "field3" } };
            var processor = new ElasticQueryParser(c => c
                .UseMappings<MyType>(client, "stuff")
                .UseGeo(l => "51.5032520,-0.1278990")
                .UseAliases(aliasMap));
            var result =
                processor.BuildQueryAsync(
                    "geo:[51.5032520,-0.1278990 TO 51.5032520,-0.1278990] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi",
                    new ElasticQueryVisitorContext { UseScoring = true }).Result;
            var sort = processor.BuildSortAsync("geo -field1").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result).Sort(sort));
            string actualRequest = actualResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff")
                .Sort(
                    s => s
                        .Ascending("field3")
                        .Descending(new Field("field1.keyword"))
                )
                .Query(q =>
                    q.GeoBoundingBox(
                        m => m.Field(p => p.Field3).BoundingBox("51.5032520,-0.1278990", "51.5032520,-0.1278990"))
                    || q.Match(y => y.Field(e => e.Field1).Query("value1"))
                    || q.TermRange(m => m.Field(g => g.Field2).GreaterThanOrEquals("1").LessThanOrEquals("4"))
                    || !q.GeoDistance(m => m.Field(p => p.Field3).Location("51.5032520,-0.1278990").Distance("75mi"))));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.LogInformation("Actual: {Request}", expectedRequest);
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
        public string MultiWord { get; set; }
        public Dictionary<string, object> Data { get; set; }
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

            if (!Boolean.TryParse(node.Term, out var isFixed))
                return;

            var query = new ExistsQuery { Field = "date_fixed" };
            node.SetQuery(isFixed ? query : !query);
        }
    }
}