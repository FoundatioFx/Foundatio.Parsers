using System;
using System.Collections.Generic;
using System.Text;
using ElasticMacros;
using Elasticsearch.Net;
using Xunit;
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace Tests {
    public class QueryParserTests {
        [Fact]
        public void CanParseQuery() {
            var parser = new QueryParser();
            var result = parser.Parse("criteria");
            Assert.NotNull(result);
            Assert.NotNull(result.Left);
            Assert.IsType<TermNode>(result.Left);
            Assert.Equal("criteria", ((TermNode)result.Left).Term);
        }

        [Fact]
        public void SimpleQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value2", Field2 = "value2" }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field2 = "value4" }, i => i.Index("stuff"));

            var processor = new ElasticMacroProcessor();
            var filterContainer = processor.Process("field1:value1");
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(filterContainer));
            string actualRequest = GetRequest(actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1")));
            string expectedRequest = GetRequest(expectedResponse);

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

            var processor = new ElasticMacroProcessor();
            var filterContainer = processor.Process("field1:value1 (field2:value2 OR field3:value3)");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(filterContainer));
            string actualRequest = GetRequest(actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Term(m => m.Field1, "value1") && (f.Term(m => m.Field2, "value2") || f.Term(m => m.Field3, "value3"))));
            string expectedRequest = GetRequest(expectedResponse);

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

            var processor = new ElasticMacroProcessor();
            var filterContainer = processor.Process("field4:[1 TO 2} OR field1:value1");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(filterContainer));
            string actualRequest = GetRequest(actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.Range(m => m.OnField(f2 => f2.Field4).GreaterOrEquals(1).Lower(2)) || f.Term(m => m.Field1, "value1")));
            string expectedRequest = GetRequest(expectedResponse);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void GeoRangeQueryProcessor() {
            var client = new ElasticClient();
            client.DeleteIndex(i => i.Index("stuff"));
            client.Refresh();

            client.CreateIndex(i => i.Index("stuff"));
            client.Map<MyType>(d => d.Dynamic().Index("stuff"));
            var res = client.Index(new MyType { Field1 = "value1", Field4 = 1 }, i => i.Index("stuff"));
            client.Index(new MyType { Field4 = 2 }, i => i.Index("stuff"));
            client.Index(new MyType { Field1 = "value1", Field4 = 3 }, i => i.Index("stuff"));

            var processor = new ElasticMacroProcessor(c => c
                .UseGeoRanges("field4")
                .UseAliases(name => name == "geo" ? "field4" : name));
            var filterContainer = processor.Process("geo:[9 TO d] OR field1:value1");

            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Filter(filterContainer));
            string actualRequest = GetRequest(actualResponse);
            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Filter(f => f.GeoBoundingBox(m => m.Field4, "9", "d") || f.Term(m => m.Field1, "value1")));
            string expectedRequest = GetRequest(expectedResponse);

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }

        [Fact]
        public void CanUseAliases() {
            var parser = new QueryParser();
            var result = parser.Parse("field1:value");
            var aliasMap = new Dictionary<string, string>();
            aliasMap.Add("field1", "field2");
            var aliased = AliasedQueryVisitor.Run(result, aliasMap);
            Assert.Equal("field2:value", aliased.ToString());
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
}
