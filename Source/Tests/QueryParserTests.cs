using System;
using System.Collections.Generic;
using ElasticMacros.Visitor;
using Xunit;
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;

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
        public void ElasticMacros() {
            var parser = new QueryParser();
            var result = parser.Parse("field1:value1 (field2:value2 OR field3:value3)");
            var filterContainer = FilterContainerVisitor.Run(result);
            //var settings = new JsonSerializerSettings {
            //    ContractResolver = new ElasticContractResolver(new ConnectionSettings(), new List<Func<Type, JsonConverter>>()),
            //    Formatting = Formatting.Indented,
            //    DefaultValueHandling = DefaultValueHandling.Ignore
            //};

            //var blah = +new TermQuery { Field = "field1", Value = "value1" } && (+new TermQuery { Field = "field2", Value = "value2" } || +new TermQuery { Field = "field3", Value = "field3" });
            //string json = JsonConvert.SerializeObject(blah, settings);

            //var parser = new ElasticMacros.Parser();
            //var result = parser.Parse("criteria");
            //var r = parser.Parse("geogrid:75044~25 avg:somefield~1");
            //r = parser.Parse("count:category (count:subcategory)");
            //r = parser.Parse("count:(category count:subcategory)");
            //Assert.NotNull(result);
            //Assert.NotNull(result.Left);
            //Assert.IsType<TermNode>(result.Left);
            //Assert.Equal("criteria", ((TermNode)result.Left).Term);
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
    }
}
