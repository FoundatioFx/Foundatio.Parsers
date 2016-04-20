using System;
using Xunit;
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Tests {
    public class QueryParserTests {
        [Fact]
        public void CanParseQuery() {
            var parser = new QueryParser();
            var result = parser.Parse("criteria");
            var r = parser.Parse("geogrid:75044~25 avg:somefield~1");
            r = parser.Parse("count:category (count:subcategory)");
            r = parser.Parse("count:(category count:subcategory)");
            Assert.NotNull(result);
            Assert.NotNull(result.Left);
            Assert.IsType<TermNode>(result.Left);
            Assert.Equal("criteria", ((TermNode)result.Left).Term);
        }
    }
}
