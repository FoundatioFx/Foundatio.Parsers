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
            Assert.NotNull(result);
            Assert.NotNull(result.Left);
            Assert.IsType<TermNode>(result.Left);
            Assert.Equal("criteria", ((TermNode)result.Left).Term);
        }
    }
}
