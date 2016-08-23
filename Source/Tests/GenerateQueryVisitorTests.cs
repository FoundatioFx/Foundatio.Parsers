using System;
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Xunit;
using Xunit.Abstractions;

namespace Tests {
    public class GenerateQueryVisitorTests {
        private readonly ITestOutputHelper _output;

        public GenerateQueryVisitorTests(ITestOutputHelper output) {
            _output = output;
        }

        [Theory]
        [InlineData(null, null, false)]
        [InlineData(":", null, false)]
        [InlineData("\":\"", "\":\"", true)]
        [InlineData("  \t", "", true)]
        [InlineData("string\"", "string\"", true)]
        [InlineData("\"quoted string\"", "\"quoted string\"", true)]
        [InlineData("criteria", "criteria", true)]
        [InlineData("(criteria)", "(criteria)", true)]
        [InlineData("field:criteria", "field:criteria", true)]
        [InlineData("field :criteria", "field:criteria", true)]
        [InlineData("-criteria", "-criteria", true)]
        [InlineData("criteria1 AND NOT criteria2", "criteria1 AND NOT criteria2", true)]
        [InlineData("criteria1 NOT criteria2", "criteria1 NOT criteria2", true)]
        [InlineData("field:criteria1 NOT field:criteria2", "field:criteria1 NOT field:criteria2", true)]
        [InlineData("criteria1   criteria2", "criteria1 criteria2", true)]
        [InlineData("criteria1 +criteria2", "criteria1 +criteria2", true)]
        [InlineData("criteria1 OR criteria2", "criteria1 OR criteria2", true)]
        [InlineData("criteria1 OR criteria2 OR criteria3", "criteria1 OR criteria2 OR criteria3", true)]
        [InlineData("criteria1 OR (criteria2 AND criteria3)", "criteria1 OR (criteria2 AND criteria3)", true)]
        [InlineData("field:[1 TO 2]", "field:[1 TO 2]", true)]
        [InlineData("field:{1 TO 2}", "field:{1 TO 2}", true)]
        [InlineData("field:[1 TO 2}", "field:[1 TO 2}", true)]
        [InlineData("field:(criteria1 criteria2)", "field:(criteria1 criteria2)", true)]
        [InlineData("data.field:(now criteria2)", "data.field:(now criteria2)", true)]
        [InlineData("field:(criteria1 OR criteria2)", "field:(criteria1 OR criteria2)", true)]
        [InlineData("field:*cr", "field:*cr", true)]
        [InlineData("field:cr*", "field:cr*", true)]
        [InlineData("date:>now", "date:>now", true)]
        [InlineData("date:<now", "date:<now", true)]
        [InlineData("_exists_:title", "_exists_:title", true)]
        [InlineData("book.\\*:(quick brown)", "book.\\*:(quick brown)", true)]
        [InlineData("date:[now/d-4d TO now/d+1d}", "date:[now/d-4d TO now/d+1d}", true)]
        [InlineData("(date:[now/d-4d TO now/d+1d})", "(date:[now/d-4d TO now/d+1d})", true)]
        [InlineData("data.date:>now", "data.date:>now", true)]
        [InlineData("data.date:[now/d-4d TO now/d+1d}", "data.date:[now/d-4d TO now/d+1d}", true)]
        [InlineData("data.date:[2012-01-01 TO 2012-12-31]", "data.date:[2012-01-01 TO 2012-12-31]", true)]
        [InlineData("data.date:[* TO 2012-12-31]", "data.date:[* TO 2012-12-31]", true)]
        [InlineData("data.date:[2012-01-01 TO *]", "data.date:[2012-01-01 TO *]", true)]
        [InlineData("(data.date:[now/d-4d TO now/d+1d})", "(data.date:[now/d-4d TO now/d+1d})", true)]
        [InlineData("criter~", "criter~", true)]
        [InlineData("criter~1", "criter~1", true)]
        [InlineData("criter^2", "criter^2", true)]
        [InlineData("\"blah criter\"~1", "\"blah criter\"~1", true)]
        [InlineData("count:>1", "count:>1", true)]
        [InlineData("book.\\*:test", "book.\\*:test", true)]
        [InlineData("count:>=1", "count:>=1", true)]
        [InlineData("count:[1..5}", "count:[1..5}", true)]
        [InlineData("count:a\\:a", "count:a\\:a", true)]
        [InlineData("count:a:a", null, false)]
        [InlineData("count:a\\:a more:stuff", "count:a\\:a more:stuff", true)]
        [InlineData("data.count:[1..5}", "data.count:[1..5}", true)]
        [InlineData("age:(>=10 AND < 20)", "age:(>=10 AND <20)", true)]
        [InlineData("age : >= 10", "age:>=10", true)]
        [InlineData("age:[1 TO 2]", "age:[1 TO 2]", true)]
        [InlineData("data.Windows-identity:ejsmith", "data.Windows-identity:ejsmith", true)]
        [InlineData("data.age:(>30 AND <=40)", "data.age:(>30 AND <=40)", true)]
        [InlineData("+>=10", "+>=10", true)]
        [InlineData(">=10", ">=10", true)]
        [InlineData("age:(+>=10)", "age:(+>=10)", true)]
        [InlineData("data.age:(+>=10 AND < 20)", "data.age:(+>=10 AND <20)", true)]
        [InlineData("data.age:(+>=10 +<20)", "data.age:(+>=10 +<20)", true)]
        [InlineData("data.age:(->=10 AND < 20)", "data.age:(->=10 AND <20)", true)]
        [InlineData("data.age:[10 TO *]", "data.age:[10 TO *]", true)]
        [InlineData("title:(full text search)^2", "title:(full text search)^2", true)]
        [InlineData("data.age:[* TO 10]", "data.age:[* TO 10]", true)]
        [InlineData("hidden:true AND data.age:(>30 AND <=40)", "hidden:true AND data.age:(>30 AND <=40)", true)]
        [InlineData("hidden:true", "hidden:true", true)]
        [InlineData("geo:\"Dallas, TX\"~75m", "geo:\"Dallas, TX\"~75m", true)]
        [InlineData("geo:\"Dallas, TX\"~75 m", "geo:\"Dallas, TX\"~75 m", true)]
        [InlineData("min:price geogrid:geo~6 count:(category count:subcategory avg:price min:price)", "min:price geogrid:geo~6 count:(category count:subcategory avg:price min:price)", true)]
        [InlineData("-type:404", "-type:404", true)]
        public void CanGenerateQuery(string query, string expected, bool isValid) {
            var parser = new QueryParser();

            IQueryNode result;
            try {
                result = parser.Parse(query);
            } catch (Exception ex) {
                Assert.False(isValid, ex.Message);
                return;
            }

            _output.WriteLine(DebugQueryVisitor.Run(result));
            var generatedQuery = GenerateQueryVisitor.Run(result);
            Assert.Equal(expected, generatedQuery);
        }
    }
}
