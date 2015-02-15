using System.Diagnostics;
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Visitor;
using Xunit;
using Xunit.Extensions;

namespace Tests {
    public class GenerateQueryVisitorTests {
        [Theory]
        [InlineData("string\"")]
        [InlineData("\"quoted string\"")]
        [InlineData("criteria")]
        [InlineData("(criteria)")]
        [InlineData("field:criteria")]
        [InlineData("-criteria")]
        [InlineData("criteria1 criteria2")]
        [InlineData("criteria1 +criteria2")]
        [InlineData("criteria1 OR criteria2")]
        [InlineData("criteria1 OR criteria2 OR criteria3")]
        [InlineData("criteria1 OR (criteria2 AND criteria3)")]
        [InlineData("field:[1 TO 2]")]
        [InlineData("field:{1 TO 2}")]
        [InlineData("field:[1 TO 2}")]
        [InlineData("data.Windows-identity:ejsmith")]
        [InlineData("field:(criteria1 criteria2)")]
        [InlineData("field:(criteria1 OR criteria2)")]
        [InlineData("date:>now")]
        [InlineData("date:>now")]
        [InlineData("date:>now")]
        [InlineData("_exists_:title")]
        [InlineData("book.\\*:(quick brown)")]
        [InlineData("date:[now/d-4d TO now/d+1d}")]
        [InlineData("(date:[now/d-4d TO now/d+1d})")]
        [InlineData("criter~")]
        [InlineData("criter~1")]
        [InlineData("criter^2")]
        [InlineData("\"blah criter\"~1")]
        [InlineData("count:[1..5}")]
        [InlineData("age:(>=10 AND < 20)")]
        public void CanGenerateQuery(string query) {
            var parser = new QueryParser();
            var result = parser.Parse(query);
            Debug.WriteLine(DebugQueryVisitor.Run(result));
            var generatedQuery = GenerateQueryVisitor.Run(result);
            Assert.Equal(query, generatedQuery);
        }
    }
}
