using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests {
    [Trait("TestType", "Unit")]
    public class QueryParserUnitTests {

        public static IEnumerable<object[]> QueriesAndExpectedStringOutput => new[]
        {
            new object[] { "NOT someField:stuff" },
            new object[] { "NOT -someField:stuff" },
            new object[] { "NOT someField:(stuff)" },
            new object[] { "NOT someField:(NOT stuff)" },
            new object[] { "NOT -someField:(stuff)" },
        };

        [Theory]
        [MemberData(nameof(QueriesAndExpectedStringOutput))]
        public void  CanParseQueriesAndOutputWithNoChanges(string expectedQuery) {
            var sut = new LuceneQueryParser();
            
            var rootNode = sut.Parse(expectedQuery);
            var actualResult = rootNode.ToString();

            Assert.Equal(expectedQuery, actualResult);
        }
    }
}
