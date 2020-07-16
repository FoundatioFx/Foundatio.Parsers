using System;
using System.Collections.Generic;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;
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
            new object[] { "something AND NOT otherthing" },
            new object[] { "something AND otherthing" },
            new object[] { "something OR otherthing" },
        };

        [Theory]
        [MemberData(nameof(QueriesAndExpectedStringOutput))]
        public void  CanParseQueriesAndOutputWithNoChanges(string expectedQuery) {
            var sut = new LuceneQueryParser();
            
            var rootNode = sut.Parse(expectedQuery);
            var actualResult = rootNode.ToString();

            Assert.Equal(expectedQuery, actualResult);
        }

        [Fact]
        public void CanParseNotBeforeParens() {
            var sut = new LuceneQueryParser();

            var result = sut.Parse("NOT (dog parrot)");

            Assert.IsType<GroupNode>(result.Left);
            Assert.True((result.Left as GroupNode).HasParens);
            Assert.True((result.Left as GroupNode).IsNegated);
        }

        [Fact]
        public void MultipleOperatorsIsNotValid() {
            var sut = new LuceneQueryParser();

            Assert.Throws<FormatException>(() => {
                var result = sut.Parse("something AND NOT OR otherthing");
            });
        }

        [Fact]
        public void DoubleOperatorsIsNotValid() {
            var sut = new LuceneQueryParser();

            Assert.Throws<FormatException>(() => {
                var result = sut.Parse("something AND OR otherthing");
            });
        }
    }
}
