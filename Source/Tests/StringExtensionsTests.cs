using System;
using Exceptionless.LuceneQueryParser.Extensions;
using Xunit;

namespace LuceneQueryParser.Tests {
    public sealed class StringExtensionsTests {
        [Theory]
        [InlineData("", "")]
        [InlineData("none", "none")]
        [InlineData(@"Escaped \. in the code", "Escaped . in the code")]
        [InlineData(@"Escap\e", "Escape")]
        [InlineData(@"Double \\ backslash", @"Double \ backslash")]
        [InlineData(@"At end \", @"At end \")]
        public void UnescapingWorks(string test, string expected) {
            var result = test.Unescape();
            Assert.Equal(expected, result);
        }

        [Theory]
        [InlineData("", "")]
        [InlineData("none", "none")]
        [InlineData(@"Lots of characters: +-&|!(){}[]^""~*?\", @"Lots of characters\: +-&|!(){}[]^""~*?\\")]
        public void EscapingWorks(string test, string expected) {
            var result = test.Escape();
            Assert.Equal(expected, result);
        }
    }
}
