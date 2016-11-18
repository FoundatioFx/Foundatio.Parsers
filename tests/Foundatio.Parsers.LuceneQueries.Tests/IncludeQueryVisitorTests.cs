using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;
using System.Collections.Generic;

namespace Foundatio.Parsers.Tests {
    public class IncludeQueryVisitorTests {
        [Fact]
        public void CanExpandIncludes() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("@include:other");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("field:value", resolved.ToString());
        }

        [Fact]
        public void CanExpandIncludesWithOtherCriteria() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("field1:value1 @include:other");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("field1:value1 field:value", resolved.ToString());
        }

        [Fact]
        public void CanExpandIncludesWithOtherCriteriaAndGrouping() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("field1:value1 OR (@include:other field2:value2)");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("field1:value1 OR (field:value field2:value2)", resolved.ToString());
        }

        [Fact]
        public void CanExpandNestedIncludes() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("@include:other");
            var includes = new Dictionary<string, string> {
                { "other", "@include:other2" },
                { "other2", "field2:value2" }
            };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("field2:value2", resolved.ToString());
        }
    }
}
