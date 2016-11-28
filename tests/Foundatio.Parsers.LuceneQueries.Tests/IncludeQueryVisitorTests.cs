using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;
using System.Collections.Generic;
using Nest;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Logging.Xunit;
using Xunit.Abstractions;
using Foundatio.Logging;

namespace Foundatio.Parsers.Tests {
    public class IncludeQueryVisitorTests : TestWithLoggingBase {
        public IncludeQueryVisitorTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void CanExpandIncludes() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("@include:other");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("(field:value)", resolved.ToString());
        }

        [Fact]
        public void CanExpandIncludesWithOtherCriteria() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("field1:value1 @include:other");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("field1:value1 (field:value)", resolved.ToString());
        }

        [Fact]
        public void CanExpandIncludesWithOtherCriteriaAndGrouping() {
            var parser = new LuceneQueryParser();
            var result = parser.Parse("field1:value1 OR (@include:other field2:value2)");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = IncludeVisitor.Run(result, includes);
            Assert.Equal("field1:value1 OR ((field:value) field2:value2)", resolved.ToString());
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
            Assert.Equal("((field2:value2))", resolved.ToString());
        }

        [Fact]
        public void CanExpandElasticIncludes() {
            var client = new ElasticClient();
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var aliases = new AliasMap { { "field", "aliased" } };

            var processor = new ElasticQueryParser(c => c.UseIncludes(includes).UseAliases(aliases));
            var result = processor.BuildQueryAsync("@include:other").Result;
            var actualResponse = client.Search<MyType>(d => d.Index("stuff").Query(q => result));
            string actualRequest = actualResponse.GetRequest();
            _logger.Info($"Actual: {actualRequest}");

            var expectedResponse = client.Search<MyType>(d => d.Index("stuff").Query(f => f.Term("aliased", "value")));
            string expectedRequest = expectedResponse.GetRequest();
            _logger.Info($"Expected: {expectedRequest}");

            Assert.Equal(expectedRequest, actualRequest);
            Assert.Equal(expectedResponse.Total, actualResponse.Total);
        }
    }
}
