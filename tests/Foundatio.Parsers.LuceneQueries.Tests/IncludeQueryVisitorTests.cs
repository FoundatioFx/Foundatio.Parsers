using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;
using Xunit.Abstractions;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests {
    public class IncludeQueryVisitorTests : TestWithLoggingBase {
        public IncludeQueryVisitorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
        }

        [Fact]
        public async Task CanExpandIncludesAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("@include:other");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = await IncludeVisitor.RunAsync(result, includes);
            Assert.Equal("(field:value)", resolved.ToString());
        }

        [Fact]
        public async Task CanSkipIncludes() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("outter @include:other @skipped:(other stuff @include:other)");
            var includes = new Dictionary<string, string> {
                { "other", "field:value" },
                { "nested", "field:value @include:other" }
            };
            var resolved = await IncludeVisitor.RunAsync(result, includes, shouldSkipInclude: (n, ctx) => true);
            Assert.Equal("outter @include:other @skipped:(other stuff @include:other)", resolved.ToString());
            
            resolved = await IncludeVisitor.RunAsync(result, includes, shouldSkipInclude: ShouldSkipInclude);
            Assert.Equal("outter (field:value) @skipped:(other stuff @include:other)", resolved.ToString());
            
            resolved = await IncludeVisitor.RunAsync(result, includes, shouldSkipInclude: (n, ctx) => !ShouldSkipInclude(n, ctx));
            Assert.Equal("outter (field:value) @skipped:(other stuff (field:value))", resolved.ToString());
            
            var nestedResult = await parser.ParseAsync("outter @skipped:(other stuff @include:nested)");
            resolved = await IncludeVisitor.RunAsync(nestedResult, includes, shouldSkipInclude: ShouldSkipInclude);
            Assert.Equal("outter @skipped:(other stuff @include:nested)", resolved.ToString());
        }

        private bool ShouldSkipInclude(TermNode node, IQueryVisitorContext context) {
            var current = node.Parent;
            while (current != null) {
                if (current is GroupNode groupNode && groupNode.Field == "@skipped")
                    return true;
                
                current = current.Parent;
            }

            return false;
        }

        [Fact]
        public async Task CanExpandIncludesWithOtherCriteriaAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:value1 @include:other");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = await IncludeVisitor.RunAsync(result, includes);
            Assert.Equal("field1:value1 (field:value)", resolved.ToString());
        }

        [Fact]
        public async Task CanExpandIncludesWithOtherCriteriaAndGroupingAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("field1:value1 OR (@include:other field2:value2)");
            var includes = new Dictionary<string, string> { { "other", "field:value" } };
            var resolved = await IncludeVisitor.RunAsync(result, includes);
            Assert.Equal("field1:value1 OR ((field:value) field2:value2)", resolved.ToString());
        }

        [Fact]
        public async Task CanExpandNestedIncludesAsync() {
            var parser = new LuceneQueryParser();
            var result = await parser.ParseAsync("@include:other");
            var includes = new Dictionary<string, string> {
                { "other", "@include:other2" },
                { "other2", "field2:value2" }
            };
            var resolved = await IncludeVisitor.RunAsync(result, includes);
            Assert.Equal("((field2:value2))", resolved.ToString());
        }
    }
}
