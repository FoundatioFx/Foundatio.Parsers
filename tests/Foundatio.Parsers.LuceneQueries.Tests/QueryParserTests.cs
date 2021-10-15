#define ENABLE_TRACING

using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Pegasus.Common;
using Pegasus.Common.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.LuceneQueries.Tests {
    [Trait("TestType", "Unit")]
    public class QueryParserTests : TestWithLoggingBase {
        public QueryParserTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
        }

        [Fact]
        public async Task CanUseForwardSlashes() {
            string query = @"hey/now";
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
            var parser = new LuceneQueryParser {
                Tracer = tracer
            };

            try {
                var result = await parser.ParseAsync(query);

                _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
                string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
                Assert.Equal(query, generatedQuery);
            } catch (FormatException ex) {
                _logger.LogInformation(tracer.ToString());
                var cursor = ex.Data["cursor"] as Cursor;
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }
        }

        [Fact]
        public async Task CanUseSpaceInFieldNames() {
            string query = @"a\ b:c";
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
            var parser = new LuceneQueryParser {
                Tracer = tracer
            };

            try {
                var result = await parser.ParseAsync(query);

                _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
                string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
                Assert.Equal(query, generatedQuery);

                var groupNode = result as GroupNode;
                Assert.NotNull(groupNode);
                var leftNode = groupNode.Left as TermNode;
                Assert.NotNull(leftNode);
                Assert.Equal("a b", leftNode.UnescapedField);
            } catch (FormatException ex) {
                _logger.LogInformation(tracer.ToString());
                var cursor = ex.Data["cursor"] as Cursor;
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }
        }

        [Fact]
        public async Task CanParseRegex() {
            string query = @"/\(\[A-Za-z\/\]+\).*?/";
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
            var parser = new LuceneQueryParser {
                Tracer = tracer
            };

            try {
                var result = await parser.ParseAsync(query);

                _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
                string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
                Assert.Equal(query, generatedQuery);

                var groupNode = result as GroupNode;
                Assert.NotNull(groupNode);
                var leftNode = groupNode.Left as TermNode;
                Assert.NotNull(leftNode);
                Assert.True(leftNode.IsRegexTerm);
            } catch (FormatException ex) {
                var cursor = ex.Data["cursor"] as Cursor;
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }
        }

        [Fact]
        public void CanHandleUnterminatedRegex() {
            var sut = new LuceneQueryParser();

            var ex = Assert.Throws<FormatException>(() => {
                var result = sut.Parse(@"/\(\[A-Za-z\/\]+\).*?");
                var ast = DebugQueryVisitor.Run(result);
            });
            Assert.Contains("Unterminated regex", ex.Message);
        }

        [Fact]
        public async Task CanParseFieldRegex() {
            string query = @"myfield:/\(\[A-Za-z\]+\).*?/";
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
            var parser = new LuceneQueryParser {
                Tracer = tracer
            };

            try {
                var result = await parser.ParseAsync(query);

                _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
                string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
                Assert.Equal(query, generatedQuery);

                var groupNode = result as GroupNode;
                Assert.NotNull(groupNode);
                var leftNode = groupNode.Left as TermNode;
                Assert.NotNull(leftNode);
                Assert.Equal("myfield", leftNode.Field);
                Assert.True(leftNode.IsRegexTerm);
            } catch (FormatException ex) {
                var cursor = ex.Data["cursor"] as Cursor;
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }
        }

        [Fact]
        public void CanParseQueryConcurrently() {
            var parser = new LuceneQueryParser();
            Parallel.For(0, 100, i => {
                var result = parser.Parse("criteria   some:criteria blah:(more      stuff)");
                Assert.NotNull(result);
            });
        }

        [Fact]
        public void CanParseNotBeforeParens() {
            var sut = new LuceneQueryParser();

            var result = sut.Parse("NOT (dog parrot)");
            var ast = DebugQueryVisitor.Run(result);

            Assert.IsType<GroupNode>(result.Left);
            Assert.True((result.Left as GroupNode).HasParens);
            Assert.True((result.Left as GroupNode).IsNegated);
        }

        [Fact]
        public void CanParseEmptyQuotes() {
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
            var parser = new LuceneQueryParser {
                Tracer = tracer
            };

            var result = parser.Parse("\"\"");
            var ast = DebugQueryVisitor.Run(result);

            Assert.IsType<TermNode>(result.Left);
            Assert.True((result.Left as TermNode).IsQuotedTerm);
            Assert.Empty((result.Left as TermNode).Term);
        }

        [Fact]
        public void MultipleOperatorsIsNotValid() {
            var sut = new LuceneQueryParser();

            Assert.Throws<FormatException>(() => {
                var result = sut.Parse("something AND NOT OR otherthing");
                var ast = DebugQueryVisitor.Run(result);
            });
        }

        [Fact]
        public void DoubleOperatorsIsNotValid() {
            var sut = new LuceneQueryParser();

            Assert.Throws<FormatException>(() => {
                var result = sut.Parse("something AND OR otherthing");
                var ast = DebugQueryVisitor.Run(result);
            });
        }

        [Fact]
        public void PrefixWithoutImmediateExpressionIsInvalid() {
            var sut = new LuceneQueryParser();

            var ex = Assert.Throws<FormatException>(() => {
                var result = sut.Parse("something + other");
                var ast = DebugQueryVisitor.Run(result);
            });
            Assert.Contains("Unexpected character '+'.", ex.Message);
        }

        [Fact]
        public void UnterminatedQuotedStringIsNotValid() {
            var sut = new LuceneQueryParser();

            var ex = Assert.Throws<FormatException>(() => {
                var result = sut.Parse("\"something");
                var ast = DebugQueryVisitor.Run(result);
            });
            Assert.Contains("Unterminated quoted string", ex.Message);
        }

        [Fact]
        public void DoubleUnterminatedQuotedStringIsNotValid() {
            var sut = new LuceneQueryParser();

            var ex = Assert.Throws<FormatException>(() => {
                var result = sut.Parse("\"something\"\"");
                var ast = DebugQueryVisitor.Run(result);
            });
            Assert.Contains("Unterminated quoted string", ex.Message);
        }

        [Fact]
        public void UnterminatedParensIsNotValid() {
            var sut = new LuceneQueryParser();

            var ex = Assert.Throws<FormatException>(() => {
                var result = sut.Parse("(something");
            });
            Assert.Contains("Missing closing paren ')' for group expression", ex.Message);
        }

        [Fact]
        public async Task CanGenerateSingleQueryAsync() {
            string query = "datehistogram:(date~2^-5\\:30 min:date max:date)";
            string expected = "datehistogram:(date~2^-5\\:30 min:date max:date)";
            var parser = new LuceneQueryParser();

            var result = await parser.ParseAsync(query);

            _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(expected, generatedQuery);

            await new AssignOperationTypeVisitor().AcceptAsync(result, null);
            _logger.LogInformation(await DebugQueryVisitor.RunAsync(result));
        }
    }

    public class TestQueryVisitor : ChainableQueryVisitor {
        public int GroupNodeCount { get; private set; } = 0;

        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            GroupNodeCount++;
            return base.VisitAsync(node, context);
        }
    }
}
