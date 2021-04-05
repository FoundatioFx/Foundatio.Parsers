using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;
using Xunit.Abstractions;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Xunit;
using Pegasus.Common.Tracing;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.LuceneQueries.Tests {
    public class CleanupQueryVisitorTests : TestWithLoggingBase {
        public CleanupQueryVisitorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
        }

        [Theory]
        [InlineData("value", "value")]
        [InlineData("(value)", "value")]
        [InlineData("((value))", "value")]
        [InlineData("(((value)))", "value")]
        [InlineData("((value )    )", "value")]
        [InlineData("test:(value)", "test:(value)")]
        [InlineData("test:((value))", "test:(value)")]
        [InlineData("NOT value", "NOT value")]
        [InlineData("NOT (value)", "NOT value")]
        [InlineData("NOT (status:fixed)", "NOT status:fixed")]
        [InlineData("project:123 NOT ((status:open OR status:regressed))", "project:123 NOT (status:open OR status:regressed)")]
        public Task CanInvertQuery(string query, string expected) {
            return CleanupAndValidateQuery(query, expected, true);
        }

        private async Task CleanupAndValidateQuery(string query, string expected, bool isValid) {
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
            var parser = new LuceneQueryParser {
                Tracer = tracer
            };

            IQueryNode result;
            try {
                result = await parser.ParseAsync(query);
            } catch (FormatException ex) {
                Assert.False(isValid, ex.Message);
                return;
            }

            string CleanedQuery = CleanupQueryVisitor.Run(result);
            string nodes = await DebugQueryVisitor.RunAsync(result);
            _logger.LogInformation(nodes);
            Assert.Equal(expected, CleanedQuery);
        }
    }
}
