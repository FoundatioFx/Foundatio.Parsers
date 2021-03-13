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
    public class InvertQueryVisitorTests : TestWithLoggingBase {
        public InvertQueryVisitorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
        }

        [Theory]
        [InlineData("value", "NOT (value)")]
        [InlineData("field:value", "NOT (field:value)")]
        [InlineData("field1:value noninvertedfield:value", "NOT (field1:value) noninvertedfield:value")]
        [InlineData("field1:value noninvertedfield:value field2:value", "NOT (field1:value) noninvertedfield:value NOT (field2:value)")]
        [InlineData("(field1:value noninvertedfield:value) field2:value", "NOT ((field1:value noninvertedfield:value) field2:value)")] // non-root level fields will always be inverted
        [InlineData("field1:value field2:value field3:value", "NOT (field1:value field2:value field3:value)")]
        [InlineData("noninvertedfield:value field1:value field2:value field3:value", "noninvertedfield:value NOT (field1:value field2:value field3:value)")]
        public Task CanInvertQuery(string query, string expected) {
            return InvertAndValidateQuery(query, expected, true);
        }

        private async Task InvertAndValidateQuery(string query, string expected, bool isValid) {
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

            string invertedQuery = InvertQueryVisitor.Run(result, new[] { "noninvertedfield" });
            string nodes = await DebugQueryVisitor.RunAsync(result);
            _logger.LogInformation(nodes);
            Assert.Equal(expected, invertedQuery);
        }
    }
}
