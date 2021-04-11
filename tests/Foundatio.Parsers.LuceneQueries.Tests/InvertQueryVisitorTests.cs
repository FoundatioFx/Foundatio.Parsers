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
        [InlineData("value", "NOT value")]
        [InlineData("NOT status:fixed", "status:fixed")]
        [InlineData("field:value", "NOT field:value")]
        [InlineData("-field:value", "field:value")]
        [InlineData("status:open OR status:regressed", "NOT (status:open OR status:regressed)")]
        [InlineData("(noninvertedfield1:value AND (noninvertedfield2:value)) field1:value", "(noninvertedfield1:value AND (noninvertedfield2:value)) NOT (field1:value)")]
        [InlineData("field1:value noninvertedfield1:value", "NOT (field1:value) noninvertedfield1:value")]
        [InlineData("field1:value noninvertedfield1:value field2:value", "NOT (field1:value) noninvertedfield1:value NOT (field2:value)")]
        [InlineData("(field1:value noninvertedfield1:value) field2:value", "NOT (field1:value noninvertedfield1:value) NOT (field2:value)")] // non-root level fields will always be inverted
        [InlineData("field1:value field2:value field3:value", "NOT (field1:value field2:value field3:value)")]
        [InlineData("noninvertedfield1:value field1:value field2:value field3:value", "noninvertedfield1:value NOT (field1:value field2:value field3:value)")]
        [InlineData("noninvertedfield1:123 (status:open OR status:regressed) noninvertedfield1:234", "noninvertedfield1:123 NOT (status:open OR status:regressed) noninvertedfield1:234")]
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

            var invertQueryVisitor = new InvertQueryVisitor(new[] { "noninvertedfield1", "noninvertedfield2" });
            result = await invertQueryVisitor.AcceptAsync(result, new QueryVisitorContext());
            string invertedQuery = result.ToString();
            string nodes = await DebugQueryVisitor.RunAsync(result);
            _logger.LogInformation(nodes);
            Assert.Equal(expected, invertedQuery);
        }
    }
}
