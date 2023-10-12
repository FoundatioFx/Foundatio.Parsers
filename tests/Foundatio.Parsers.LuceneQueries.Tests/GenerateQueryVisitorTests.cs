using System;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;
using Xunit.Abstractions;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Xunit;
using Pegasus.Common.Tracing;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class GenerateQueryVisitorTests : TestWithLoggingBase {
    public GenerateQueryVisitorTests(ITestOutputHelper output) : base(output) {
        Log.MinimumLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("value1 value2", GroupOperator.Default, "value1 value2")]
    [InlineData("value1 value2", GroupOperator.And, "value1 AND value2")]
    [InlineData("value1 value2", GroupOperator.Or, "value1 OR value2")]
    [InlineData("value1 value2 value3", GroupOperator.Default, "value1 value2 value3")]
    [InlineData("value1 value2 value3", GroupOperator.And, "value1 AND value2 AND value3")]
    [InlineData("value1 value2 value3", GroupOperator.Or, "value1 OR value2 OR value3")]
    [InlineData("value1 value2 value3 value4", GroupOperator.And, "value1 AND value2 AND value3 AND value4")]
    [InlineData("(value1 value2) OR (value3 value4)", GroupOperator.And, "(value1 AND value2) OR (value3 AND value4)")]
    public Task DefaultOperatorApplied(string query, GroupOperator groupOperator, string expected) {
        return GenerateQuery(query, groupOperator, expected);
    }

    private async Task GenerateQuery(string query, GroupOperator defaultOperator, string expected) {
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
        var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser {
            Tracer = tracer
        };

        IQueryNode parsedQuery = await parser.ParseAsync(query);

        var context = new QueryVisitorContext { DefaultOperator = defaultOperator };
        string result = GenerateQueryVisitor.Run(parsedQuery, context);
        string nodes = await DebugQueryVisitor.RunAsync(parsedQuery);
        _logger.LogInformation(nodes);
        Assert.Equal(expected, result);
    }
}
