using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Pegasus.Common.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class InvertQueryVisitorTests : TestWithLoggingBase
{
    public InvertQueryVisitorTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = LogLevel.Trace;
    }

    [Fact]
    public Task CanInvertTermQuery()
    {
        return InvertAndValidateQuery("value", "(NOT value)", null, true);
    }

    [Fact]
    public Task CanInvertFieldQuery()
    {
        return InvertAndValidateQuery("field:value", "(NOT field:value)", null, true);
    }

    [Fact]
    public Task CanInvertNotFieldQuery()
    {
        return InvertAndValidateQuery("NOT field:value", "field:value", null, true);
    }

    [Fact]
    public Task CanInvertMultipleTermsQuery()
    {
        return InvertAndValidateQuery("field1:value field2:value field3:value", "(NOT (field1:value field2:value field3:value))", null, true);
    }

    [Fact]
    public Task CanInvertOrGroupQuery()
    {
        return InvertAndValidateQuery("(field1:value OR field2:value)", "(NOT (field1:value OR field2:value))", null, true);
    }

    [Fact]
    public Task CanInvertFieldWithNonInvertedFieldQuery()
    {
        return InvertAndValidateQuery("field:value noninvertedfield1:value", "(NOT field:value) noninvertedfield1:value", null, true);
    }

    [Fact]
    public Task CanInvertAlternateCriteria()
    {
        return InvertAndValidateQuery("value", "(is_deleted:true OR (NOT value))", "is_deleted:true", true);
    }

    [Fact]
    public Task CanInvertAlternateCriteriaAndNonInvertedField()
    {
        return InvertAndValidateQuery("noninvertedfield1:value field1:value", "noninvertedfield1:value (is_deleted:true OR (NOT field1:value))", "is_deleted:true", true);
    }

    [Fact]
    public Task CanInvertNonInvertedFieldAndOrGroup()
    {
        return InvertAndValidateQuery("noninvertedfield1:value (field1:value OR field2:value)", "noninvertedfield1:value (NOT (field1:value OR field2:value))", null, true);
    }

    [Fact]
    public Task CanInvertAlternateCriteriaAndNonInvertedFieldAndOrGroup()
    {
        return InvertAndValidateQuery("noninvertedfield1:value (field1:value OR field2:value)", "noninvertedfield1:value (is_deleted:true OR (NOT (field1:value OR field2:value)))", "is_deleted:true", true);
    }

    [Fact]
    public Task CanInvertGroupNonInvertedField()
    {
        return InvertAndValidateQuery("(field1:value noninvertedfield1:value) field2:value", "((NOT field1:value) noninvertedfield1:value) (NOT field2:value)", null, true);
    }

    [Theory]
    [InlineData("noninvertedfield1:value", "noninvertedfield1:value", "is_deleted:true")]
    [InlineData("noninvertedfield1:value1 OR noninvertedfield1:value2", "noninvertedfield1:value1 OR noninvertedfield1:value2", "is_deleted:true")]
    [InlineData("(noninvertedfield1:value)", "(noninvertedfield1:value)", "is_deleted:true")]
    [InlineData("NOT status:fixed", "status:fixed")]
    [InlineData("field:value", "(NOT field:value)")]
    [InlineData("-field:value", "field:value")]
    [InlineData("status:open OR status:regressed", "(NOT (status:open OR status:regressed))")]
    [InlineData("(noninvertedfield1:value AND (noninvertedfield2:value)) field1:value", "(noninvertedfield1:value AND (noninvertedfield2:value)) (NOT field1:value)")]
    [InlineData("field1:value noninvertedfield1:value", "(NOT field1:value) noninvertedfield1:value")]
    [InlineData("field1:value noninvertedfield1:value field2:value", "(NOT field1:value) noninvertedfield1:value (NOT field2:value)")]
    [InlineData("field1:value field2:value field3:value", "(NOT (field1:value field2:value field3:value))")]
    [InlineData("noninvertedfield1:value field1:value field2:value field3:value", "noninvertedfield1:value (NOT (field1:value field2:value field3:value))")]
    [InlineData("noninvertedfield1:value field1:value field2:value field3:value", "noninvertedfield1:value (is_deleted:true OR (NOT (field1:value field2:value field3:value)))", "is_deleted:true")]
    [InlineData("noninvertedfield1:123 (status:open OR status:regressed) noninvertedfield1:234", "noninvertedfield1:123 (NOT (status:open OR status:regressed)) noninvertedfield1:234")]
    [InlineData("first_occurrence:[1609459200000 TO 1609730450521] (noninvertedfield1:537650f3b77efe23a47914f4 (status:open OR status:regressed))", "(NOT first_occurrence:[1609459200000 TO 1609730450521]) (noninvertedfield1:537650f3b77efe23a47914f4 (NOT (status:open OR status:regressed)))")]
    public Task CanInvertQuery(string query, string expected, string alternateInvertedCriteria = null)
    {
        return InvertAndValidateQuery(query, expected, alternateInvertedCriteria, true);
    }

    private async Task InvertAndValidateQuery(string query, string expected, string alternateInvertedCriteria, bool isValid)
    {
#if ENABLE_TRACING
            var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
        var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        IQueryNode result;
        try
        {
            result = await parser.ParseAsync(query);
        }
        catch (FormatException ex)
        {
            Assert.False(isValid, ex.Message);
            return;
        }

        var invertQueryVisitor = new InvertQueryVisitor(["noninvertedfield1", "noninvertedfield2"]);
        var context = new QueryVisitorContext();

        if (!String.IsNullOrWhiteSpace(alternateInvertedCriteria))
        {
            var invertedAlternate = await parser.ParseAsync(alternateInvertedCriteria);
            context.SetAlternateInvertedCriteria(invertedAlternate);
        }

        result = await invertQueryVisitor.AcceptAsync(result, context);
        string invertedQuery = result.ToString();
        string nodes = await DebugQueryVisitor.RunAsync(result);
        _logger.LogInformation("{Result}", nodes);
        Assert.Equal(expected, invertedQuery);
    }
}
