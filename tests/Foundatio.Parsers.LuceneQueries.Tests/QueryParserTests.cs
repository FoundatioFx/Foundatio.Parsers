#define ENABLE_TRACING

using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Pegasus.Common;
using Pegasus.Common.Tracing;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.LuceneQueries.Tests;

[Trait("TestType", "Unit")]
public class QueryParserTests : TestWithLoggingBase
{
    public QueryParserTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultMinimumLevel = LogLevel.Trace;
    }

    [Fact]
    public async Task CanUseForwardSlashes()
    {
        string query = @"hey/now";
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        try
        {
            var result = await parser.ParseAsync(query);

            _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(query, generatedQuery);
        }
        catch (FormatException ex)
        {
            _logger.LogInformation("{Result}", tracer.ToString());
            var cursor = ex.Data["cursor"] as Cursor;
            if (cursor != null)
            {
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }

            throw;
        }
    }

    [Fact]
    public async Task CanUseSpaceInFieldNames()
    {
        string query = @"a\ b:c";
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        try
        {
            var result = await parser.ParseAsync(query);

            _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(query, generatedQuery);

            var groupNode = result as GroupNode;
            Assert.NotNull(groupNode);
            var leftNode = groupNode.Left as TermNode;
            Assert.NotNull(leftNode);
            Assert.Equal("a b", leftNode.UnescapedField);
        }
        catch (FormatException ex)
        {
            _logger.LogInformation("{Result}", tracer.ToString());
            var cursor = ex.Data["cursor"] as Cursor;
            if (cursor != null)
            {
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }

            throw;
        }
    }

    [Fact]
    public async Task CanParseRegex()
    {
        string query = @"/\(\[A-Za-z\/\]+\).*?/";
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        try
        {
            var result = await parser.ParseAsync(query);

            _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(query, generatedQuery);

            var groupNode = result as GroupNode;
            Assert.NotNull(groupNode);
            var leftNode = groupNode.Left as TermNode;
            Assert.NotNull(leftNode);
            Assert.True(leftNode.IsRegexTerm);
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            if (cursor != null)
            {
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }

            throw;
        }
    }

    [Fact]
    public void DataBackslashShouldBeValidBeginningOfString()
    {
        var sut = new LuceneQueryParser();
        var result = sut.Parse("\"\\something\"");
        _logger.LogInformation(DebugQueryVisitor.Run(result));
    }

    [Fact]
    public void CanHandleDateRange()
    {
#if FALSE
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
        var tracer = NullTracer.Instance;
#endif
        var sut = new LuceneQueryParser {
            Tracer = tracer
        };
        string query = "mydate:[now/d TO now/d+30d/d]";
        var result = sut.Parse(query);
        _logger.LogInformation(DebugQueryVisitor.Run(result));
    }

    [Fact]
    public void CanHandleEmpty()
    {
        var sut = new LuceneQueryParser();

        var result = sut.Parse(String.Empty);
        Assert.NotNull(result);
        Assert.Null(result.Left);
        Assert.Null(result.Right);
    }

    [Fact]
    public void CanHandleUnterminatedRegex()
    {
        var sut = new LuceneQueryParser();

        var ex = Assert.Throws<FormatException>(() =>
        {
            var result = sut.Parse(@"/\(\[A-Za-z\/\]+\).*?");
            string ast = DebugQueryVisitor.Run(result);
        });
        Assert.Contains("Unterminated regex", ex.Message);
    }

    [Fact]
    public async Task CanParseFieldRegex()
    {
        string query = @"myfield:/\(\[A-Za-z\]+\).*?/";
#if ENABLE_TRACING
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
            var tracer = NullTracer.Instance;
#endif
        var parser = new LuceneQueryParser
        {
            Tracer = tracer
        };

        try
        {
            var result = await parser.ParseAsync(query);

            _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
            string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
            Assert.Equal(query, generatedQuery);

            var groupNode = result as GroupNode;
            Assert.NotNull(groupNode);
            var leftNode = groupNode.Left as TermNode;
            Assert.NotNull(leftNode);
            Assert.Equal("myfield", leftNode.Field);
            Assert.True(leftNode.IsRegexTerm);
        }
        catch (FormatException ex)
        {
            var cursor = ex.Data["cursor"] as Cursor;
            if (cursor != null)
            {
                throw new FormatException($"[{cursor.Line}:{cursor.Column}] {ex.Message}", ex);
            }

            throw;
        }
    }

    [Fact]
    public void CanParseQueryConcurrently()
    {
        var parser = new LuceneQueryParser();
        Parallel.For(0, 100, _ =>
        {
            var result = parser.Parse("criteria   some:criteria blah:(more      stuff)");
            Assert.NotNull(result);
        });
    }

    [Fact]
    public void CanParseNotBeforeParens()
    {
        var sut = new LuceneQueryParser();

        var result = sut.Parse("NOT (dog parrot)");
        string ast = DebugQueryVisitor.Run(result);

        Assert.IsType<GroupNode>(result.Left);
        Assert.True((result.Left as GroupNode).HasParens);
        Assert.True((result.Left as GroupNode).IsNegated);
    }

    [Fact]
    public void CanParsePrefix()
    {
        var sut = new LuceneQueryParser();

        var result = sut.Parse(@"""jakarta apache"" !""Apache Lucene""");
        string ast = DebugQueryVisitor.Run(result);

        var left = result.Left as TermNode;
        var right = result.Right as TermNode;
        Assert.NotNull(left);
        Assert.NotNull(right);
        Assert.Equal("jakarta apache", left.Term);
        Assert.Null(left.Prefix);
        Assert.False(left.IsExcluded());
        Assert.Equal("Apache Lucene", right.Term);
        Assert.Equal("!", right.Prefix);
        Assert.True(right.IsExcluded());

        result = sut.Parse(@"""jakarta apache"" -""Apache Lucene""");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermNode;
        right = result.Right as TermNode;
        Assert.NotNull(left);
        Assert.NotNull(right);
        Assert.Equal("jakarta apache", left.Term);
        Assert.Equal("Apache Lucene", right.Term);
        Assert.Equal("-", right.Prefix);
        Assert.True(right.IsExcluded());
    }

    [Fact]
    public void CanParseRanges()
    {
        var sut = new LuceneQueryParser();

        var result = sut.Parse("[1 TO 2]");
        string ast = DebugQueryVisitor.Run(result);

        var left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.True(left.MinInclusive);
        Assert.False(left.IsMinQuotedTerm);
        Assert.True(left.MaxInclusive);
        Assert.False(left.IsMaxQuotedTerm);

        result = sut.Parse("{1 TO 2]");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.True(left.MaxInclusive);

        result = sut.Parse("{1 TO 2}");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.False(left.MaxInclusive);

        result = sut.Parse("[1 TO 2}");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.True(left.MinInclusive);
        Assert.False(left.MaxInclusive);

        result = sut.Parse(@"[ ""1"" TO ""2""]");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.True(left.MinInclusive);
        Assert.True(left.IsMinQuotedTerm);
        Assert.True(left.MaxInclusive);
        Assert.True(left.IsMaxQuotedTerm);
        Assert.Equal("1", left.Min);
        Assert.Equal("2", left.Max);

        result = sut.Parse(@">1");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.False(left.IsMinQuotedTerm);
        Assert.False(left.MaxInclusive);
        Assert.False(left.IsMaxQuotedTerm);
        Assert.Equal("1", left.Min);
        Assert.Null(left.Max);

        result = sut.Parse(@">=1");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.True(left.MinInclusive);
        Assert.False(left.IsMinQuotedTerm);
        Assert.False(left.MaxInclusive);
        Assert.False(left.IsMaxQuotedTerm);
        Assert.Equal("1", left.Min);
        Assert.Null(left.Max);

        result = sut.Parse(@"<1");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.False(left.IsMinQuotedTerm);
        Assert.False(left.MaxInclusive);
        Assert.False(left.IsMaxQuotedTerm);
        Assert.Null(left.Min);
        Assert.Equal("1", left.Max);

        result = sut.Parse(@"<=1");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.False(left.IsMinQuotedTerm);
        Assert.True(left.MaxInclusive);
        Assert.False(left.IsMaxQuotedTerm);
        Assert.Null(left.Min);
        Assert.Equal("1", left.Max);

        result = sut.Parse(@">""1""");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.True(left.IsMinQuotedTerm);
        Assert.False(left.MaxInclusive);
        Assert.False(left.IsMaxQuotedTerm);
        Assert.Equal("1", left.Min);
        Assert.Null(left.Max);

        result = sut.Parse(@"<""1""");
        ast = DebugQueryVisitor.Run(result);

        left = result.Left as TermRangeNode;
        Assert.NotNull(left);
        Assert.False(left.MinInclusive);
        Assert.False(left.IsMinQuotedTerm);
        Assert.False(left.MaxInclusive);
        Assert.True(left.IsMaxQuotedTerm);
        Assert.Null(left.Min);
        Assert.Equal("1", left.Max);
    }

    [Fact]
    public void CanParseEmptyQuotes()
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

        var result = parser.Parse("\"\"");
        string ast = DebugQueryVisitor.Run(result);

        Assert.IsType<TermNode>(result.Left);
        Assert.True(((TermNode)result.Left).IsQuotedTerm);
        Assert.Empty(((TermNode)result.Left).Term);
    }

    [Fact]
    public void MultipleOperatorsIsNotValid()
    {
        var sut = new LuceneQueryParser();

        Assert.Throws<FormatException>(() =>
        {
            var result = sut.Parse("something AND NOT OR otherthing");
            string ast = DebugQueryVisitor.Run(result);
        });
    }

    [Fact]
    public void DoubleOperatorsIsNotValid()
    {
        var sut = new LuceneQueryParser();

        Assert.Throws<FormatException>(() =>
        {
            var result = sut.Parse("something AND OR otherthing");
            string ast = DebugQueryVisitor.Run(result);
        });
    }

    [Fact]
    public void UnterminatedQuotedStringIsNotValid()
    {
        var sut = new LuceneQueryParser();

        var ex = Assert.Throws<FormatException>(() =>
        {
            var result = sut.Parse("\"something");
            string ast = DebugQueryVisitor.Run(result);
        });
        Assert.Contains("Unterminated quoted string", ex.Message);
    }

    [Fact]
    public void DoubleUnterminatedQuotedStringIsNotValid()
    {
        var sut = new LuceneQueryParser();

        var ex = Assert.Throws<FormatException>(() =>
        {
            var result = sut.Parse("\"something\"\"");
            string ast = DebugQueryVisitor.Run(result);
        });
        Assert.Contains("Unterminated quoted string", ex.Message);
    }

    [Fact]
    public void UnterminatedParensIsNotValid()
    {
        var sut = new LuceneQueryParser();

        var ex = Assert.Throws<FormatException>(() => sut.Parse("(something"));
        Assert.Contains("Missing closing paren ')' for group expression", ex.Message);
    }

    [Fact]
    public async Task CanGenerateSingleQueryAsync()
    {
        string query = "datehistogram:(date~2^-5\\:30 min:date max:date)";
        string expected = "datehistogram:(date~2^-5\\:30 min:date max:date)";
        var parser = new LuceneQueryParser();

        var result = await parser.ParseAsync(query);

        _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
        string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
        Assert.Equal(expected, generatedQuery);

        await new AssignOperationTypeVisitor().AcceptAsync(result, null);
        _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
    }
}

public class TestQueryVisitor : ChainableQueryVisitor
{
    public int GroupNodeCount { get; private set; } = 0;

    public override Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        GroupNodeCount++;
        return base.VisitAsync(node, context);
    }
}
