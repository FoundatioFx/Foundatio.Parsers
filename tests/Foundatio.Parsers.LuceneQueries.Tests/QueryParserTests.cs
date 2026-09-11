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

namespace Foundatio.Parsers.LuceneQueries.Tests;

[Trait("TestType", "Unit")]
public class QueryParserTests : TestWithLoggingBase
{
    public QueryParserTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = LogLevel.Trace;
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
            Assert.NotNull(result);

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
            Assert.NotNull(result);

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
            Assert.NotNull(result);

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
    public async Task CanHandleDateRange()
    {
#if FALSE
        var tracer = new LoggingTracer(_logger, reportPerformance: true);
#else
        var tracer = NullTracer.Instance;
#endif
        var sut = new LuceneQueryParser
        {
            Tracer = tracer
        };
        string query = "mydate:[now/d TO now/d+30d/d]";
        var result = sut.Parse(query);
        Assert.NotNull(result);
        _logger.LogInformation(DebugQueryVisitor.Run(result));

        string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
        Assert.Equal(query, generatedQuery);

        var groupNode = result as GroupNode;
        Assert.NotNull(groupNode);
        var rangeNode = groupNode.Left as TermRangeNode;
        Assert.NotNull(rangeNode);
        Assert.Equal("mydate", rangeNode.Field);
        Assert.Equal("now/d", rangeNode.Min);
        Assert.Equal("now/d+30d/d", rangeNode.Max);
        Assert.True(rangeNode.MinInclusive);
        Assert.True(rangeNode.MaxInclusive);
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
            Assert.NotNull(result);

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
        Assert.True((result.Left as GroupNode)!.HasParens);
        Assert.True((result.Left as GroupNode)!.IsNegated);
        Assert.Null((result.Left as GroupNode)!.Prefix);
        Assert.True((result.Left as GroupNode)!.IsExcluded());
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
        Assert.Empty(((TermNode)result.Left).Term!);
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
        Assert.NotNull(result);

        _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
        string generatedQuery = await GenerateQueryVisitor.RunAsync(result);
        Assert.Equal(expected, generatedQuery);

        await new AssignOperationTypeVisitor().AcceptAsync(result, new QueryVisitorContext());
        _logger.LogInformation("{Result}", await DebugQueryVisitor.RunAsync(result));
    }

    [Fact]
    public void Parse_WithFuzzyTermInGroup_SetsProximityOnInnerTermNode()
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("searchKeywords:(\"Wellness\"~)");

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));
        string generatedQuery = GenerateQueryVisitor.Run(result);
        Assert.Equal("searchKeywords:(\"Wellness\"~)", generatedQuery);

        var groupNode = result.Left as GroupNode;
        Assert.NotNull(groupNode);
        Assert.Equal("searchKeywords", groupNode.Field);
        Assert.True(groupNode.HasParens);

        var termNode = groupNode.Left as TermNode;
        Assert.NotNull(termNode);
        Assert.Equal("Wellness", termNode.Term);
        Assert.True(termNode.IsQuotedTerm);
        Assert.Equal(String.Empty, termNode.Proximity);
    }

    [Fact]
    public void Parse_WithNegatedFieldGroup_InnerTermIsNodeOrGroupNegated()
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("-field:(value)");

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var groupNode = result.Left as GroupNode;
        Assert.NotNull(groupNode);
        Assert.True(groupNode.HasParens);
        Assert.Equal("-", groupNode.Prefix);
        Assert.False(groupNode.IsNegated);
        Assert.True(groupNode.IsExcluded());

        var termNode = groupNode.Left as TermNode;
        Assert.NotNull(termNode);
        Assert.False(termNode.IsExcluded());
        Assert.True(termNode.IsNodeOrGroupNegated());
    }

    [Theory]
    [InlineData(null)]
    [InlineData(false)]
    [InlineData(true)]
    public void CleanupQuery_WithNullOrFalseIsNegated_ProducesSameOutput(bool? outerIsNegated)
    {
        // Arrange
        // IsNegated is a bool? but null and false carry no distinct meaning: only true is significant.
        // This pins that equivalence through CleanupQueryVisitor's group-collapse and double-negative
        // logic so a future change can't quietly make the two values behave differently.
        var parser = new LuceneQueryParser();

        string RunWithInner(bool? innerIsNegated)
        {
            var result = parser.Parse("NOT ((value))");
            var outerGroup = (GroupNode)result.Left!;
            outerGroup.IsNegated = outerIsNegated;
            if (outerGroup.Left is GroupNode innerGroup)
                innerGroup.IsNegated = innerIsNegated;

            return CleanupQueryVisitor.Run(result) ?? String.Empty;
        }

        // Act
        string withNull = RunWithInner(null);
        string withFalse = RunWithInner(false);

        // Assert
        Assert.Equal(withNull, withFalse);
    }

    [Fact]
    public void Parse_WithNestedGroupInsideExcludedGroup_IsNodeOrGroupNegatedInspectsParent()
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("-(field:(value))");

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var outerGroup = result.Left as GroupNode;
        Assert.NotNull(outerGroup);
        Assert.Equal("-", outerGroup.Prefix);
        Assert.True(outerGroup.IsExcluded());

        // The inner group already has parens, so the search must start at its parent rather than
        // matching itself, or the excluded outer group would never be inspected.
        var innerGroup = outerGroup.Left as GroupNode;
        Assert.NotNull(innerGroup);
        Assert.True(innerGroup.HasParens);
        Assert.False(innerGroup.IsExcluded());
        Assert.True(innerGroup.IsNodeOrGroupNegated());
    }

    [Theory]
    [InlineData("NOT field:value", true, null, true, false)]
    [InlineData("NOT [1 TO 2]", true, null, true, false)]
    [InlineData("NOT >1", true, null, true, false)]
    [InlineData("NOT field:[1 TO 2]", true, null, true, false)]
    [InlineData("NOT _exists_:field", true, null, true, false)]
    [InlineData("NOT _missing_:field", true, null, true, false)]
    [InlineData("-field:value", null, "-", true, false)]
    [InlineData("!field:value", null, "!", true, false)]
    [InlineData("-[1 TO 2]", false, "-", true, false)]
    [InlineData("!field:[1 TO 2]", false, "!", true, false)]
    [InlineData("+field:value", null, "+", false, true)]
    [InlineData("field:value", null, null, false, false)]
    // Whether a non-negated node reports null or false is inconsistent by rule and is asserted
    // here as-is to lock the existing public AST. Always compare IsNegated against true.
    [InlineData("[1 TO 2]", null, null, false, false)]
    [InlineData("field:[1 TO 2]", false, null, false, false)]
    [InlineData("_exists_:field", false, null, false, false)]
    [InlineData("_missing_:field", false, null, false, false)]
    public void Parse_WithNegation_StoresNotKeywordInIsNegatedAndOperatorsInPrefix(string query, bool? expectedIsNegated, string? expectedPrefix, bool expectedExcluded, bool expectedRequired)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse(query);

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var node = result.Left as IFieldQueryNode;
        Assert.NotNull(node);
        Assert.Equal(expectedIsNegated, node.IsNegated);
        Assert.Equal(expectedPrefix, node.Prefix);
        Assert.Equal(expectedExcluded, node.IsExcluded());
        Assert.Equal(expectedRequired, node.IsRequired());
        Assert.Equal(query, GenerateQueryVisitor.Run(result));
    }

    [Theory]
    [InlineData("NOT [1 TO 2]")]
    [InlineData("NOT >1")]
    [InlineData("NOT field:[1 TO 2]")]
    public void Parse_WithNegatedRange_SetsIsNegatedAndRoundTrips(string query)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse(query);

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var rangeNode = result.Left as TermRangeNode;
        Assert.NotNull(rangeNode);
        Assert.True(rangeNode.IsNegated);
        Assert.Null(rangeNode.Prefix);
        Assert.True(rangeNode.IsExcluded());
        Assert.Equal(query, GenerateQueryVisitor.Run(result));
        Assert.Equal(query, CleanupQueryVisitor.Run(result));
    }

    [Theory]
    [InlineData("field:NOT (value)", "NOT field:(value)")]
    [InlineData("field:NOT (a b)", "NOT field:(a b)")]
    public void Parse_WithNotKeywordBeforeFieldGroup_PreservesNegation(string query, string expectedQuery)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse(query);

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var groupNode = result.Left as GroupNode;
        Assert.NotNull(groupNode);
        Assert.Equal("field", groupNode.Field);
        Assert.True(groupNode.IsNegated);
        Assert.True(groupNode.IsExcluded());
        Assert.Equal(expectedQuery, GenerateQueryVisitor.Run(result));
    }

    [Theory]
    [InlineData("field:-(value)", "-field:(value)", "-", true, false)]
    [InlineData("field:!(value)", "!field:(value)", "!", true, false)]
    [InlineData("field:+(value)", "+field:(value)", "+", false, true)]
    public void Parse_WithPrefixInsideFieldGroup_PreservesPrefix(string query, string expectedQuery, string expectedPrefix, bool expectedExcluded, bool expectedRequired)
    {
        // Arrange
        // A prefix written after the colon is captured by paren_exp, and field_exp used to overwrite it
        // with the field name's null prefix, silently dropping the operator.
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse(query);

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var groupNode = result.Left as GroupNode;
        Assert.NotNull(groupNode);
        Assert.Equal("field", groupNode.Field);
        Assert.Equal(expectedPrefix, groupNode.Prefix);
        Assert.Equal(expectedExcluded, groupNode.IsExcluded());
        Assert.Equal(expectedRequired, groupNode.IsRequired());
        Assert.Equal(expectedQuery, GenerateQueryVisitor.Run(result));
    }

    [Theory]
    [InlineData("field1:value1")]
    [InlineData("NOT field1:value1")]
    [InlineData("field1:NOT value1")]
    [InlineData("-field1:value1")]
    [InlineData("field1:-value1")]
    [InlineData("!field1:value1")]
    [InlineData("field1:!value1")]
    [InlineData("+field1:value1")]
    [InlineData("field1:+value1")]
    [InlineData("field1:(value1)")]
    [InlineData("NOT field1:(value1)")]
    [InlineData("field1:NOT (value1)")]
    [InlineData("-field1:(value1)")]
    [InlineData("field1:-(value1)")]
    [InlineData("!field1:(value1)")]
    [InlineData("field1:!(value1)")]
    [InlineData("+field1:(value1)")]
    [InlineData("field1:+(value1)")]
    [InlineData("field1:(-value1)")]
    [InlineData("field1:(NOT value1)")]
    [InlineData("(field1:value1)")]
    [InlineData("NOT (field1:value1)")]
    [InlineData("-(field1:value1)")]
    [InlineData("!(field1:value1)")]
    [InlineData("NOT (NOT field1:value1)")]
    [InlineData("-(-field1:value1)")]
    [InlineData("field4:[1 TO 2]")]
    [InlineData("NOT field4:[1 TO 2]")]
    [InlineData("-field4:[1 TO 2]")]
    [InlineData("!field4:[1 TO 2]")]
    [InlineData("[1 TO 2]")]
    [InlineData("NOT [1 TO 2]")]
    [InlineData("-[1 TO 2]")]
    [InlineData("field4:<3")]
    [InlineData("NOT field4:<3")]
    [InlineData("-field4:<3")]
    [InlineData("_exists_:field2")]
    [InlineData("NOT _exists_:field2")]
    [InlineData("-_exists_:field2")]
    [InlineData("!_exists_:field2")]
    [InlineData("_missing_:field2")]
    [InlineData("NOT _missing_:field2")]
    [InlineData("-_missing_:field2")]
    [InlineData("field1:value1 AND NOT field2:value2")]
    [InlineData("NOT field1:value1 OR -field2:value2")]
    [InlineData("field1:value1 AND (NOT field2:value2 OR -field3:value3)")]
    public void Parse_WithNegationOrPrefix_GeneratedQueryReparsesToItself(string query)
    {
        // Arrange
        // Negation may be re-emitted in the canonical leading position, so the first generated query is
        // not always the input. It must be a fixed point though: re-parsing it has to produce the same text,
        // otherwise a prefix or NOT is being dropped or duplicated somewhere in the round trip.
        var parser = new LuceneQueryParser();

        // Act
        string generated = GenerateQueryVisitor.Run(parser.Parse(query));
        string regenerated = GenerateQueryVisitor.Run(parser.Parse(generated));

        // Assert
        Assert.Equal(generated, regenerated);
    }

    [Fact]
    public void Parse_WithoutProximityModifier_ProximityIsNull()
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("field:value");

        // Assert
        var termNode = result.Left as TermNode;
        Assert.NotNull(termNode);
        Assert.Null(termNode.Proximity);
    }

    [Theory]
    [InlineData("term~", null, false, "")]
    [InlineData("term~1", null, false, "1")]
    [InlineData("roam~0.8", null, false, "0.8")]
    [InlineData("\"phrase query\"~2", null, true, "2")]
    [InlineData("field:term~", "field", false, "")]
    [InlineData("field:\"phrase\"~3", "field", true, "3")]
    public void Parse_WithProximityModifier_SetsProximityOnTermNode(string query, string? expectedField, bool expectedQuoted, string expectedProximity)
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse(query);

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));
        string generatedQuery = GenerateQueryVisitor.Run(result);
        Assert.Equal(query, generatedQuery);

        var termNode = result.Left as TermNode;
        Assert.NotNull(termNode);
        Assert.Equal(expectedField, termNode.Field);
        Assert.Equal(expectedQuoted, termNode.IsQuotedTerm);
        Assert.Equal(expectedProximity, termNode.Proximity);
    }

    [Fact]
    public void Parse_WithRequiredPrefixAndNotKeyword_IsNodeOrGroupNegatedIgnoresNegation()
    {
        // Arrange
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("NOT +field:value");

        // Assert
        _logger.LogInformation("{Result}", DebugQueryVisitor.Run(result));

        var termNode = result.Left as TermNode;
        Assert.NotNull(termNode);
        Assert.Equal("+", termNode.Prefix);
        Assert.True(termNode.IsNegated);
        Assert.True(termNode.IsRequired());
        Assert.True(termNode.IsExcluded());

        // IsNodeOrGroupNegated returns false when the node is required, even though NOT is present
        Assert.False(termNode.IsNodeOrGroupNegated());
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
