using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class IncludeScopeTests
{
    [Theory]
    [InlineData("(")]
    [InlineData("@include:broken")]
    [InlineData("@include:outer")]
    public async Task IncludeVisitor_WithFailedExpansion_RestoresRecursionStack(string expression)
    {
        var parser = new LuceneQueryParser();
        var context = new QueryVisitorContext();
        var includes = new Dictionary<string, string> { { "outer", expression }, { "broken", "(" } };
        await IncludeVisitor.RunAsync(parser.Parse("@include:outer"), includes, context);

        Assert.False(context.GetValidationResult().IsValid);
        Assert.Empty(Assert.IsType<Stack<string>>(context.Data["@IncludeStackKey"]));
    }
}
