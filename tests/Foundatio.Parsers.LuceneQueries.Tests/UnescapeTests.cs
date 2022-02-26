using Foundatio.Xunit;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public sealed class UnescapeTests : TestWithLoggingBase {
    public UnescapeTests(ITestOutputHelper output) : base(output) {
        Log.MinimumLevel = LogLevel.Trace;
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("none", "none")]
    [InlineData(@"Escaped \. in the code", "Escaped . in the code")]
    [InlineData(@"Escap\e", "Escape")]
    [InlineData(@"Double \\ backslash", @"Double \ backslash")]
    [InlineData(@"At end \", @"At end \")]
    public void UnescapingWorks(string test, string expected) {
        var result = test.Unescape();
        Assert.Equal(expected, result);
    }
}
