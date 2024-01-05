using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class RemoveFieldsQueryVisitorTests : TestWithLoggingBase
{
    public RemoveFieldsQueryVisitorTests(ITestOutputHelper output) : base(output)
    {
        Log.MinimumLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    [Fact]
    public async Task CanRemoveField()
    {
        var parser = new LuceneQueryParser();
        var result = await parser.ParseAsync("field1:value field2:value (field3:value OR field4:value (field5:value)) field6:value");
        var queryResult = await RemoveFieldsQueryVisitor.RunAsync(result, new[] { "field1" });

        Assert.Equal("field2:value (field3:value OR field4:value (field5:value)) field6:value", queryResult);
    }

    [Fact]
    public async Task CanRemoveFieldWithFunc()
    {
        var parser = new LuceneQueryParser();
        var result = await parser.ParseAsync("field1:value field2:value (field3:value OR field4:value (field5:value)) field6:value");
        var queryResult = await RemoveFieldsQueryVisitor.RunAsync(result, f => f == "field3");

        Assert.Equal("field1:value field2:value (field4:value (field5:value)) field6:value", queryResult);
    }
}
