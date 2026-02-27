using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class RemoveFieldsQueryVisitorTests : TestWithLoggingBase
{
    public RemoveFieldsQueryVisitorTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    [Fact]
    public async Task CanRemoveField()
    {
        var parser = new LuceneQueryParser();
        var result = await parser.ParseAsync("field1:value field2:value (field3:value OR field4:value (field5:value)) field6:value");
        string queryResult = await RemoveFieldsQueryVisitor.RunAsync(result, ["field1"]);

        Assert.Equal("field2:value (field3:value OR field4:value (field5:value)) field6:value", queryResult);
    }

    [Fact]
    public async Task CanRemoveFieldWithFunc()
    {
        var parser = new LuceneQueryParser();
        var result = await parser.ParseAsync("field1:value field2:value (field3:value OR field4:value (field5:value)) field6:value");
        string queryResult = await RemoveFieldsQueryVisitor.RunAsync(result, f => f == "field3");

        Assert.Equal("field1:value field2:value (field4:value (field5:value)) field6:value", queryResult);
    }
}
