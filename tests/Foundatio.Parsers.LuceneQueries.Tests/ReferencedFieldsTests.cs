using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Xunit;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class ReferencedFieldsTests : TestWithLoggingBase
{
    public ReferencedFieldsTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace;
    }

    [Fact]
    public async Task CanGetReferencedFields()
    {
        var parser = new LuceneQueryParser();
        var result = await parser.ParseAsync("field1:value field2:value (field3:value OR field4:value (field5:value)) field6:value");
        var fields = result.GetReferencedFields();

        Assert.Equal(6, fields.Count);
        Assert.Contains("field1", fields);
        Assert.Contains("field2", fields);
        Assert.Contains("field3", fields);
        Assert.Contains("field4", fields);
        Assert.Contains("field5", fields);
        Assert.Contains("field6", fields);

        // make sure caching works
        fields = result.GetReferencedFields();

        Assert.Equal(6, fields.Count);
        Assert.Contains("field1", fields);
        Assert.Contains("field2", fields);
        Assert.Contains("field3", fields);
        Assert.Contains("field4", fields);
        Assert.Contains("field5", fields);
        Assert.Contains("field6", fields);
    }

    [Fact]
    public async Task CanGetTopLevelReferencedFields()
    {
        var parser = new LuceneQueryParser();
        var result = await parser.ParseAsync("field1:value field2:value (field3:value OR field4:value (field5:value)) field6:value");
        var fields = result.GetReferencedFields(currentGroupOnly: true);

        Assert.Equal(3, fields.Count);
        Assert.Contains("field1", fields);
        Assert.Contains("field2", fields);
        Assert.Contains("field6", fields);

        // make sure caching works
        fields = result.GetReferencedFields(currentGroupOnly: true);

        Assert.Equal(3, fields.Count);
        Assert.Contains("field1", fields);
        Assert.Contains("field2", fields);
        Assert.Contains("field6", fields);
    }
}
