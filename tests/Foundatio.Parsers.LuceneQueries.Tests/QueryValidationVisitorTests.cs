using Foundatio.Logging.Xunit;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.Tests {
    public class QueryValidationVisitorTests : TestWithLoggingBase {
        public QueryValidationVisitorTests(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void GetGatherQueryInfo() {
            var info = ValidationVisitor.GetInfo("stuff hey:now nested:(stuff:33)");
            Assert.Equal(2, info.MaxNodeDepth);
            Assert.Equal(4, info.ReferencedFields.Count);
            Assert.True(info.ReferencedFields.Contains("_all"));
        }
    }
}
