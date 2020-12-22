using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.Tests {
    public class DebugQueryVisitorTest {
        /// <summary>
        /// This test ensures that the DebugQueryVisitor shows changes applied by a FieldResolverQueryVisitor
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task DebugQueryVisitorReflectsResolvedFields() {
            var parser = new LuceneQueryParser();
            var result1 = await parser.ParseAsync("field1:value");
            var result2 = await parser.ParseAsync("field1:value");

            // We assert that the two queries are identical
            Assert.Equal("field1:value", result1.ToString());
            Assert.Equal("field1:value", result2.ToString());

            // We swap field1 with field2 on result1
            await FieldResolverQueryVisitor.RunAsync(result1, new FieldMap { { "field1", "field2" } });
            // We swap field1 with field3 on result1
            await FieldResolverQueryVisitor.RunAsync(result2, new FieldMap { { "field1", "field3" } });

            // We check that the queries are indeed updated
            Assert.Equal("field2:value", result1.ToString());
            Assert.Equal("field3:value", result2.ToString());

            // We assert that the DebugQueryVisitor outputs should be different for these two queries now
            string debugQuery1 = await DebugQueryVisitor.RunAsync(result1);
            string debugQuery2 = await DebugQueryVisitor.RunAsync(result2);

            Assert.NotEqual(debugQuery1, debugQuery2);
        }
    }
}
