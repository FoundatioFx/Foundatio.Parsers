using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Parsers.LuceneQueries.Tests {
    [Trait("TestType", "Unit")]
    public class QueryValidatorTests : TestWithLoggingBase {
        public QueryValidatorTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
        }

        [Fact]
        public async Task InvalidSyntax() {
            var info = await QueryValidator.ValidateQueryAsync(@":");
            Assert.False(info.IsValid);
            Assert.NotNull(info.Message);
            Assert.Contains("Unexpected", info.Message);
        }

        [Fact]
        public async Task ThrowInvalidSyntax() {
            var ex = await Assert.ThrowsAsync<QueryValidationException>(() => QueryValidator.ValidateQueryAndThrowAsync(@":"));
            Assert.Contains("Unexpected", ex.Message);
            Assert.False(ex.ValidationInfo.IsValid);
            Assert.NotNull(ex.ValidationInfo.Message);
            Assert.Contains("Unexpected", ex.ValidationInfo.Message);
        }

        [Fact]
        public async Task AllowedFields() {
            var options = new QueryValidationOptions();
            options.AllowedFields.Add("allowedfield");
            var info = await QueryValidator.ValidateQueryAsync(@"blah allowedfield:value", options);
            Assert.True(info.IsValid);
        }

        [Fact]
        public async Task AllowedOperations() {
            var options = new QueryValidationOptions();
            options.AllowedOperations.Add("terms");
            var info = await QueryValidator.ValidateAggregationsAsync(@"terms:blah", options);
            Assert.True(info.IsValid);
        }

        [Fact]
        public async Task NonAllowedOperations() {
            var options = new QueryValidationOptions();
            options.AllowedOperations.Add("terms");
            var info = await QueryValidator.ValidateAggregationsAsync(@"terms:blah notallowed:blah", options);
            Assert.False(info.IsValid);
        }

        [Fact]
        public async Task ResolvedFields() {
            var options = new QueryValidationOptions {
                AllowUnresolvedFields = false
            };
            var context = new QueryVisitorContext();
            context.SetFieldResolver(f => f == "field1" ? f : null);
            var info = await QueryValidator.ValidateQueryAsync(@"field1:blah", options, context);
            Assert.True(info.IsValid);
        }

        [Fact]
        public async Task NonResolvedFields() {
            var options = new QueryValidationOptions {
                AllowUnresolvedFields = false
            };
            var context = new QueryVisitorContext();
            context.SetFieldResolver(f => f == "field1" ? f : null);
            var info = await QueryValidator.ValidateQueryAsync(@"field1:blah field2:blah", options, context);
            Assert.False(info.IsValid);
            Assert.Contains("field2", info.UnresolvedFields);
        }

        [Fact]
        public async Task NonResolvedThrowsFields() {
            var options = new QueryValidationOptions {
                AllowUnresolvedFields = false
            };
            var context = new QueryVisitorContext();
            context.SetFieldResolver(f => f == "field1" ? f : null);
            var ex = await Assert.ThrowsAsync<QueryValidationException>(() => QueryValidator.ValidateQueryAndThrowAsync(@"field1:blah field2:blah", options, context));
            Assert.Contains("resolved", ex.Message);
            Assert.Contains("field2", ex.ValidationInfo.UnresolvedFields);
            Assert.False(ex.ValidationInfo.IsValid);
            Assert.NotNull(ex.ValidationInfo.Message);
            Assert.Contains("resolved", ex.ValidationInfo.Message);
        }

        // allowed fields
        // allowed operations
        // 
        // as part of a chain of other visitors
        // add tests to elastic that make use of a resolver
    }
}
