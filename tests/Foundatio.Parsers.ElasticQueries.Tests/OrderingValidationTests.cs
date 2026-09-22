using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

[Trait("TestType", "Unit")]
public class OrderingValidationTests
{
    [Theory]
    [InlineData("terms:(!category)", "!", "category")]
    [InlineData("terms:(NOT category)", "NOT", "category")]
    [InlineData("terms:(NOT +category)", "NOT", "category")]
    [InlineData("terms:(NOT -category)", "NOT", "category")]
    [InlineData("+terms:(!category)", "!", "category")]
    [InlineData("-terms:(NOT +category)", "NOT", "category")]
    [InlineData("terms:(!category -max:price)", "!", "category")]
    [InlineData("terms:(category +max:(NOT price))", "NOT", "price")]
    public async Task BuildAggregationsAsync_WithNegatedPrimaryField_MatchesValidationError(string aggregations, string expectedOperator, string expectedField)
    {
        var parser = new ElasticQueryParser();
        string expectedMessage = $"Boolean operator ({expectedOperator}) is not supported in aggregation expressions for field ({expectedField}): use + for ascending or - for descending order.";

        var result = await parser.ValidateAggregationsAsync(aggregations);
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildAggregationsAsync(aggregations));

        Assert.False(result.IsValid);
        Assert.Equal(expectedMessage, Assert.Single(result.ValidationErrors).Message);
        Assert.NotNull(exception.Result);
        Assert.Equal(expectedMessage, Assert.Single(exception.Result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData("!price", "!")]
    [InlineData("NOT price", "NOT")]
    [InlineData("NOT +price", "NOT")]
    [InlineData("NOT -price", "NOT")]
    [InlineData("!(price name)", "!")]
    [InlineData("NOT (price name)", "NOT")]
    public async Task BuildSortAsync_WithBooleanNegation_MatchesValidationError(string sort, string expectedOperator)
    {
        var parser = new ElasticQueryParser();

        var result = await parser.ValidateSortAsync(sort);
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildSortAsync(sort));

        Assert.False(result.IsValid);
        string expectedMessage = Assert.Single(result.ValidationErrors).Message;
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", expectedMessage);
        Assert.NotNull(exception.Result);
        Assert.Equal(expectedMessage, Assert.Single(exception.Result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData("!@include:ordering", "!")]
    [InlineData("NOT @include:ordering", "NOT")]
    public async Task BuildSortAsync_WithNegatedInclude_ThrowsValidationException(string sort, string expectedOperator)
    {
        var parser = new ElasticQueryParser(c => c.UseIncludes(_ => "price"));
        var context = new ElasticQueryVisitorContext();
        context.SetIncludeResolver(_ => Task.FromResult<string?>("price"));

        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => parser.BuildSortAsync(sort, context));

        Assert.NotNull(exception.Result);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", Assert.Single(exception.Result.ValidationErrors).Message);
        Assert.Empty(exception.Result.UnresolvedIncludes);
    }
}
