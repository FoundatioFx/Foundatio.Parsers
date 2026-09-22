using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

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
    [InlineData("terms:(NOT category +max:price)", "NOT", "category")]
    [InlineData("terms:(category -max:(!price))", "!", "price")]
    [InlineData("terms:(category +max:(NOT price))", "NOT", "price")]
    public async Task ValidateAggregationsAsync_WithNegatedPrimaryField_ReportsSingleError(string aggregations, string expectedOperator, string expectedField)
    {
        string expectedMessage = $"Boolean operator ({expectedOperator}) is not supported in aggregation expressions for field ({expectedField}): use + for ascending or - for descending order.";

        var result = await QueryValidator.ValidateAggregationsAsync(aggregations);

        Assert.False(result.IsValid);
        Assert.Equal(QueryTypes.Aggregation, result.QueryType);
        Assert.Equal(expectedMessage, Assert.Single(result.ValidationErrors).Message);

        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => QueryValidator.ValidateAggregationsAndThrowAsync(aggregations));
        Assert.NotNull(exception.Result);
        Assert.Equal(expectedMessage, Assert.Single(exception.Result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData("terms:(category)")]
    [InlineData("terms:(category -max:price)")]
    [InlineData("terms:(category +max:price)")]
    [InlineData("terms:(category -max:(price))")]
    [InlineData("terms:(category +max:(price))")]
    public async Task ValidateAggregationsAsync_WithExplicitOrdering_PreservesFieldAndOperationValidation(string aggregations)
    {
        var options = new QueryValidationOptions();
        options.AllowedFields.Add("category");
        options.AllowedFields.Add("price");
        options.AllowedOperations.Add("terms");
        options.AllowedOperations.Add("max");

        var result = await QueryValidator.ValidateAggregationsAsync(aggregations, options);

        Assert.True(result.IsValid, result.Message);
        Assert.Empty(result.ValidationErrors);
        Assert.Contains("category", result.ReferencedFields);
        Assert.Contains("terms", result.Operations.Keys);
    }

    [Theory]
    [InlineData(QueryTypes.Sort, "!@include:ordering", "price", "!")]
    [InlineData(QueryTypes.Sort, "NOT @include:ordering", "price", "NOT")]
    [InlineData(QueryTypes.Sort, "NOT +@include:ordering", "price", "NOT")]
    [InlineData(QueryTypes.Aggregation, "!@include:ordering", "max:price", "!")]
    [InlineData(QueryTypes.Aggregation, "NOT @include:ordering", "max:price", "NOT")]
    [InlineData(QueryTypes.Aggregation, "NOT +@include:ordering", "max:price", "NOT")]
    public async Task ValidateAsync_WithNegatedOrderingInclude_ReportsSingleError(string queryType, string expression, string includedExpression, string expectedOperator)
    {
        var context = new QueryVisitorContext();
        context.SetIncludeResolver(_ => includedExpression);

        var result = queryType == QueryTypes.Sort
            ? await QueryValidator.ValidateSortAsync(expression, context: context)
            : await QueryValidator.ValidateAggregationsAsync(expression, context: context);

        Assert.False(result.IsValid);
        Assert.Equal(queryType, result.QueryType);
        string message = Assert.Single(result.ValidationErrors).Message;
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported", message);
        Assert.Contains("use + for ascending or - for descending order", message);
        Assert.Empty(result.UnresolvedIncludes);
    }

    [Theory]
    [InlineData("!field:value")]
    [InlineData("NOT field:value")]
    [InlineData("field:(!value)")]
    [InlineData("field:(NOT value)")]
    [InlineData("field:value OR +required:value")]
    [InlineData("field:value OR -excluded:value")]
    public async Task ValidateQueryAsync_WithQueryOperators_RemainsValid(string query)
    {
        var result = await QueryValidator.ValidateQueryAsync(query);

        Assert.True(result.IsValid, result.Message);
        Assert.Equal(QueryTypes.Query, result.QueryType);
    }

    [Theory]
    [InlineData("!field:[1 TO 2]", "!")]
    [InlineData("NOT field:[1 TO 2]", "NOT")]
    [InlineData("!_exists_:field", "!")]
    [InlineData("NOT _exists_:field", "NOT")]
    [InlineData("!_missing_:field", "!")]
    [InlineData("NOT _missing_:field", "NOT")]
    public async Task ValidateSortAsync_WithNegatedNonTermNode_ReportsSingleError(string sort, string expectedOperator)
    {
        var result = await QueryValidator.ValidateSortAsync(sort);

        Assert.False(result.IsValid);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", Assert.Single(result.ValidationErrors).Message);
    }
}
