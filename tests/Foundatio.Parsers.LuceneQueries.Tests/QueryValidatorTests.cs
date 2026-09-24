using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Xunit;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

[Trait("TestType", "Unit")]
public class QueryValidatorTests : TestWithLoggingBase
{
    public QueryValidatorTests(ITestOutputHelper output) : base(output)
    {
        Log.DefaultLogLevel = LogLevel.Trace;
    }

    [Fact]
    public async Task InvalidSyntax()
    {
        var info = await QueryValidator.ValidateQueryAsync(@":");
        Assert.False(info.IsValid);
        Assert.NotNull(info.Message);
        Assert.Contains("Unexpected", info.Message);
    }

    [Fact]
    public async Task ThrowInvalidSyntax()
    {
        var ex = await Assert.ThrowsAsync<QueryValidationException>(() => QueryValidator.ValidateQueryAndThrowAsync(@":"));
        Assert.Contains("Unexpected", ex.Message);
        Assert.NotNull(ex.Result);
        Assert.False(ex.Result.IsValid);
        Assert.NotNull(ex.Result.Message);
        Assert.Contains("Unexpected", ex.Result.Message);
    }

    [Fact]
    public async Task AllowedFields()
    {
        var options = new QueryValidationOptions();
        options.AllowedFields.Add("allowedfield");
        var info = await QueryValidator.ValidateQueryAsync(@"blah allowedfield:value", options);
        Assert.True(info.IsValid);
    }

    [Fact]
    public async Task AllowedFieldsWithAliases()
    {
        var options = new QueryValidationOptions();
        options.AllowedFields.Add("allowedfield");
        var context = new QueryVisitorContext();
        var aliasMap = new FieldMap { { "allowedfield", "idx1" } };
        context.SetFieldResolver(aliasMap.ToHierarchicalFieldResolver());

        var info = await QueryValidator.ValidateQueryAsync(@"allowedfield:test", options, context);
        Assert.True(info.IsValid);

        // do not allow the resolved version in the query; allowed is an explicit list of fields
        info = await QueryValidator.ValidateQueryAsync(@"idx1:test", options, context);
        Assert.False(info.IsValid);
    }

    [Fact]
    public async Task RestrictedFields()
    {
        var options = new QueryValidationOptions();
        options.RestrictedFields.Add("restrictedfield");
        var info = await QueryValidator.ValidateQueryAsync(@"blah restrictedfield:value", options);
        Assert.False(info.IsValid);
    }

    [Fact]
    public async Task RestrictedFieldsWithAliases()
    {
        var options = new QueryValidationOptions();
        options.RestrictedFields.Add("restrictedField");
        var context = new QueryVisitorContext();
        var aliasMap = new FieldMap { { "restrictedField", "idx1" } };
        context.SetFieldResolver(aliasMap.ToHierarchicalFieldResolver());
        var info = await QueryValidator.ValidateQueryAsync(@"restrictedField:test", options, context);
        Assert.False(info.IsValid);

        info = await QueryValidator.ValidateQueryAsync(@"idx1:test", options, context);
        Assert.False(info.IsValid);
    }

    [Fact]
    public async Task AllowLeadingWildcards()
    {
        var options = new QueryValidationOptions();
        options.AllowLeadingWildcards = false;
        var info = await QueryValidator.ValidateQueryAsync(@"blah allowedfield:*alue", options);
        Assert.False(info.IsValid);
        Assert.Contains("wildcard", info.Message);

        options.AllowLeadingWildcards = true;
        info = await QueryValidator.ValidateQueryAsync(@"blah allowedfield:*alue", options);
        Assert.True(info.IsValid);
    }

    [Fact]
    public async Task AllowedOperations()
    {
        var options = new QueryValidationOptions();
        options.AllowedOperations.Add("terms");
        var info = await QueryValidator.ValidateAggregationsAsync(@"terms:blah", options);
        Assert.True(info.IsValid);
    }

    [Fact]
    public async Task NonAllowedOperations()
    {
        var options = new QueryValidationOptions();
        options.AllowedOperations.Add("terms");
        var info = await QueryValidator.ValidateAggregationsAsync(@"terms:blah notallowed:blah", options);
        Assert.False(info.IsValid);
    }

    [Fact]
    public async Task RestrictedOperations()
    {
        var options = new QueryValidationOptions();
        options.RestrictedOperations.Add("terms");
        var info = await QueryValidator.ValidateAggregationsAsync(@"terms:blah", options);
        Assert.False(info.IsValid);
    }

    [Fact]
    public async Task NonRestrictedOperations()
    {
        var options = new QueryValidationOptions();
        options.RestrictedOperations.Add("terms");
        var info = await QueryValidator.ValidateAggregationsAsync(@"sum:blah", options);
        Assert.True(info.IsValid);
    }

    [Fact]
    public async Task ResolvedFields()
    {
        var options = new QueryValidationOptions
        {
            AllowUnresolvedFields = false
        };
        var context = new QueryVisitorContext();
        context.SetFieldResolver(f => f == "field1" ? f : null);
        var info = await QueryValidator.ValidateQueryAsync(@"field1:blah", options, context);
        Assert.True(info.IsValid);
    }

    [Fact]
    public async Task NonResolvedFields()
    {
        var options = new QueryValidationOptions
        {
            AllowUnresolvedFields = false
        };
        var context = new QueryVisitorContext();
        context.SetFieldResolver(f => f == "field1" ? f : null);
        var info = await QueryValidator.ValidateQueryAsync(@"field1:blah field2:blah", options, context);
        Assert.False(info.IsValid);
        Assert.Contains("field2", info.UnresolvedFields!);
    }

    [Fact]
    public async Task NonResolvedThrowsFields()
    {
        var options = new QueryValidationOptions
        {
            AllowUnresolvedFields = false
        };
        var context = new QueryVisitorContext();
        context.SetFieldResolver(f => f == "field1" ? f : null);
        var ex = await Assert.ThrowsAsync<QueryValidationException>(() => QueryValidator.ValidateQueryAndThrowAsync(@"field1:blah field2:blah", options, context));
        Assert.Contains("resolved", ex.Message);
        Assert.NotNull(ex.Result);
        Assert.NotNull(ex.Result.UnresolvedFields);
        Assert.Contains("field2", ex.Result.UnresolvedFields);
        Assert.False(ex.Result.IsValid);
        Assert.NotNull(ex.Result.Message);
        Assert.Contains("resolved", ex.Result.Message!);
    }

    [Fact]
    public void CanParseWildcardQuery()
    {
        var sut = new LuceneQueryParser();
        var node = sut.Parse("*");
        var result = ValidationVisitor.Run(node);
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("!price", "!")]
    [InlineData("NOT price", "NOT")]
    [InlineData("NOT +price", "NOT")]
    [InlineData("price !name", "!")]
    [InlineData("!(price name)", "!")]
    [InlineData("NOT (price)", "NOT")]
    public async Task ValidateSortAsync_WithBooleanNegationOperator_ReturnsValidationError(string sort, string expectedOperator)
    {
        // Arrange: no parser configuration required.

        // Act
        var info = await QueryValidator.ValidateSortAsync(sort);

        // Assert
        Assert.False(info.IsValid);
        Assert.NotNull(info.Message);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", info.Message);
        Assert.Contains("use + for ascending or - for descending order", info.Message);
    }

    [Theory]
    [InlineData("price")]
    [InlineData("+price")]
    [InlineData("-price")]
    [InlineData("-price +name")]
    [InlineData("-(a b +c)")]
    public async Task ValidateSortAsync_WithExplicitOrderingOperators_IsValid(string sort)
    {
        // Arrange: no parser configuration required.

        // Act
        var info = await QueryValidator.ValidateSortAsync(sort);

        // Assert
        Assert.True(info.IsValid, info.Message);
    }

    [Theory]
    [InlineData("terms:(field1 !max:field4)", "!")]
    [InlineData("terms:(field1 NOT max:field4)", "NOT")]
    [InlineData("!max:field4", "!")]
    [InlineData("NOT max:field4", "NOT")]
    public async Task ValidateAggregationsAsync_WithBooleanNegationOperator_ReturnsValidationError(string aggregations, string expectedOperator)
    {
        // Arrange: no parser configuration required.

        // Act
        var info = await QueryValidator.ValidateAggregationsAsync(aggregations);

        // Assert
        Assert.False(info.IsValid);
        Assert.NotNull(info.Message);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in aggregation expressions", info.Message);
        Assert.Contains("use + for ascending or - for descending order", info.Message);
    }

    [Theory]
    [InlineData("terms:(field1 -max:field4)")]
    [InlineData("terms:(field1 +max:field4)")]
    [InlineData("max:field4")]
    public async Task ValidateAggregationsAsync_WithExplicitOrderingOperators_IsValid(string aggregations)
    {
        // Arrange: no parser configuration required.

        // Act
        var info = await QueryValidator.ValidateAggregationsAsync(aggregations);

        // Assert
        Assert.True(info.IsValid, info.Message);
    }

    [Theory]
    [InlineData("!field1:value1")]
    [InlineData("NOT field1:value1")]
    [InlineData("field1:value1 NOT field2:value2")]
    [InlineData("-field1:value1")]
    public async Task ValidateQueryAsync_WithBooleanNegationOperator_IsValid(string query)
    {
        // Arrange: no parser configuration required.

        // Act
        var info = await QueryValidator.ValidateQueryAsync(query);

        // Assert
        Assert.True(info.IsValid, info.Message);
    }

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
        // Arrange
        string expectedMessage = $"Boolean operator ({expectedOperator}) is not supported in aggregation expressions for field ({expectedField}): use + for ascending or - for descending order.";

        // Act
        var result = await QueryValidator.ValidateAggregationsAsync(aggregations);
        var exception = await Assert.ThrowsAsync<QueryValidationException>(() => QueryValidator.ValidateAggregationsAndThrowAsync(aggregations));

        // Assert
        Assert.False(result.IsValid);
        Assert.Equal(QueryTypes.Aggregation, result.QueryType);
        Assert.Equal(expectedMessage, Assert.Single(result.ValidationErrors).Message);

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
        // Arrange
        var options = new QueryValidationOptions();
        options.AllowedFields.Add("category");
        options.AllowedFields.Add("price");
        options.AllowedOperations.Add("terms");
        options.AllowedOperations.Add("max");

        // Act
        var result = await QueryValidator.ValidateAggregationsAsync(aggregations, options);

        // Assert
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
        // Arrange
        var context = new QueryVisitorContext();
        context.SetIncludeResolver(_ => Task.FromResult<string?>(includedExpression));

        // Act
        var result = queryType is QueryTypes.Sort
            ? await QueryValidator.ValidateSortAsync(expression, context: context)
            : await QueryValidator.ValidateAggregationsAsync(expression, context: context);

        // Assert
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
        // Arrange: no parser configuration required.

        // Act
        var result = await QueryValidator.ValidateQueryAsync(query);

        // Assert
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
        // Arrange: no parser configuration required.

        // Act
        var result = await QueryValidator.ValidateSortAsync(sort);

        // Assert
        Assert.False(result.IsValid);
        Assert.Contains($"Boolean operator ({expectedOperator}) is not supported in sort expressions", Assert.Single(result.ValidationErrors).Message);
    }

    [Theory]
    [InlineData(QueryTypes.Sort, @"\!price")]
    [InlineData(QueryTypes.Sort, "\"!price\"")]
    [InlineData(QueryTypes.Aggregation, @"terms:(\!category)")]
    [InlineData(QueryTypes.Aggregation, "terms:(\"!category\")")]
    public async Task ValidateAsync_WithLiteralExclamationMark_RemainsValid(string queryType, string expression)
    {
        // Arrange: no parser configuration required.

        // Act
        var result = queryType is QueryTypes.Sort
            ? await QueryValidator.ValidateSortAsync(expression)
            : await QueryValidator.ValidateAggregationsAsync(expression);

        // Assert
        Assert.True(result.IsValid, result.Message);
    }

    [Theory]
    [InlineData(QueryTypes.Sort, "!price NOT name")]
    [InlineData(QueryTypes.Sort, "NOT (!price)")]
    [InlineData(QueryTypes.Aggregation, "terms:(!category NOT max:price)")]
    [InlineData(QueryTypes.Aggregation, "!terms:(!category)")]
    public async Task ValidateAsync_WithTwoNegatedNodes_ReportsEachOnce(string queryType, string expression)
    {
        // Arrange: no parser configuration required.

        // Act
        var result = queryType is QueryTypes.Sort
            ? await QueryValidator.ValidateSortAsync(expression)
            : await QueryValidator.ValidateAggregationsAsync(expression);

        // Assert
        Assert.False(result.IsValid);
        Assert.Equal(2, result.ValidationErrors.Count);
        Assert.All(result.ValidationErrors, error => Assert.StartsWith("Boolean operator (", error.Message));
    }
}
