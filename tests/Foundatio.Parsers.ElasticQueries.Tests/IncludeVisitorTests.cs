using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Xunit;

namespace Foundatio.Parsers.ElasticQueries.Tests;

public class IncludeVisitorTests
{
    [Theory]
    [InlineData("terms:(field1 @include:/one.*/)")]
    [InlineData("terms:(field1 @include:one)")]
    [InlineData("terms:(field1 @exclude:/two.*/ @include:/one.*/)")]
    public async Task BuildAggregationsAsync_WithTermsIncludeAndContextIncludeResolver_AppliesTermsInclude(string aggregations)
    {
        // Arrange
        int resolverCalls = 0;
        var parser = new ElasticQueryParser(c => c.UseIncludes((string _) => "field2:value"));
        var context = new ElasticQueryVisitorContext().SetIncludeResolver(_ =>
        {
            resolverCalls++;
            return Task.FromResult<string?>(null);
        });

        // Act
        var result = await parser.BuildAggregationsAsync(aggregations, context);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.ToDictionary()["terms_field1"].Terms!.Include);
        Assert.Equal(0, resolverCalls);
    }

    [Fact]
    public async Task BuildQueryAsync_WithContextIncludeResolver_ResolvesInclude()
    {
        // Arrange
        int resolverCalls = 0;
        var parser = new ElasticQueryParser(c => c.UseIncludes((string _) => "unused"));
        var context = new ElasticQueryVisitorContext().SetIncludeResolver(_ =>
        {
            resolverCalls++;
            return Task.FromResult<string?>("field2:value");
        });

        // Act
        var result = await parser.BuildQueryAsync("field1:value @include:stuff", context);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(1, resolverCalls);
    }

    [Fact]
    public async Task BuildQueryAsync_WithConfiguredIncludeResolver_ResolvesInclude()
    {
        // Arrange
        int resolverCalls = 0;
        var parser = new ElasticQueryParser(c => c.UseIncludes((string _) =>
        {
            resolverCalls++;
            return "field2:value";
        }));

        // Act
        var result = await parser.BuildQueryAsync("field1:value @include:stuff");

        // Assert
        Assert.NotNull(result);
        Assert.Equal(1, resolverCalls);
    }
}

