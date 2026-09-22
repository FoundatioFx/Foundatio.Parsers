using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class QueryNodeExtensionsTests
{
    [Theory]
    [InlineData(null, null, null, null, false)]
    [InlineData(false, null, false, null, false)]
    [InlineData(true, null, null, null, true)]
    [InlineData(null, "-", null, null, true)]
    [InlineData(null, "!", null, null, true)]
    [InlineData(null, null, true, null, true)]
    [InlineData(null, null, null, "-", true)]
    [InlineData(null, null, null, "!", true)]
    [InlineData(null, null, null, "+", false)]
    [InlineData(null, null, true, "+", true)]
    [InlineData(null, "+", true, null, false)]
    [InlineData(null, "+", null, "-", false)]
    [InlineData(null, "+", null, "!", false)]
    [InlineData(true, "+", true, "-", false)]
    [InlineData(true, null, true, null, true)]
    [InlineData(null, "-", null, "-", true)]
    [InlineData(null, "!", null, "!", true)]
    public void IsNodeOrGroupNegated_WithParenthesizedGroupOperators_PreservesRequiredPrecedence(
        bool? isNegated, string? prefix, bool? parentIsNegated, string? parentPrefix, bool expected)
    {
        // Arrange
        var node = new GroupNode { HasParens = true, IsNegated = isNegated, Prefix = prefix };
        var parent = new GroupNode
        {
            HasParens = true,
            IsNegated = parentIsNegated,
            Prefix = parentPrefix,
            Left = node
        };

        // Act
        bool result = node.IsNodeOrGroupNegated();

        // Assert
        Assert.Same(parent, node.Parent);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(null, null, false)]
    [InlineData(false, null, false)]
    [InlineData(true, null, true)]
    [InlineData(null, "-", true)]
    [InlineData(null, "!", true)]
    [InlineData(null, "+", false)]
    [InlineData(true, "+", false)]
    public void IsNodeOrGroupNegated_WithDetachedGroup_UsesOnlyLocalOperators(
        bool? isNegated, string? prefix, bool expected)
    {
        // Arrange
        var node = new GroupNode { HasParens = true, IsNegated = isNegated, Prefix = prefix };

        // Act
        bool result = node.IsNodeOrGroupNegated();

        // Assert
        Assert.Null(node.Parent);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("field:value")]
    [InlineData("field:[1 TO 2]")]
    [InlineData("_exists_:field")]
    [InlineData("_missing_:field")]
    [InlineData("field:(value)")]
    public void IsNodeOrGroupNegated_WithNonParenthesizedRoot_UsesRootFallback(string query)
    {
        // Arrange
        var parser = new LuceneQueryParser();
        var root = parser.Parse(query);
        var node = Assert.IsAssignableFrom<IFieldQueryNode>(root.Left);
        root.Left = new GroupNode { Left = node };
        root.IsNegated = true;

        // Act
        bool result = node.IsNodeOrGroupNegated();

        // Assert
        Assert.Null(root.Parent);
        Assert.False(root.HasParens);
        Assert.False(node.IsExcluded());
        Assert.True(result);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IsNodeOrGroupNegated_WithNonParenthesizedAncestor_InspectsOnlyNearestBoundary(bool boundaryIsNegated)
    {
        // Arrange
        var node = new GroupNode { HasParens = true };
        var boundary = new GroupNode
        {
            HasParens = true,
            IsNegated = boundaryIsNegated,
            Left = new GroupNode { IsNegated = true, Left = node }
        };
        var root = new GroupNode { IsNegated = true, Left = boundary };

        // Act
        bool result = node.IsNodeOrGroupNegated();

        // Assert
        Assert.Same(root, boundary.Parent);
        Assert.Equal(boundaryIsNegated, result);
    }
}
