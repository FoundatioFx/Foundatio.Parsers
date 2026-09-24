using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Xunit;

namespace Foundatio.Parsers.LuceneQueries.Tests;

[Trait("TestType", "Unit")]
public class QueryNodeExtensionsTests
{
    [Fact]
    public void IsNodeOrGroupNegated_WithNullNode_ReturnsFalse()
    {
        // Arrange
        // IsExcluded() and IsRequired() both tolerate a null receiver, so this helper must too.
        // Suppressed because the scenario under test is a caller without nullable reference types enabled.
        IFieldQueryNode node = null!;

        // Act
        bool result = node.IsNodeOrGroupNegated();

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IsNodeOrGroupNegated_WithRootNode_ReturnsFalse()
    {
        // Arrange
        // The root group has no parent, so the parent walk must not throw.
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("field1:value1");

        // Assert
        Assert.False(result.IsNodeOrGroupNegated());
    }

    [Fact]
    public void IsNodeOrGroupNegated_WithTermInsideNestedFieldGroup_StopsAtNearestParenthesizedGroup()
    {
        // Arrange
        // Pins the documented depth limit: the walk stops at the nearest parenthesized group, so a term
        // can disagree with the groups enclosing it. See https://github.com/FoundatioFx/Foundatio.Parsers/issues/279.
        var parser = new LuceneQueryParser();

        // Act
        var nested = parser.Parse("-(field:(value))");
        var flat = parser.Parse("-(field:value)");

        // Assert
        var nestedOuter = Assert.IsType<GroupNode>(nested.Left);
        var nestedInner = Assert.IsType<GroupNode>(nestedOuter.Left);
        var nestedTerm = Assert.IsType<TermNode>(nestedInner.Left);

        Assert.True(nestedOuter.IsNodeOrGroupNegated());
        Assert.True(nestedInner.IsNodeOrGroupNegated());

        // The term's nearest group is the parenthesized "field:(...)" group, which is not itself
        // excluded, so the excluded outer group is never reached.
        Assert.False(nestedTerm.IsNodeOrGroupNegated());

        // Without the intermediate parenthesized field group, the same term does see the negation.
        var flatOuter = Assert.IsType<GroupNode>(flat.Left);
        var flatTerm = Assert.IsType<TermNode>(flatOuter.Left);
        Assert.True(flatTerm.IsNodeOrGroupNegated());
    }

    [Fact]
    public void IsNodeOrGroupNegated_WithDoublyNestedGroups_MatchesDocumentedExample()
    {
        // Arrange
        // Pins the exact example in the IsNodeOrGroupNegated XML docs, so the documented result cannot
        // drift from the implementation. See https://github.com/FoundatioFx/Foundatio.Parsers/issues/279.
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("NOT (a:(b:(c)))");

        // Assert
        var notGroup = Assert.IsType<GroupNode>(result.Left);
        var groupA = Assert.IsType<GroupNode>(notGroup.Left);
        var groupB = Assert.IsType<GroupNode>(groupA.Left);
        var termC = Assert.IsType<TermNode>(groupB.Left);

        Assert.True(notGroup.IsExcluded());

        // "a" sees the excluded NOT group, because that group is its nearest enclosing parenthesized group.
        Assert.True(groupA.IsNodeOrGroupNegated());

        // "b" does not: its nearest enclosing group is "a:(...)", which is parenthesized but not excluded,
        // so the walk stops there and never reaches the NOT group.
        Assert.False(groupB.IsNodeOrGroupNegated());
        Assert.False(termC.IsNodeOrGroupNegated());
    }

    [Fact]
    public void IsNodeOrGroupNegated_WithMultipleTermsInExcludedGroup_ReportsSyntacticContextOnly()
    {
        // Arrange
        // Pins the multi-term example in the IsNodeOrGroupNegated docs: a "true" here describes the term's
        // syntactic context, not what the query excludes. See https://github.com/FoundatioFx/Foundatio.Parsers/issues/279.
        var parser = new LuceneQueryParser();

        // Act
        var result = parser.Parse("NOT (status:active AND region:us)");

        // Assert
        var notGroup = Assert.IsType<GroupNode>(result.Left);
        var statusTerm = Assert.IsType<TermNode>(notGroup.Left);
        var regionTerm = Assert.IsType<TermNode>(notGroup.Right);

        Assert.True(notGroup.HasParens);
        Assert.True(notGroup.IsExcluded());
        Assert.Equal(GroupOperator.And, notGroup.Operator);

        // Both terms report negated because their nearest enclosing parenthesized group is excluded, even
        // though the query only excludes records matching both terms - an active record outside the US
        // still matches. Neither term is individually negated.
        Assert.False(statusTerm.IsExcluded());
        Assert.False(regionTerm.IsExcluded());
        Assert.True(statusTerm.IsNodeOrGroupNegated());
        Assert.True(regionTerm.IsNodeOrGroupNegated());
    }

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
