using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes;

/// <summary>
/// Represents a node in the parsed query abstract syntax tree (AST).
/// </summary>
/// <remarks>
/// All query elements (terms, groups, ranges) implement this interface.
/// Use <see cref="AcceptAsync"/> to traverse the tree with a visitor.
/// </remarks>
public interface IQueryNode
{
    /// <summary>
    /// The parent node in the AST, or null for the root node.
    /// </summary>
    IQueryNode Parent { get; set; }

    /// <summary>
    /// The child nodes of this node.
    /// </summary>
    IEnumerable<IQueryNode> Children { get; }

    /// <summary>
    /// Extensible property bag for storing custom data during visitor traversal.
    /// </summary>
    IDictionary<string, object> Data { get; }

    /// <summary>
    /// Accepts a visitor for tree traversal using the visitor pattern.
    /// </summary>
    Task<IQueryNode> AcceptAsync(IQueryNodeVisitor visitor, IQueryVisitorContext context);

    /// <summary>
    /// Returns the query string representation of this node.
    /// </summary>
    string ToString();

    /// <summary>
    /// Creates a deep copy of this node and its children.
    /// </summary>
    IQueryNode Clone();
}

/// <summary>
/// A query node that targets a specific field with optional negation.
/// </summary>
public interface IFieldQueryNode : IQueryNode
{
    /// <summary>
    /// Whether this node is negated (NOT operator applied).
    /// </summary>
    bool? IsNegated { get; set; }

    /// <summary>
    /// The prefix modifier (+, -, etc.) applied to this node.
    /// </summary>
    string Prefix { get; set; }

    /// <summary>
    /// The field name this query targets, or null for default field queries.
    /// </summary>
    string Field { get; set; }

    /// <summary>
    /// The field name with escape sequences resolved.
    /// </summary>
    string UnescapedField { get; }
}

/// <summary>
/// A field query node that supports Lucene boost and proximity operators.
/// </summary>
public interface IFieldQueryWithProximityAndBoostNode : IFieldQueryNode
{
    /// <summary>
    /// The boost factor (^) to increase or decrease relevance scoring.
    /// </summary>
    string Boost { get; set; }

    /// <summary>
    /// The boost value with escape sequences resolved.
    /// </summary>
    string UnescapedBoost { get; }

    /// <summary>
    /// The proximity/slop factor (~) for fuzzy or phrase proximity searches.
    /// </summary>
    string Proximity { get; set; }

    /// <summary>
    /// The proximity value with escape sequences resolved.
    /// </summary>
    string UnescapedProximity { get; }
}
