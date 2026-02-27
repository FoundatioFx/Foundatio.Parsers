using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// Defines a visitor for traversing query AST nodes.
/// </summary>
/// <remarks>
/// Implement this interface to create custom query transformations or analysis.
/// </remarks>
public interface IQueryNodeVisitor
{
    /// <summary>
    /// Visits a node in the query AST.
    /// </summary>
    /// <param name="node">The node to visit.</param>
    /// <param name="context">The visitor context containing traversal state.</param>
    /// <returns>The visited node, potentially transformed.</returns>
    Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context);
}
