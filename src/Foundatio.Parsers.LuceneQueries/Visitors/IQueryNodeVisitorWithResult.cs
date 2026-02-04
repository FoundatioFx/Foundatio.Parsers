using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// A visitor that produces a typed result after traversing the query AST.
/// </summary>
/// <typeparam name="T">The type of result produced by the visitor.</typeparam>
public interface IQueryNodeVisitorWithResult<T> : IQueryNodeVisitor
{
    /// <summary>
    /// Traverses the AST starting from the given node and returns a result.
    /// </summary>
    /// <param name="node">The root node to start traversal from.</param>
    /// <param name="context">The visitor context containing traversal state.</param>
    /// <returns>The result of visiting the AST.</returns>
    Task<T> AcceptAsync(IQueryNode node, IQueryVisitorContext context);
}
