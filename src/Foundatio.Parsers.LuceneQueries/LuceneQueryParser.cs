using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries;

public partial class LuceneQueryParser : IQueryParser
{
    public virtual Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null)
    {
        var result = Parse(query);
        return Task.FromResult<IQueryNode>(result);
    }

    public IQueryNode Parse(string query, IQueryVisitorContext context)
    {
        return ParseAsync(query, context).GetAwaiter().GetResult();
    }
}

/// <summary>
/// Defines a parser that converts query strings into an AST.
/// </summary>
public interface IQueryParser
{
    /// <summary>
    /// Parses a query string into an abstract syntax tree.
    /// </summary>
    /// <param name="query">The query string to parse.</param>
    /// <param name="context">Optional visitor context for parsing configuration.</param>
    /// <returns>The root node of the parsed query AST.</returns>
    Task<IQueryNode> ParseAsync(string query, IQueryVisitorContext context = null);
}
