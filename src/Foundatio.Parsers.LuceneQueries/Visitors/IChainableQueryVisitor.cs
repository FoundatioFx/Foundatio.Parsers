using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// A visitor that can be composed with other visitors in a processing pipeline.
/// </summary>
/// <remarks>
/// Use <see cref="ChainedQueryVisitor"/> to combine multiple chainable visitors.
/// </remarks>
public interface IChainableQueryVisitor : IQueryNodeVisitorWithResult<IQueryNode> { }
