using System;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IChainableQueryVisitor : IQueryNodeVisitorWithResult<IQueryNode> {}
}
