using System;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public interface IChainableQueryVisitor : IQueryNodeVisitorWithResult<IQueryNode> {}
}
