using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public abstract class QueryNodeVisitorWithResultBase<T> : QueryNodeVisitorBase, IQueryNodeVisitorWithResult<T> {
        public abstract T Accept(IQueryNode node, IQueryVisitorContext context);
    }

    public abstract class ChainableQueryVisitor : QueryNodeVisitorWithResultBase<IQueryNode>, IChainableQueryVisitor {
        public override IQueryNode Accept(IQueryNode node, IQueryVisitorContext context) {
            node.Accept(this, context);
            return node;
        }
    }
}