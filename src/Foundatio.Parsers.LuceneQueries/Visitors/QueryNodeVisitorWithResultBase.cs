using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public abstract class QueryNodeVisitorWithResultBase<T> : QueryNodeVisitorBase, IQueryNodeVisitorWithResult<T> {
        public abstract Task<T> AcceptAsync(IQueryNode node, IQueryVisitorContext context);
    }

    public abstract class ChainableQueryVisitor : QueryNodeVisitorWithResultBase<IQueryNode>, IChainableQueryVisitor {
        public override Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            node.AcceptAsync(this, context);
            return Task.FromResult(node);
        }
    }
}