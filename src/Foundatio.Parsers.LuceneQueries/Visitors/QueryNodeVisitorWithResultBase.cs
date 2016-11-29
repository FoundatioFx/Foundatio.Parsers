using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public abstract class QueryNodeVisitorWithResultBase<T> : QueryNodeVisitorBase, IQueryNodeVisitorWithResult<T> {
        public abstract Task<T> AcceptAsync(IQueryNode node, IQueryVisitorContext context);
    }

    public abstract class ChainableQueryVisitor : QueryNodeVisitorWithResultBase<IQueryNode>, IChainableQueryVisitor {
        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context).ConfigureAwait(false);
            return node;
        }
    }
}