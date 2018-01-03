using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public abstract class QueryNodeVisitorBase : IQueryNodeVisitor {
        public virtual async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            foreach (var child in node.Children)
                await VisitAsync(child, context).ConfigureAwait(false);
        }

        public virtual void Visit(TermNode node, IQueryVisitorContext context) {}

        public virtual Task VisitAsync(TermNode node, IQueryVisitorContext context) {
            Visit(node, context);
            return Task.CompletedTask;
        }

        public virtual void Visit(TermRangeNode node, IQueryVisitorContext context) {}

        public virtual Task VisitAsync(TermRangeNode node, IQueryVisitorContext context) {
            Visit(node, context);
            return Task.CompletedTask;
        }

        public virtual void Visit(ExistsNode node, IQueryVisitorContext context) {}

        public virtual Task VisitAsync(ExistsNode node, IQueryVisitorContext context) {
            Visit(node, context);
            return Task.CompletedTask;
        }

        public virtual void Visit(MissingNode node, IQueryVisitorContext context) {}

        public virtual Task VisitAsync(MissingNode node, IQueryVisitorContext context) {
            Visit(node, context);
            return Task.CompletedTask;
        }

        public virtual Task VisitAsync(IQueryNode node, IQueryVisitorContext context) {
            if (node is GroupNode groupNode)
                return VisitAsync(groupNode, context);

            if (node is TermNode termNode)
                return VisitAsync(termNode, context);

            if (node is TermRangeNode termRangeNode)
                return VisitAsync(termRangeNode, context);

            if (node is MissingNode missingNode)
                return VisitAsync(missingNode, context);

            if (node is ExistsNode existsNode)
                return VisitAsync(existsNode, context);

            return Task.CompletedTask;
        }
    }
}
