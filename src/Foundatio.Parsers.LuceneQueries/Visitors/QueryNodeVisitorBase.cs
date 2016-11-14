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
            var groupNode = node as GroupNode;
            if (groupNode != null)
                return VisitAsync(groupNode, context);

            var termNode = node as TermNode;
            if (termNode != null)
                return VisitAsync(termNode, context);

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                return VisitAsync(termRangeNode, context);

            var missingNode = node as MissingNode;
            if (missingNode != null)
                return VisitAsync(missingNode, context);

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                return VisitAsync(existsNode, context);

            return Task.CompletedTask;
        }
    }
}
