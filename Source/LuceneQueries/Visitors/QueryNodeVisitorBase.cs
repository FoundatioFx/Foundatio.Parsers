using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public abstract class QueryNodeVisitorBase : IQueryNodeVisitor {
        public virtual void Visit(GroupNode node, IQueryVisitorContext context) {
            foreach (var child in node.Children)
                Visit(child, context);
        }

        public virtual void Visit(TermNode node, IQueryVisitorContext context) { }
        public virtual void Visit(TermRangeNode node, IQueryVisitorContext context) { }
        public virtual void Visit(ExistsNode node, IQueryVisitorContext context) { }
        public virtual void Visit(MissingNode node, IQueryVisitorContext context) { }

        public virtual void Visit(IQueryNode node, IQueryVisitorContext context) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                Visit(groupNode, context);

            var termNode = node as TermNode;
            if (termNode != null)
                Visit(termNode, context);

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                Visit(termRangeNode, context);

            var missingNode = node as MissingNode;
            if (missingNode != null)
                Visit(missingNode, context);

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                Visit(existsNode, context);
        }
    }
}
