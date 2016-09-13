using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public abstract class QueryNodeVisitorBase : IQueryNodeVisitor {
        public virtual void Visit(GroupNode node) {
            foreach (var child in node.Children)
                Visit(child);
        }

        public virtual void Visit(TermNode node) { }
        public virtual void Visit(TermRangeNode node) { }
        public virtual void Visit(ExistsNode node) { }
        public virtual void Visit(MissingNode node) { }

        public virtual void Visit(IQueryNode node) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                Visit(groupNode);

            var termNode = node as TermNode;
            if (termNode != null)
                Visit(termNode);

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                Visit(termRangeNode);

            var missingNode = node as MissingNode;
            if (missingNode != null)
                Visit(missingNode);

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                Visit(existsNode);
        }
    }
}
