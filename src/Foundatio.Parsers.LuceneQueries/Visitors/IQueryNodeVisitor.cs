using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryNodeVisitor {
        void Visit(GroupNode node, IQueryVisitorContext context);
        void Visit(TermNode node, IQueryVisitorContext context);
        void Visit(TermRangeNode node, IQueryVisitorContext context);
        void Visit(ExistsNode node, IQueryVisitorContext context);
        void Visit(MissingNode node, IQueryVisitorContext context);
    }
}
