using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryNodeVisitor {
        void Visit(GroupNode node);
        void Visit(TermNode node);
        void Visit(TermRangeNode node);
        void Visit(ExistsNode node);
        void Visit(MissingNode node);
    }
}
