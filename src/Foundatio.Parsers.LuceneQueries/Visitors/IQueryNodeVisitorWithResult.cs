using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryNodeVisitorWithResult<T> : IQueryNodeVisitor {
        T Accept(IQueryNode node, IQueryVisitorContext context);
    }
}