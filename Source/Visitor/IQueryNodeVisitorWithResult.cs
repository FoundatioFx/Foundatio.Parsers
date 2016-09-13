using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public interface IQueryNodeVisitorWithResult<T>: IQueryNodeVisitor {
        T Accept(IQueryNode node);
    }
}