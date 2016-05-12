using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public abstract class QueryNodeVisitorBase : IQueryNodeVisitor {
        public virtual void Visit(GroupNode node) { }
        public virtual void Visit(TermNode node) { }
        public virtual void Visit(TermRangeNode node) { }
        public virtual void Visit(ExistsNode node) { }
        public virtual void Visit(MissingNode node) { }
    }

    public abstract class QueryNodeVisitorWithResultBase<T> : IQueryNodeVisitor, IQueryNodeVisitorWithResult<T> {
        public virtual void Visit(GroupNode node) {}
        public virtual void Visit(TermNode node) {}
        public virtual void Visit(TermRangeNode node) {}
        public virtual void Visit(ExistsNode node) { }
        public virtual void Visit(MissingNode node) { }
        public abstract T Accept(IQueryNode node);
    }

    public interface IQueryNodeVisitorWithResult<T> {
        T Accept(IQueryNode node);
    }
}
