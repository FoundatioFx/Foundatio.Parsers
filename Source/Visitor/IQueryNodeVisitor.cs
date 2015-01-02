using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public interface IQueryNodeVisitor {
        void Visit(GroupNode node);
        void Visit(TermNode node);
        void Visit(FieldExpressionNode node);
    }
}
