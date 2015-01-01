using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class GenerateQueryVisitor : QueryNodeVisitorBase {
        public override void Visit(GroupNode node) {
            base.Visit(node);
        }

        public override void Visit(TermNode node) {
            base.Visit(node);
        }

        public override void Visit(FieldExpressionNode node) {
            base.Visit(node);
        }
    }
}
