using System;
using System.Text;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class GenerateQueryVisitor : QueryNodeVisitorWithResultBase<string> {
        private readonly StringBuilder _builder = new StringBuilder();

        public override void Visit(GroupNode node) {
            _builder.Append(node.ToString(true));
        }

        public override void Visit(TermNode node) {
            _builder.Append(node.ToString(true));
        }

        public override void Visit(TermRangeNode node) {
            _builder.Append(node.ToString(true));
        }

        public override void Visit(ExistsNode node) {
            _builder.Append(node.ToString(true));
        }

        public override void Visit(MissingNode node) {
            _builder.Append(node.ToString(true));
        }

        public override string Accept(IQueryNode node) {
            node.Accept(this, false);
            return _builder.ToString();
        }

        public static string Run(IQueryNode node) {
            return new GenerateQueryVisitor().Accept(node);
        }
    }
}
