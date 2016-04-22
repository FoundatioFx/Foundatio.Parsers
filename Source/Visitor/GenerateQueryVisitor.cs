using System;
using System.Text;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class GenerateQueryVisitor : QueryNodeVisitorBase {
        private readonly StringBuilder _builder = new StringBuilder();

        public override void Visit(GroupNode node) {
            _builder.Append(node);
        }

        public override void Visit(TermNode node) {
            _builder.Append(node);
        }

        public override void Visit(TermRangeNode node) {
            _builder.Append(node);
        }

        public override void Visit(ExistsNode node) {
            _builder.Append(node);
        }

        public override void Visit(MissingNode node) {
            _builder.Append(node);
        }

        public string Query => _builder.ToString();

        public static string Run(IQueryNode node) {
            var visitor = new GenerateQueryVisitor();
            node.Accept(visitor, false);
            return visitor.Query;
        }
    }
}
