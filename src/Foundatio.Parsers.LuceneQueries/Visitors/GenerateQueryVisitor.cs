using System;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class GenerateQueryVisitor : QueryNodeVisitorWithResultBase<string> {
        private readonly StringBuilder _builder = new StringBuilder();

        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            _builder.Append(node);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            _builder.Append(node);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            _builder.Append(node);
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            _builder.Append(node);
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            _builder.Append(node);
        }

        public override string Accept(IQueryNode node, IQueryVisitorContext context) {
            node.Accept(this, context);
            return _builder.ToString();
        }

        public static string Run(IQueryNode node) {
            return new GenerateQueryVisitor().Accept(node, null);
        }
    }
}
