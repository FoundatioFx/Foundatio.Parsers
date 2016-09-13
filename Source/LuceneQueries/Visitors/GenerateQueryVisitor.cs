using System;
using System.Text;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class GenerateQueryVisitor : QueryNodeVisitorWithResultBase<string> {
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

        public override string Accept(IQueryNode node) {
            node.Accept(this);
            return _builder.ToString();
        }

        public static string Run(IQueryNode node) {
            return new GenerateQueryVisitor().Accept(node);
        }
    }
}
