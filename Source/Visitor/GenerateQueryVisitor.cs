using System;
using System.Text;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class GenerateQueryVisitor : QueryNodeVisitorBase {
        private readonly StringBuilder _builder = new StringBuilder();

        public override void Visit(GroupNode node) {
            if (node.Field != null)
                node.Field.Accept(this, false);

            if (node.HasParens)
                _builder.Append("(");

            if (node.Left != null)
                node.Left.Accept(this, false);

            if (!String.IsNullOrEmpty(node.Operator))
                _builder.Append(" " + node.Operator + " ");
            else if (node.Right != null)
                _builder.Append(" ");

            if (node.Right != null)
                node.Right.Accept(this, false);

            if (node.HasParens)
                _builder.Append(")");
        }

        public override void Visit(TermNode node) {
            _builder.Append(node.Prefix);

            if (node.Field != null)
                node.Field.Accept(this, false);

            if (node.MinInclusive.HasValue)
                _builder.Append(node.MinInclusive.Value ? "[" : "{");

            _builder.Append(node.TermMin);

            if (!String.IsNullOrEmpty(node.TermMin) && !String.IsNullOrEmpty(node.TermMax))
                _builder.Append(node.TermDelimiter ?? " TO ");

            _builder.Append(node.TermMax);

            if (node.MaxInclusive.HasValue)
                _builder.Append(node.MaxInclusive.Value ? "]" : "}");

            _builder.Append(node.IsQuotedTerm ? "\"" + node.Term + "\"" : node.Term);

            if (node.Boost.HasValue)
                _builder.Append("^" + node.Boost);

            if (node.Proximity.HasValue)
                _builder.Append("~" + (node.Proximity.Value != Double.MinValue ? node.Proximity.ToString() : String.Empty));
        }

        public override void Visit(FieldExpressionNode node) {
            if (String.IsNullOrEmpty(node.Field))
                return;

            _builder.Append(node.Prefix);
            _builder.Append(node.Field);
            _builder.Append(":");
        }

        public string Query { get { return _builder.ToString(); } }

        public static string Run(IQueryNode node) {
            var visitor = new GenerateQueryVisitor();
            node.Accept(visitor, false);
            return visitor.Query;
        }
    }
}
