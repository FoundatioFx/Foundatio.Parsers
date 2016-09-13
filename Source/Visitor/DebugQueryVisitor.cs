using System;
using System.CodeDom.Compiler;
using System.IO;
using System.Text;
using Exceptionless.LuceneQueryParser.Extensions;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class DebugQueryVisitor : QueryNodeVisitorWithResultBase<string> {
        private readonly StringBuilder _builder = new StringBuilder();
        private readonly IndentedTextWriter _writer;

        public DebugQueryVisitor() {
            _writer = new IndentedTextWriter(new StringWriter(_builder));
        }

        public override void Visit(GroupNode node) {
            _writer.WriteLine("Group:");
            _writer.Indent++;
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);

            _writer.WriteIf(node.Left != null, "Left - ");
            node.Left?.Accept(this);

            _writer.WriteIf(node.Right != null, "Right - ");
            node.Right?.Accept(this);

            _writer.WriteLineIf(node.Operator != GroupOperator.Default, "Operator: {0}", node.Operator);
            _writer.WriteLineIf(node.HasParens, "Parens: true");

            _writer.Indent--;
        }

        public override void Visit(TermNode node) {
            _writer.WriteLine("Term: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.WriteLine("IsQuoted: {0}", node.IsQuotedTerm);
            _writer.WriteLineIf(node.Term != null, "Term: {0}", node.Term);
            _writer.WriteLineIf(node.Boost != null, "Boost: {0}", node.Boost);
            _writer.WriteLineIf(node.Proximity != null, "Proximity: {0}", node.Proximity);

            _writer.Indent--;
        }

        public override void Visit(TermRangeNode node) {
            _writer.WriteLine("Term Range: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.WriteLineIf(node.Operator != null, "Operator: {0}", node.Operator);
            _writer.WriteLineIf(node.Max != null, "Max: {0}", node.Max);
            _writer.WriteLineIf(node.Min != null, "Min: {0}", node.Min);
            _writer.WriteLineIf(node.MinInclusive.HasValue, "MinInclusive: {0}", node.MinInclusive);
            _writer.WriteLineIf(node.MaxInclusive.HasValue, "MaxInclusive: {0}", node.MaxInclusive);

            _writer.Indent--;
        }

        public override void Visit(ExistsNode node) {
            _writer.WriteLine("Exists: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);

            _writer.Indent--;
        }

        public override void Visit(MissingNode node) {
            _writer.WriteLine("Missing: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);

            _writer.Indent--;
        }

        public override string Accept(IQueryNode node) {
            node.Accept(this);
            return _builder.ToString();
        }

        public static string Run(IQueryNode node) {
            return new DebugQueryVisitor().Accept(node);
        }
    }
}
