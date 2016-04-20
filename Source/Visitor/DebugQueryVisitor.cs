using System.CodeDom.Compiler;
using System.IO;
using System.Text;
using Exceptionless.LuceneQueryParser.Extensions;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class DebugQueryVisitor : QueryNodeVisitorBase {
        private readonly StringBuilder _builder = new StringBuilder();
        private readonly IndentedTextWriter _writer;

        public DebugQueryVisitor() {
            _writer = new IndentedTextWriter(new StringWriter(_builder));
        }

        public override void Visit(GroupNode node) {
            _writer.WriteLine("Group:");
            _writer.Indent++;

            node.Field?.Accept(this, false);

            _writer.WriteIf(node.Left != null, "Left - ");
            node.Left?.Accept(this, false);

            _writer.WriteIf(node.Right != null, "Right - ");
            node.Right?.Accept(this, false);

            _writer.WriteLineIf(node.Operator != null, "Operator: {0}", node.Operator);
            _writer.WriteLineIf(node.HasParens, "Parens: true");

            _writer.Indent--;
        }

        public override void Visit(TermNode node) {
            _writer.WriteLine("Term: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.WriteLineIf(node.Term != null, "Term: {0}", node.Term);
            _writer.WriteLineIf(node.TermMax != null, "TermMax: {0}", node.TermMax);
            _writer.WriteLineIf(node.TermMin != null, "TermMin: {0}", node.TermMin);
            _writer.WriteLineIf(node.Boost.HasValue, "Boost: {0}", node.Boost);
            _writer.WriteLineIf(node.MinInclusive.HasValue, "MinInclusive: {0}", node.MinInclusive);
            _writer.WriteLineIf(node.MaxInclusive.HasValue, "MaxInclusive: {0}", node.MaxInclusive);
            _writer.WriteLineIf(node.Proximity.HasValue, "Proximity: {0}", node.Proximity);
            
            if (node.Field != null)
                node.Field.Accept(this, false);

            _writer.Indent--;
        }

        public override void Visit(FieldExpressionNode node) {
            _writer.WriteLine("Field: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Name: {0}", node.Field);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.Indent--;
        }

        public string Result => _builder.ToString();

        public static string Run(IQueryNode node) {
            var visitor = new DebugQueryVisitor();
            node.Accept(visitor, false);
            return visitor.Result;
        }
    }
}
