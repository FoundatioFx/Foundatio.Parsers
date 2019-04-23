using System;
using System.CodeDom.Compiler;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class DebugQueryVisitor : QueryNodeVisitorWithResultBase<string> {
        private readonly StringBuilder _builder = new StringBuilder();
        private readonly IndentedTextWriter _writer;

        public DebugQueryVisitor() {
            _writer = new IndentedTextWriter(new StringWriter(_builder));
        }

        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await _writer.WriteLineAsync("Group:").ConfigureAwait(false);
            _writer.Indent++;
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.GetOriginalField() != null, "Field: {0}", node.GetOriginalField());
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.WriteLineIf(node.Boost != null, "Boost: {0}", node.Boost);
            _writer.WriteLineIf(node.Proximity != null, "Proximity: {0}", node.Proximity);

            _writer.WriteIf(node.Left != null, "Left - ");
            if (node.Left != null)
                await node.Left.AcceptAsync(this, context).ConfigureAwait(false);

            _writer.WriteIf(node.Right != null, "Right - ");
            if (node.Right != null)
                await node.Right.AcceptAsync(this, context).ConfigureAwait(false);

            _writer.WriteLineIf(node.Operator != GroupOperator.Default, "Operator: {0}", node.Operator);
            _writer.WriteLineIf(node.HasParens, "Parens: true");

            WriteData(node);

            _writer.Indent--;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            _writer.WriteLine("Term: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.GetOriginalField() != null, "Field: {0}", node.GetOriginalField());
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.WriteLine("IsQuoted: {0}", node.IsQuotedTerm);
            _writer.WriteLineIf(node.Term != null, "Term: {0}", node.Term);
            _writer.WriteLineIf(node.Boost != null, "Boost: {0}", node.Boost);
            _writer.WriteLineIf(node.Proximity != null, "Proximity: {0}", node.Proximity);

            WriteData(node);

            _writer.Indent--;
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            _writer.WriteLine("Term Range: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.GetOriginalField() != null, "Original Field: {0}", node.GetOriginalField());
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);
            _writer.WriteLineIf(node.Operator != null, "Operator: {0}", node.Operator);
            _writer.WriteLineIf(node.Max != null, "Max: {0}", node.Max);
            _writer.WriteLineIf(node.Min != null, "Min: {0}", node.Min);
            _writer.WriteLineIf(node.MinInclusive.HasValue, "MinInclusive: {0}", node.MinInclusive);
            _writer.WriteLineIf(node.MaxInclusive.HasValue, "MaxInclusive: {0}", node.MaxInclusive);

            WriteData(node);

            _writer.Indent--;
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            _writer.WriteLine("Exists: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.GetOriginalField() != null, "Original Field: {0}", node.GetOriginalField());
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);

            WriteData(node);

            _writer.Indent--;
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            _writer.WriteLine("Missing: ");
            _writer.Indent++;
            _writer.WriteLineIf(node.Field != null, "Field: {0}", node.Field);
            _writer.WriteLineIf(node.GetOriginalField() != null, "Original Field: {0}", node.GetOriginalField());
            _writer.WriteLineIf(node.IsNegated.HasValue, "IsNegated: {0}", node.IsNegated);
            _writer.WriteLineIf(node.Prefix != null, "Prefix: {0}", node.Prefix);

            WriteData(node);

            _writer.Indent--;
        }

        private void WriteData(QueryNodeBase node) {
            if (node.Data.Count <= 0)
                return;

            _writer.WriteLine("Data:");
            _writer.Indent++;
            foreach (var kvp in node.Data) {
                _writer.Write(kvp.Key);
                _writer.Write(": ");
                _writer.WriteLine(kvp.Value.ToString());
            }
            _writer.Indent--;
        }

        public override async Task<string> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context);
            return _builder.ToString();
        }

        public static Task<string> RunAsync(IQueryNode node, IQueryVisitorContext context = null) {
            return new DebugQueryVisitor().AcceptAsync(node, context);
        }

        public static string Run(IQueryNode node, IQueryVisitorContext context = null) {
            return RunAsync(node, context).GetAwaiter().GetResult();
        }
    }
}
