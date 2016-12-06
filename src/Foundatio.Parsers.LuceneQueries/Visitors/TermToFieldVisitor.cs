using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class TermToFieldVisitor : ChainableQueryVisitor {
        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Term) || (node.Field != null && node.Field.StartsWith("@")))
                return;

            node.Field = node.Term;
            node.Term = null;
        }

        public static Task RunAsync(IQueryNode node, IQueryVisitorContext context = null) {
            return new TermToFieldVisitor().AcceptAsync(node, context);
        }

        public static void Run(IQueryNode node, IQueryVisitorContext context = null) {
            RunAsync(node, context).GetAwaiter().GetResult();
        }
    }
}
