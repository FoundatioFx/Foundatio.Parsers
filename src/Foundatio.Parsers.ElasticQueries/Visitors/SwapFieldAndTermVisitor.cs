using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class SwapFieldAndTermVisitor: ChainableQueryVisitor {
        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field)) {
                var leftTerm = node.Left as TermNode;
                if (leftTerm == null || !String.IsNullOrEmpty(leftTerm.Field))
                    throw new ApplicationException("The first item in an aggregation group must be the name of the target field.");

                leftTerm.Field = node.Field;
                node.Field = leftTerm.Term;
                leftTerm.Term = null;
            }

            base.Visit(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Term))
                return;

            var field = node.Field;
            node.Field = node.Term;
            node.Term = field;
        }
    }
}
