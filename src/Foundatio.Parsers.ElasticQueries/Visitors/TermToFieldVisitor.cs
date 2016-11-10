using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class TermToFieldVisitor : ChainableQueryVisitor {
        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Term))
                return;

            node.Field = node.Term;
            node.Term = null;
        }
    }
}
