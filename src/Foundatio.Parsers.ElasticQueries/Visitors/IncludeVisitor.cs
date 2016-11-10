using System;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class IncludeVisitor: ChainableQueryVisitor {
        private readonly Func<string, string> _resolveInclude;
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public IncludeVisitor(Func<string, string> resolveInclude = null) {
            _resolveInclude = resolveInclude;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null || node.Field != "@include")
                return;

            string includedQuery = _resolveInclude(node.Term);
            if (String.IsNullOrEmpty(includedQuery))
                return;

            var result = _parser.Parse(includedQuery);
            var parent = node.Parent as GroupNode;
            if (parent == null)
                return;

            if (parent.Left == node)
                parent.Left = result;
            else if (parent.Right == node)
                parent.Right = result;
        }
    }
}