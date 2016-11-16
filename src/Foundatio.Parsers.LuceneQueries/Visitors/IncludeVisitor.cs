﻿using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class IncludeVisitor: ChainableQueryVisitor {
        private readonly Func<string, Task<string>> _resolveInclude;
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public IncludeVisitor(Func<string, Task<string>> resolveInclude = null) {
            _resolveInclude = resolveInclude;
        }

        public override async Task VisitAsync(TermNode node, IQueryVisitorContext context) {
            if (node.Field != "@include")
                return;

            string includedQuery = await _resolveInclude(node.Term).ConfigureAwait(false);
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