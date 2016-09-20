using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter {
    public class CombineFiltersVisitor : ChainableQueryVisitor {
        private readonly ElasticQueryParserConfiguration _config;

        public CombineFiltersVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(GroupNode node) {
            if (node.GetFilter() != null) {
                base.Visit(node);
                return;
            }

            FilterContainer filter = null;
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var childFilter = child.GetFilter();
                var op = node.GetOperator(_config.DefaultFilterOperator);
                if (child.IsNodeNegated())
                    childFilter = !childFilter;

                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    filter &= childFilter;
                } else if (op == Operator.Or) {
                    filter |= childFilter;
                }
            }

            node.SetFilter(filter);
            base.Visit(node);
        }
    }
}
