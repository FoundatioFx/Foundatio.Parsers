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

        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            base.Visit(node, context);

            FilterContainer query = node.GetFilter()?.ToContainer();
            FilterContainer container = query;

            var nested = ((IFilterContainer)query)?.Nested as NestedFilter;
            if (nested != null)
                container = nested.Filter as FilterContainer;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                FilterContainer childFilter;
                if (child is GroupNode)
                    childFilter = child.GetFilterContainer() ?? child.GetFilterOrDefault()?.ToContainer();
                else
                    childFilter = child.GetFilterOrDefault()?.ToContainer();

                var op = node.GetOperator(_config.DefaultFilterOperator);
                if (child.IsNodeNegated())
                    childFilter = !childFilter;

                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    container &= childFilter;
                } else if (op == Operator.Or) {
                    container |= childFilter;
                }
            }

            if (nested != null) {
                nested.Filter = container;
                node.SetFilterContainer(nested);
            } else {
                node.SetFilterContainer(container);
            }
        }
    }
}
