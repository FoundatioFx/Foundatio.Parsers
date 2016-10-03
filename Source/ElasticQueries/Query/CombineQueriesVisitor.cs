using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Query {
    public class CombineQueriesVisitor : ChainableQueryVisitor {
        private readonly ElasticQueryParserConfiguration _config;

        public CombineQueriesVisitor(ElasticQueryParserConfiguration config) {
            _config = config;
        }

        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            base.Visit(node, context);

            QueryContainer query = node.GetQuery()?.ToContainer();
            QueryContainer container = query;
            
            var nested = ((IQueryContainer)query)?.Nested as NestedQuery;
            if (nested != null)
                container = nested.Query as QueryContainer;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                QueryContainer childQuery;
                if (child is GroupNode)
                    childQuery = child.GetQueryContainer() ?? child.GetQueryOrDefault()?.ToContainer();
                else
                    childQuery = child.GetQueryOrDefault()?.ToContainer();

                var op = node.GetOperator(_config.DefaultQueryOperator);
                if (child.IsNodeNegated())
                    childQuery = !childQuery;

                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    container &= childQuery;
                } else if (op == Operator.Or) {
                    container |= childQuery;
                }
            }

            if (nested != null) {
                nested.Query = container;
                node.SetQueryContainer(nested);
            } else {
                node.SetQueryContainer(container);
            }
        }
    }
}
