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

        public override void Visit(GroupNode node) {
            if (node.GetQuery() != null) {
                base.Visit(node);
                return;
            }

            QueryBase query = null;
            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var childQuery = child.GetQuery();
                var op = node.GetOperator(_config.DefaultQueryOperator);
                if (child.IsNodeNegated())
                    childQuery = !childQuery;

                if (op == Operator.Or && !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+")
                    op = Operator.And;

                if (op == Operator.And) {
                    query &= childQuery;
                } else if (op == Operator.Or) {
                    query |= childQuery;
                }
            }

            node.SetQuery(query);
            base.Visit(node);
        }
    }
}
