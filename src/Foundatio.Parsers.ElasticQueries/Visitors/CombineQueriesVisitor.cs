using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class CombineQueriesVisitor : ChainableQueryVisitor {
        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            base.Visit(node, context);

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));
            
			QueryBase query = node.GetQuery(() => node.GetDefaultQuery(context));
            QueryBase container = query;
            var nested = query as NestedQuery;
            if (nested != null)
                container = null;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var childQuery = child.GetQuery(() => child.GetDefaultQuery(context));
                var op = node.GetOperator(elasticContext.DefaultOperator);
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
                node.SetQuery(nested);
            } else {
                node.SetQuery(container);
            }
        }
    }
}
