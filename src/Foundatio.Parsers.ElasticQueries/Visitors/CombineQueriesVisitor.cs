using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Foundatio.Parsers.ElasticQueries.Visitors {

    public class CombineQueriesVisitor : ChainableQueryVisitor {

        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await base.VisitAsync(node, context).ConfigureAwait(false);

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            QueryBase query = node.GetQuery(() => node.GetDefaultQuery(context));
            QueryBase container = query;
            var nested = query as NestedQuery;
            if (nested != null && node.Parent != null)
                container = null;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var childQuery = child.GetQuery(() => child.GetDefaultQuery(context));
                if (childQuery == null) continue;
                
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

        public override async Task VisitAsync(TermNode node, IQueryVisitorContext context) {
            await base.VisitAsync(node, context).ConfigureAwait(false);

            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            var scopedNode = node.GetScopedNode() as IFieldQueryNode;
            if (scopedNode != null && node.Field == null && 
                (node.GetQuery(() => node.GetDefaultQuery(context)) is MatchQuery || node.GetQuery(() => node.GetDefaultQuery(context)) is MultiMatchQuery)) {
                if (!scopedNode.Data.ContainsKey("match_terms")) {
                    scopedNode.Data["match_terms"] = new List<TermNode>();
                }
                ((List<TermNode>)scopedNode.Data["match_terms"]).Add(node);
                node.SetQuery(null);
            }
        }
    }
}