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
            
            // TODO: Only stop on scoped group nodes (parens). Gather all child queries (including scoped groups) and then combine them.
            // Combining only happens at the scoped group level though.
            // Merge all non-field terms together into a single match or multi-match query
            // Merge all nested queries for the same nested field together

            if (!(context is IElasticQueryVisitorContext elasticContext))
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
                if (child.IsExcluded())
                    childQuery = !childQuery;

                if (op == Operator.Or && node.IsRequired())
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

            if (node.GetGroupNode() is IFieldQueryNode groupNode && node.Field == null
                && (node.GetQuery(() => node.GetDefaultQuery(context)) is MatchQuery || node.GetQuery(() => node.GetDefaultQuery(context)) is MultiMatchQuery)) {
                if (!groupNode.Data.ContainsKey("match_terms"))
                    groupNode.Data["match_terms"] = new List<TermNode>();
                
                ((List<TermNode>)groupNode.Data["match_terms"]).Add(node);
                node.SetQuery(null);
            }
        }
    }
}