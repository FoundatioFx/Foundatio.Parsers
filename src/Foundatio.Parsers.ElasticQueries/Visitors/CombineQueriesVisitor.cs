using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Foundatio.Parsers.ElasticQueries.Visitors {

    public class CombineQueriesVisitor : ChainableQueryVisitor {

        public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            await base.VisitAsync(node, context).ConfigureAwait(false);
            
            // Only stop on scoped group nodes (parens). Gather all child queries (including scoped groups) and then combine them.
            // Combining only happens at the scoped group level though.
            // Merge all non-field terms together into a single match or multi-match query
            // Merge all nested queries for the same nested field together

            if (context is not IElasticQueryVisitorContext elasticContext)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            QueryBase query = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).ConfigureAwait(false);
            QueryBase container = query;
            var nested = query as NestedQuery;
            if (nested != null && node.Parent != null)
                container = null;

            foreach (var child in node.Children.OfType<IFieldQueryNode>()) {
                var childQuery = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).ConfigureAwait(false);
                if (childQuery == null) continue;

                var op = node.GetOperator(elasticContext);
                if (child.IsExcluded())
                    childQuery = !childQuery;

                if (op == GroupOperator.Or && node.IsRequired())
                    op = GroupOperator.And;

                if (op == GroupOperator.And) {
                    container &= childQuery;
                } else if (op == GroupOperator.Or) {
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