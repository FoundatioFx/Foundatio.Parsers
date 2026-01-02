using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineQueriesVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        // Only stop on scoped group nodes (parens). Gather all child queries (including scoped groups) and then combine them.
        // Combining only happens at the scoped group level though.
        // Merge all non-field terms together into a single match or multi-match query
        // Merge all nested queries for the same nested field together

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        Query query = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).ConfigureAwait(false);
        Query container = query;
        var nested = query?.Nested;
        if (nested != null && node.Parent != null)
            container = null;

        bool hasOrClauses = false;
        var filterQueries = new List<Query>(); // Collect AND queries for filter mode

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var childQuery = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).ConfigureAwait(false);
            if (childQuery == null) continue;

            var op = node.GetOperator(elasticContext);
            if (child.IsExcluded())
                childQuery = !childQuery;

            if (op == GroupOperator.Or && node.IsRequired())
                op = GroupOperator.And;

            if (op == GroupOperator.And)
            {
                if (elasticContext.UseScoring)
                {
                    container &= childQuery;
                }
                else
                {
                    // For filter mode, collect queries to build filter array directly
                    filterQueries.Add(childQuery);
                }
            }
            else if (op == GroupOperator.Or)
            {
                container |= childQuery;
                hasOrClauses = true;
            }
        }

        // For filter mode with AND queries, build a BoolQuery with filter array directly
        // Only wrap in BoolQuery if we have more than one filter query
        if (!elasticContext.UseScoring && filterQueries.Count > 1)
        {
            container = new BoolQuery
            {
                Filter = filterQueries
            };
        }
        else if (!elasticContext.UseScoring && filterQueries.Count == 1)
        {
            container = filterQueries[0];
        }

        // If we have OR clauses and the container is a BoolQuery with only should clauses, set minimum_should_match = 1
        // Set MinimumShouldMatch on root queries OR parens groups within an AND context.
        // Don't set it on parens groups within an OR context - this allows proper flattening of nested OR groups.
        bool isRootQuery = node.Parent == null;
        bool parentUsesAndOperator = node.Parent is GroupNode parentGroup && parentGroup.GetOperator(elasticContext) == GroupOperator.And;
        bool shouldSetMinimumShouldMatch = isRootQuery || (node.HasParens && parentUsesAndOperator);

        if (hasOrClauses && container?.Bool != null && shouldSetMinimumShouldMatch)
        {
            var boolQuery = container.Bool;
            bool hasOnlyShouldClauses = (boolQuery.Should?.Count > 0)
                && (boolQuery.Must == null || boolQuery.Must.Count == 0)
                && (boolQuery.Filter == null || boolQuery.Filter.Count == 0);

            if (hasOnlyShouldClauses && boolQuery.MinimumShouldMatch == null)
            {
                boolQuery.MinimumShouldMatch = 1;
            }
        }

        if (nested != null)
        {
            nested.Query = container;
            node.SetQuery(nested);
        }
        else
        {
            node.SetQuery(container);
        }
    }
}
