using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

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

        QueryBase query = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).ConfigureAwait(false);
        QueryBase container = query;
        var nested = query as NestedQuery;
        if (nested != null && node.Parent != null)
            container = null;

        var op = node.GetOperator(elasticContext);

        // Group nested queries by their path for combining
        var nestedQueries = new Dictionary<string, List<(IFieldQueryNode Node, QueryContainer InnerQuery)>>();
        var regularQueries = new List<(IFieldQueryNode Node, QueryBase Query)>();

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var childQuery = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).ConfigureAwait(false);
            if (childQuery == null) continue;

            // Check if this is a nested query from an individual term node (not an explicit nested group)
            // Explicit nested groups (like "nested:(...)") are GroupNodes with a nested Field
            // We only want to combine nested queries from individual term nodes
            bool isExplicitNestedGroup = child is GroupNode groupChild && !String.IsNullOrEmpty(groupChild.Field);

            if (childQuery is NestedQuery childNested && childNested.Path != null && !isExplicitNestedGroup)
            {
                string pathKey = childNested.Path.Name;
                if (!nestedQueries.ContainsKey(pathKey))
                    nestedQueries[pathKey] = new List<(IFieldQueryNode, QueryContainer)>();
                nestedQueries[pathKey].Add((child, childNested.Query));
            }
            else
            {
                regularQueries.Add((child, childQuery));
            }
        }

        // Process regular queries
        foreach (var (child, childQuery) in regularQueries)
        {
            var q = childQuery;
            if (child.IsExcluded())
                q = !q;

            var effectiveOp = op;
            if (effectiveOp == GroupOperator.Or && node.IsRequired())
                effectiveOp = GroupOperator.And;

            if (effectiveOp == GroupOperator.And)
                container &= q;
            else if (effectiveOp == GroupOperator.Or)
                container |= q;
        }

        // Process nested queries - combine queries with the same path
        foreach (var (path, pathQueries) in nestedQueries)
        {
            QueryContainer combinedInner = null;

            foreach (var (child, innerQuery) in pathQueries)
            {
                QueryContainer q = innerQuery;
                if (child.IsExcluded())
                    q = !q;

                var effectiveOp = op;
                if (effectiveOp == GroupOperator.Or && node.IsRequired())
                    effectiveOp = GroupOperator.And;

                if (effectiveOp == GroupOperator.And)
                    combinedInner &= q;
                else if (effectiveOp == GroupOperator.Or)
                    combinedInner |= q;
            }

            var combinedNested = new NestedQuery { Path = path, Query = combinedInner };
            QueryBase nestedToAdd = combinedNested;

            var effectiveContainerOp = op;
            if (effectiveContainerOp == GroupOperator.Or && node.IsRequired())
                effectiveContainerOp = GroupOperator.And;

            if (effectiveContainerOp == GroupOperator.And)
                container &= nestedToAdd;
            else if (effectiveContainerOp == GroupOperator.Or)
                container |= nestedToAdd;
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
