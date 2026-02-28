using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineAggregationsVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        // Skip fieldless intermediate groups; their children are collected
        // by GetLeafFieldNodes from the nearest root or named-field ancestor.
        if (node.Parent is not null && String.IsNullOrEmpty(node.Field))
            return;

        if (context is not IElasticQueryVisitorContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var container = await GetParentContainerAsync(node, context);
        var termsAggregation = container.Value as TermsAggregation;

        var nestedAggregations = new Dictionary<string, List<(IFieldQueryNode Node, AggregationMap Agg)>>();
        var regularAggregations = new List<(IFieldQueryNode Node, AggregationMap Agg)>();

        foreach (var child in GetLeafFieldNodes(node))
        {
            var aggregation = await child.GetAggregationAsync(() => child.GetDefaultAggregationAsync(context));
            if (aggregation is null)
                continue;

            string nestedPath = child.GetNestedPath();
            if (nestedPath is not null)
            {
                if (!nestedAggregations.ContainsKey(nestedPath))
                    nestedAggregations[nestedPath] = [];
                nestedAggregations[nestedPath].Add((child, aggregation));
            }
            else
            {
                regularAggregations.Add((child, aggregation));
            }
        }

        foreach (var (child, aggregation) in regularAggregations)
        {
            AddAggregation(container, termsAggregation, child, aggregation);
        }

        foreach (var (nestedPath, childAggregations) in nestedAggregations)
        {
            var nestedAgg = new AggregationMap("nested_" + nestedPath, new NestedAggregation { Path = nestedPath });

            foreach (var (child, aggregation) in childAggregations)
            {
                nestedAgg.Aggregations.Add(aggregation);
                AddTermsOrder(termsAggregation, child, aggregation);
            }

            if (container.Value is null || container.Value.IsBucketAggregation())
            {
                container.Aggregations.Add(nestedAgg);
            }
        }

        if (node.Parent is null)
            node.SetAggregation(container);
    }

    private static void AddAggregation(AggregationMap container, TermsAggregation termsAggregation, IFieldQueryNode child, AggregationMap aggregation)
    {
        if (container.Value is null || container.Value.IsBucketAggregation())
        {
            container.Aggregations.Add(aggregation);
        }

        AddTermsOrder(termsAggregation, child, aggregation);
    }

    private static void AddTermsOrder(TermsAggregation termsAggregation, IFieldQueryNode child, AggregationMap aggregation)
    {
        if (termsAggregation is null || (child.Prefix != "-" && child.Prefix != "+"))
            return;

        termsAggregation.Order ??= new List<KeyValuePair<Field, SortOrder>>();
        termsAggregation.Order.Add(new KeyValuePair<Field, SortOrder>(aggregation.Name, child.Prefix == "-" ? SortOrder.Desc : SortOrder.Asc));
    }

    /// <summary>
    /// Collects all leaf IFieldQueryNode descendants in the same order as the original
    /// recursive post-order visitor. In a right-recursive AST, the Right subtree is
    /// processed via recursion before the Left child is yielded at each level.
    /// GroupNodes with a Field (explicit nested groups) are returned as-is.
    /// </summary>
    private static IEnumerable<IFieldQueryNode> GetLeafFieldNodes(GroupNode node)
    {
        if (node.Right is GroupNode rightGroup && String.IsNullOrEmpty(rightGroup.Field))
        {
            foreach (var descendant in GetLeafFieldNodes(rightGroup))
                yield return descendant;

            if (node.Left is GroupNode leftGroup && String.IsNullOrEmpty(leftGroup.Field))
            {
                foreach (var descendant in GetLeafFieldNodes(leftGroup))
                    yield return descendant;
            }
            else if (node.Left is IFieldQueryNode leftField)
            {
                yield return leftField;
            }
        }
        else
        {
            if (node.Left is GroupNode leftGroup && String.IsNullOrEmpty(leftGroup.Field))
            {
                foreach (var descendant in GetLeafFieldNodes(leftGroup))
                    yield return descendant;
            }
            else if (node.Left is IFieldQueryNode leftField)
            {
                yield return leftField;
            }

            if (node.Right is GroupNode rightGroupWithField)
            {
                yield return rightGroupWithField;
            }
            else if (node.Right is IFieldQueryNode rightField)
            {
                yield return rightField;
            }
        }
    }

    private async Task<AggregationMap> GetParentContainerAsync(IQueryNode node, IQueryVisitorContext context)
    {
        AggregationMap container = null;
        var currentNode = node;
        while (container is null && currentNode is not null)
        {
            IQueryNode n = currentNode;
            container = await n.GetAggregationAsync(async () =>
            {
                var result = await n.GetDefaultAggregationAsync(context);
                if (result is not null)
                    n.SetAggregation(result);

                return result;
            });

            if (currentNode.Parent is not null)
                currentNode = currentNode.Parent;
            else
                break;
        }

        if (container is null)
        {
            container = new AggregationMap(null, null);
            currentNode.SetAggregation(container);
        }

        return container;
    }
}
