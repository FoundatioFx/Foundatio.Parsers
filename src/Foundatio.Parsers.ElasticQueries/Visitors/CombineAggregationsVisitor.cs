using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class CombineAggregationsVisitor : ChainableQueryVisitor
{
    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context)
    {
        await base.VisitAsync(node, context).ConfigureAwait(false);

        if (node.Parent != null && String.IsNullOrEmpty(node.Field))
            return;

        if (context is not IElasticQueryVisitorContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var container = await GetParentContainerAsync(node, context);
        var termsAggregation = container as ITermsAggregation;

        var nestedAggregations = new Dictionary<string, List<(IFieldQueryNode Node, AggregationBase Agg)>>();
        var regularAggregations = new List<(IFieldQueryNode Node, AggregationBase Agg)>();

        foreach (var child in GetLeafFieldNodes(node))
        {
            var aggregation = await child.GetAggregationAsync(() => child.GetDefaultAggregationAsync(context));
            if (aggregation == null)
                continue;

            string nestedPath = child.GetNestedPath();
            if (nestedPath != null)
            {
                if (!nestedAggregations.ContainsKey(nestedPath))
                    nestedAggregations[nestedPath] = new List<(IFieldQueryNode, AggregationBase)>();
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
            var nestedAgg = new NestedAggregation("nested_" + nestedPath)
            {
                Path = nestedPath,
                Aggregations = new AggregationDictionary()
            };

            foreach (var (child, aggregation) in childAggregations)
            {
                nestedAgg.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
                AddTermsOrder(termsAggregation, child, aggregation);
            }

            if (container is BucketAggregationBase bucketContainer)
            {
                bucketContainer.Aggregations ??= new AggregationDictionary();
                bucketContainer.Aggregations[((IAggregation)nestedAgg).Name] = (AggregationContainer)nestedAgg;
            }
        }

        if (node.Parent == null)
            node.SetAggregation(container);
    }

    private static void AddAggregation(AggregationBase container, ITermsAggregation termsAggregation, IFieldQueryNode child, AggregationBase aggregation)
    {
        if (container is BucketAggregationBase bucketContainer)
        {
            bucketContainer.Aggregations ??= new AggregationDictionary();
            bucketContainer.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
        }

        AddTermsOrder(termsAggregation, child, aggregation);
    }

    private static void AddTermsOrder(ITermsAggregation termsAggregation, IFieldQueryNode child, AggregationBase aggregation)
    {
        if (termsAggregation == null || (child.Prefix != "-" && child.Prefix != "+"))
            return;

        termsAggregation.Order ??= new List<TermsOrder>();
        termsAggregation.Order.Add(new TermsOrder
        {
            Key = ((IAggregation)aggregation).Name,
            Order = child.Prefix == "-" ? SortOrder.Descending : SortOrder.Ascending
        });
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

    private async Task<AggregationBase> GetParentContainerAsync(IQueryNode node, IQueryVisitorContext context)
    {
        AggregationBase container = null;
        var currentNode = node;
        while (container == null && currentNode != null)
        {
            IQueryNode n = currentNode;
            container = await n.GetAggregationAsync(async () =>
            {
                var result = await n.GetDefaultAggregationAsync(context);
                if (result != null)
                    n.SetAggregation(result);

                return result;
            });

            if (currentNode.Parent != null)
                currentNode = currentNode.Parent;
            else
                break;
        }

        if (container == null)
        {
            container = new ChildrenAggregation(null, null);
            currentNode.SetAggregation(container);
        }

        return container;
    }
}
