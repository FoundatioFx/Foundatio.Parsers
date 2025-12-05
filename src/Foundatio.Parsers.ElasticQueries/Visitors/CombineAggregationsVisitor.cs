using System;
using System.Collections.Generic;
using System.Linq;
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

        // Only process aggregations at the root GroupNode or at explicit nested groups
        // Skip intermediate GroupNodes that are part of the parsed tree structure
        if (node.Parent != null && String.IsNullOrEmpty(node.Field))
            return;

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var container = await GetParentContainerAsync(node, context);
        var termsAggregation = container as ITermsAggregation;

        // Group aggregations by nested path
        var nestedAggregations = new Dictionary<string, List<(IFieldQueryNode Node, AggregationBase Agg)>>();
        var regularAggregations = new List<(IFieldQueryNode Node, AggregationBase Agg)>();

        // Collect all field query nodes from immediate children (including nested GroupNodes)
        // GetAllFieldQueryNodes returns nodes in depth-first bottom-up order to match original visitor behavior
        foreach (var child in GetAllFieldQueryNodes(node))
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

        // Add regular aggregations
        foreach (var (child, aggregation) in regularAggregations)
        {
            AddAggregationToContainer(container, termsAggregation, child, aggregation);
        }

        // Add nested aggregations wrapped in NestedAggregation
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

                if (termsAggregation != null && (child.Prefix == "-" || child.Prefix == "+"))
                {
                    if (termsAggregation.Order == null)
                        termsAggregation.Order = new List<TermsOrder>();

                    termsAggregation.Order.Add(new TermsOrder
                    {
                        Key = ((IAggregation)aggregation).Name,
                        Order = child.Prefix == "-" ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
            }

            if (container is BucketAggregationBase bucketContainer)
            {
                if (bucketContainer.Aggregations == null)
                    bucketContainer.Aggregations = new AggregationDictionary();

                bucketContainer.Aggregations[((IAggregation)nestedAgg).Name] = (AggregationContainer)nestedAgg;
            }
        }

        if (node.Parent == null)
            node.SetAggregation(container);
    }

    private void AddAggregationToContainer(AggregationBase container, ITermsAggregation termsAggregation, IFieldQueryNode child, AggregationBase aggregation)
    {
        if (container is BucketAggregationBase bucketContainer)
        {
            if (bucketContainer.Aggregations == null)
                bucketContainer.Aggregations = new AggregationDictionary();

            bucketContainer.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
        }

        if (termsAggregation != null && (child.Prefix == "-" || child.Prefix == "+"))
        {
            if (termsAggregation.Order == null)
                termsAggregation.Order = new List<TermsOrder>();

            termsAggregation.Order.Add(new TermsOrder
            {
                Key = ((IAggregation)aggregation).Name,
                Order = child.Prefix == "-" ? SortOrder.Descending : SortOrder.Ascending
            });
        }
    }

    /// <summary>
    /// Gets all IFieldQueryNode descendants from a GroupNode, flattening nested GroupNodes.
    /// Returns nodes in depth-first bottom-up order to match the original visitor behavior
    /// where base.VisitAsync processes children first before the foreach loop adds aggregations.
    /// </summary>
    private static IEnumerable<IFieldQueryNode> GetAllFieldQueryNodes(GroupNode node)
    {
        // If Right is a GroupNode (without a field), recurse into it first
        // This matches the original behavior where the Right subtree's aggregations
        // are added during the recursive VisitAsync call before this node's foreach runs
        if (node.Right is GroupNode rightGroup && String.IsNullOrEmpty(rightGroup.Field))
        {
            foreach (var descendant in GetAllFieldQueryNodes(rightGroup))
                yield return descendant;

            // After recursing into Right, add Left (if it's an IFieldQueryNode)
            if (node.Left is GroupNode leftGroup && String.IsNullOrEmpty(leftGroup.Field))
            {
                foreach (var descendant in GetAllFieldQueryNodes(leftGroup))
                    yield return descendant;
            }
            else if (node.Left is IFieldQueryNode leftField)
            {
                yield return leftField;
            }
        }
        else
        {
            // Right is not a GroupNode (or has a Field), so iterate in [Left, Right] order
            // This handles the deepest nodes where both children are TermNodes
            if (node.Left is GroupNode leftGroup && String.IsNullOrEmpty(leftGroup.Field))
            {
                foreach (var descendant in GetAllFieldQueryNodes(leftGroup))
                    yield return descendant;
            }
            else if (node.Left is IFieldQueryNode leftField)
            {
                yield return leftField;
            }

            if (node.Right is GroupNode rightGroupWithField)
            {
                // Explicit nested groups (with a Field) should be returned as-is
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
