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

        // Skip fieldless intermediate groups; their children are collected
        // by GetLeafFieldNodes from the nearest root or named-field ancestor.
        if (node.Parent is not null && String.IsNullOrEmpty(node.Field))
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
            if (aggregation is null)
                continue;

            string? nestedPath = child.GetNestedPath();
            if (nestedPath is not null)
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
            var innermostAgg = new NestedAggregation($"nested_{nestedPath}")
            {
                Path = nestedPath,
                Aggregations = new AggregationDictionary()
            };

            foreach (var (child, aggregation) in childAggregations)
            {
                var nestedFilter = child.GetNestedFilter();
                if (nestedFilter is not null)
                {
                    var filteredAgg = new FilterAggregation($"filtered_{((IAggregation)aggregation).Name}")
                    {
                        Filter = nestedFilter,
                        Aggregations = new AggregationDictionary
                        {
                            [((IAggregation)aggregation).Name] = (AggregationContainer)aggregation
                        }
                    };
                    innermostAgg.Aggregations[((IAggregation)filteredAgg).Name] = (AggregationContainer)filteredAgg;
                }
                else
                {
                    innermostAgg.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
                }

                string bucketPrefix = BuildHierarchicalBucketPathPrefix(nestedPath, context);
                if (nestedFilter is not null)
                    AddTermsOrder(termsAggregation, child, aggregation, $"{bucketPrefix}filtered_{((IAggregation)aggregation).Name}>");
                else
                    AddTermsOrder(termsAggregation, child, aggregation, bucketPrefix.Length > 0 ? bucketPrefix : null);
            }

            // Build hierarchical nested aggregation chain if needed
            var topLevelAgg = BuildHierarchicalNestedAgg(nestedPath, innermostAgg, context);

            if (container is BucketAggregationBase bucketContainer)
            {
                bucketContainer.Aggregations ??= new AggregationDictionary();
                bucketContainer.Aggregations[((IAggregation)topLevelAgg).Name] = (AggregationContainer)topLevelAgg;
            }
        }

        if (node.Parent is null)
            node.SetAggregation(container);
    }

    private static void AddAggregation(AggregationBase container, ITermsAggregation? termsAggregation, IFieldQueryNode child, AggregationBase aggregation)
    {
        if (container is BucketAggregationBase bucketContainer)
        {
            bucketContainer.Aggregations ??= new AggregationDictionary();
            bucketContainer.Aggregations[((IAggregation)aggregation).Name] = (AggregationContainer)aggregation;
        }

        AddTermsOrder(termsAggregation, child, aggregation);
    }

    private static void AddTermsOrder(ITermsAggregation? termsAggregation, IFieldQueryNode child, AggregationBase aggregation, string? bucketPathPrefix = null)
    {
        if (termsAggregation is null || child.Prefix is not "-" and not "+")
            return;

        string aggName = ((IAggregation)aggregation).Name;
        string key = bucketPathPrefix is not null ? $"{bucketPathPrefix}{aggName}" : aggName;

        termsAggregation.Order ??= new List<TermsOrder>();
        termsAggregation.Order.Add(new TermsOrder
        {
            Key = key,
            Order = child.Prefix is "-" ? SortOrder.Descending : SortOrder.Ascending
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
        AggregationBase? container = null;
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
            container = new ChildrenAggregation(null, null);
            currentNode!.SetAggregation(container);
        }

        return container;
    }

    private static AggregationBase BuildHierarchicalNestedAgg(
        string deepestPath, NestedAggregation innermostAgg, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            return innermostAgg;

        var pathSegments = deepestPath.Split('.');
        var nestedPaths = new List<string>();
        string current = "";
        for (int i = 0; i < pathSegments.Length; i++)
        {
            current = i == 0 ? pathSegments[i] : $"{current}.{pathSegments[i]}";
            if (elasticContext.MappingResolver.IsNestedPropertyType(current))
                nestedPaths.Add(current);
        }

        if (nestedPaths.Count <= 1)
            return innermostAgg;

        // Build from outermost to innermost, placing the innermost aggregation at the deepest level
        AggregationBase result = innermostAgg;
        for (int i = nestedPaths.Count - 2; i >= 0; i--)
        {
            var wrapper = new NestedAggregation($"nested_{nestedPaths[i]}")
            {
                Path = nestedPaths[i],
                Aggregations = new AggregationDictionary
                {
                    [((IAggregation)result).Name] = (AggregationContainer)result
                }
            };
            result = wrapper;
        }

        return result;
    }

    private static string BuildHierarchicalBucketPathPrefix(
        string deepestPath, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            return $"nested_{deepestPath}>";

        var pathSegments = deepestPath.Split('.');
        var nestedPaths = new List<string>();
        string current = "";
        for (int i = 0; i < pathSegments.Length; i++)
        {
            current = i == 0 ? pathSegments[i] : $"{current}.{pathSegments[i]}";
            if (elasticContext.MappingResolver.IsNestedPropertyType(current))
                nestedPaths.Add(current);
        }

        if (nestedPaths.Count <= 1)
            return $"nested_{deepestPath}>";

        var parts = nestedPaths.Select(p => $"nested_{p}>").ToList();
        return string.Join("", parts);
    }
}
