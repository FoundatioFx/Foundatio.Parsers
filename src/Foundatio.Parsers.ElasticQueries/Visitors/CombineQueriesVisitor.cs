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
        await base.VisitAsync(node, context).AnyContext();

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        Query? query = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).AnyContext();
        Query? container = query;
        var nested = query?.Nested;

        // Reset container for non-root nested groups so children combine into a fresh
        // query that becomes the NestedQuery's inner query at the end of this method.
        if (nested is not null && node.Parent is not null)
            container = null;

        var op = GetEffectiveOperator(node, elasticContext);

        var nestedQueries = new Dictionary<string, List<(IFieldQueryNode Node, Query InnerQuery)>>();
        var regularQueries = new List<(IFieldQueryNode Node, Query Query)>();

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var childQuery = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).AnyContext();
            if (childQuery is null)
                continue;

            // Explicit nested groups (e.g., nested:(...)) were already combined by a recursive
            // visit, so treat them as atomic queries rather than coalescing their inner queries.
            bool isExplicitNestedGroup = child is GroupNode groupChild && groupChild.GetNestedPath() is not null;

            var childNested = childQuery.Nested;
            if (childNested is not null && childNested.Path is not null && !isExplicitNestedGroup)
            {
                string pathKey = childNested.Path.ToString();
                if (!nestedQueries.ContainsKey(pathKey))
                    nestedQueries[pathKey] = [];
                nestedQueries[pathKey].Add((child, childNested.Query));
            }
            else
            {
                regularQueries.Add((child, childQuery));
            }
        }

        bool useScoring = elasticContext.UseScoring;

        foreach (var (child, childQuery) in regularQueries)
        {
            Query? q = childQuery;
            if (child.IsExcluded())
                q = !q;

            container = Combine(container, q, op, useScoring);
        }

        // Build nested queries per path, then nest child paths inside parent paths
        var builtNestedQueries = new Dictionary<string, Query>();
        var negatedNestedQueries = new Dictionary<string, List<Query>>();

        foreach (var (path, pathQueries) in nestedQueries)
        {
            Query? combinedInner = null;
            foreach (var (child, innerQuery) in pathQueries)
            {
                Query q = innerQuery;

                var childFilter = child.GetNestedFilter();
                if (childFilter is not null)
                    q = ApplyNestedFilter(q, childFilter);

                if (child.IsExcluded())
                {
                    if (!negatedNestedQueries.ContainsKey(path))
                        negatedNestedQueries[path] = new List<Query>();
                    negatedNestedQueries[path].Add(new NestedQuery(path, q));
                    continue;
                }

                combinedInner = Combine(combinedInner, q, op, useScoring);
            }

            if (combinedInner is not null)
                builtNestedQueries[path] = new NestedQuery(path, combinedInner);
        }

        // Nest child paths inside their parent paths (deepest first).
        // Both positive and negated child nested queries are folded into parent nested queries.
        // Include shared ancestor paths so sibling children (e.g., parent.childA + parent.childB)
        // are correlated within the same parent nested query.
        var originalPaths = builtNestedQueries.Keys.Union(negatedNestedQueries.Keys).Distinct().ToList();
        var allPaths = new HashSet<string>(originalPaths);

        var ancestorUseCounts = new Dictionary<string, int>();
        foreach (var path in originalPaths)
        {
            var chain = NestedPathResolver.GetNestedPathChain(path, elasticContext.MappingResolver);
            foreach (var ancestor in chain.Take(chain.Count - 1))
                ancestorUseCounts[ancestor] = ancestorUseCounts.GetValueOrDefault(ancestor) + 1;
        }

        foreach (var (ancestor, count) in ancestorUseCounts)
        {
            if (count > 1 || originalPaths.Contains(ancestor))
                allPaths.Add(ancestor);
        }

        var sortedPaths = allPaths.OrderByDescending(p => p.Length).ToList();
        foreach (string childPath in sortedPaths)
        {
            string? parentPath = sortedPaths.FirstOrDefault(p =>
                p.Length < childPath.Length && childPath.StartsWith(p + "."));

            if (parentPath is null)
                continue;

            // Collect child queries to fold into the parent nested query's inner query.
            Query? childInner = null;

            if (builtNestedQueries.TryGetValue(childPath, out var childPositive))
            {
                childInner = Combine(childInner, childPositive, op, useScoring);
                builtNestedQueries.Remove(childPath);
            }

            if (negatedNestedQueries.TryGetValue(childPath, out var childNegated))
            {
                foreach (var negated in childNegated)
                    childInner = Combine(childInner, !negated, op, useScoring);

                negatedNestedQueries.Remove(childPath);
            }

            if (childInner is null)
                continue;

            // Get or create the parent nested query, then fold child into its inner query.
            if (builtNestedQueries.TryGetValue(parentPath, out var existingParent) && existingParent.Nested is { } parentNested)
            {
                parentNested.Query = Combine(parentNested.Query, childInner, op, useScoring)!;
            }
            else
            {
                builtNestedQueries[parentPath] = new NestedQuery(parentPath, childInner);
            }
        }

        foreach (var (_, nestedQuery) in builtNestedQueries)
        {
            container = Combine(container, nestedQuery, op, useScoring);
        }

        // Any remaining negated nested queries that have no parent get combined at top level
        foreach (var (_, negatedList) in negatedNestedQueries)
        {
            foreach (var negated in negatedList)
                container = Combine(container, !negated, op, useScoring);
        }

        // If we have OR clauses and the container is a BoolQuery with only should clauses,
        // set minimum_should_match = 1 so at least one clause must match.
        if (op == GroupOperator.Or && container?.Bool is { } boolQuery)
        {
            bool isRootQuery = node.Parent is null;
            bool parentUsesAndOperator = node.Parent is GroupNode parentGroup && parentGroup.GetOperator(elasticContext) == GroupOperator.And;
            bool shouldSetMinimumShouldMatch = isRootQuery || (node.HasParens && parentUsesAndOperator);

            if (shouldSetMinimumShouldMatch)
            {
                bool hasOnlyShouldClauses = boolQuery.Should is { Count: > 0 }
                    && (boolQuery.Must is null or { Count: 0 })
                    && (boolQuery.Filter is null or { Count: 0 });

                if (hasOnlyShouldClauses && boolQuery.MinimumShouldMatch is null)
                {
                    boolQuery.MinimumShouldMatch = 1;
                }
            }
        }

        if (nested is not null)
        {
            if (container is null)
            {
                node.RemoveQuery();
                return;
            }

            var groupNestedFilter = node.GetNestedFilter();
            if (groupNestedFilter is not null)
            {
                nested.Query = ApplyNestedFilter(container, groupNestedFilter);
            }
            else
            {
                nested.Query = container;
            }

            node.SetQuery(nested);
        }
        else
        {
            node.SetQuery(container);
        }
    }

    private static GroupOperator GetEffectiveOperator(GroupNode node, IElasticQueryVisitorContext context)
    {
        var op = node.GetOperator(context);
        if (op is GroupOperator.Or && node.IsRequired())
            op = GroupOperator.And;
        return op;
    }

    private static Query? Combine(Query? left, Query? right, GroupOperator op, bool useScoring = true)
    {
        if (left is null)
            return right;
        if (right is null)
            return left;

        if (op == GroupOperator.And)
        {
            if (!useScoring)
            {
                var filters = new List<Query>();
                AddToFilterList(filters, left);
                AddToFilterList(filters, right);
                return new BoolQuery { Filter = filters };
            }

            return left & right;
        }

        if (op is GroupOperator.Or)
            return left | right;

        return left;
    }

    private static void AddToFilterList(List<Query> filters, Query? query)
    {
        if (query is null)
            return;

        if (query.IsFilterOnlyBoolQuery() && query.Bool is { Filter: { } existingFilters })
        {
            filters.AddRange(existingFilters);
        }
        else
        {
            filters.Add(query);
        }
    }

    private static Query ApplyNestedFilter(Query query, Query? filter)
    {
        if (filter is null)
            return query;

        if (query.Bool is { } existingBool
            && existingBool.Must is { Count: > 0 }
            && existingBool.Should is null or { Count: 0 }
            && existingBool.MustNot is null or { Count: 0 })
        {
            existingBool.Filter = existingBool.Filter is { Count: > 0 }
                ? [..existingBool.Filter, filter]
                : [filter];
            return query;
        }

        return new BoolQuery
        {
            Must = [query],
            Filter = [filter]
        };
    }
}
