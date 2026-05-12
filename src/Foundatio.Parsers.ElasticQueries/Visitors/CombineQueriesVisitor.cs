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

        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        QueryBase? query = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).ConfigureAwait(false);
        QueryBase? container = query;
        var nested = query as NestedQuery;

        // Reset container for non-root nested groups so children combine into a fresh
        // query that becomes the NestedQuery's inner query at the end of this method.
        if (nested is not null && node.Parent is not null)
            container = null;

        var op = GetEffectiveOperator(node, elasticContext);

        var nestedQueries = new Dictionary<string, List<(IFieldQueryNode Node, QueryContainer InnerQuery)>>();
        var regularQueries = new List<(IFieldQueryNode Node, QueryBase Query)>();

        foreach (var child in node.Children.OfType<IFieldQueryNode>())
        {
            var childQuery = await child.GetQueryAsync(() => child.GetDefaultQueryAsync(context)).ConfigureAwait(false);
            if (childQuery is null)
                continue;

            // Explicit nested groups (e.g., nested:(...)) were already combined by a recursive
            // visit, so treat them as atomic queries rather than coalescing their inner queries.
            bool isExplicitNestedGroup = child is GroupNode groupChild && groupChild.GetNestedPath() is not null;

            if (childQuery is NestedQuery childNested && childNested.Path is not null && !isExplicitNestedGroup)
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

        foreach (var (child, childQuery) in regularQueries)
        {
            QueryBase q = childQuery;
            if (child.IsExcluded())
                q = !q;

            container = Combine(container, q, op);
        }

        // Build nested queries per path, then nest child paths inside parent paths
        var builtNestedQueries = new Dictionary<string, QueryBase>();
        var negatedNestedQueries = new Dictionary<string, List<QueryBase>>();

        foreach (var (path, pathQueries) in nestedQueries)
        {
            QueryContainer? combinedInner = null;
            foreach (var (child, innerQuery) in pathQueries)
            {
                QueryContainer q = innerQuery;

                var childFilter = child.GetNestedFilter();
                if (childFilter is not null)
                    q = new BoolQuery { Must = [q], Filter = [childFilter] };

                if (child.IsExcluded())
                {
                    QueryBase negatedNested = new NestedQuery { Path = path, Query = q };
                    if (!negatedNestedQueries.ContainsKey(path))
                        negatedNestedQueries[path] = new List<QueryBase>();
                    negatedNestedQueries[path].Add(negatedNested);
                    continue;
                }

                combinedInner = Combine(combinedInner, q, op);
            }

            if (combinedInner is not null)
                builtNestedQueries[path] = new NestedQuery { Path = path, Query = combinedInner };
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

            // Ensure parent nested query exists as a container for child queries
            if (!builtNestedQueries.TryGetValue(parentPath, out var parentEntry))
            {
                parentEntry = new NestedQuery { Path = parentPath };
                builtNestedQueries[parentPath] = parentEntry;
            }

            var parentNested = (NestedQuery)parentEntry;

            // Fold positive child into parent
            if (builtNestedQueries.TryGetValue(childPath, out var childPositive))
            {
                parentNested.Query = parentNested.Query is not null
                    ? Combine(parentNested.Query, (QueryContainer)childPositive, op)
                    : childPositive;
                builtNestedQueries.Remove(childPath);
            }

            // Fold negated children into parent
            if (negatedNestedQueries.TryGetValue(childPath, out var childNegated))
            {
                foreach (var negated in childNegated)
                {
                    parentNested.Query = parentNested.Query is not null
                        ? Combine(parentNested.Query, !negated, op)
                        : !negated;
                }
                negatedNestedQueries.Remove(childPath);
            }
        }

        foreach (var (_, nestedQuery) in builtNestedQueries)
        {
            container = Combine(container, nestedQuery, op);
        }

        // Any remaining negated nested queries that have no parent get combined at top level
        foreach (var (_, negatedList) in negatedNestedQueries)
        {
            foreach (var negated in negatedList)
                container = Combine(container, !negated, op);
        }

        if (nested is not null)
        {
            if (container is null)
            {
                node.RemoveQuery();
                return;
            }

            var nestedFilter = node.GetNestedFilter();
            if (nestedFilter is not null)
            {
                QueryContainer inner = new BoolQuery { Must = [container], Filter = [nestedFilter] };
                nested.Query = inner;
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

    private static QueryBase? Combine(QueryBase? left, QueryBase right, GroupOperator op)
    {
        if (left is null)
            return right;

        if (op is GroupOperator.And)
            return left & right;

        if (op is GroupOperator.Or)
            return left | right;

        return left;
    }

    private static QueryContainer? Combine(QueryContainer? left, QueryContainer right, GroupOperator op)
    {
        if (left is null)
            return right;

        if (op is GroupOperator.And)
            return left & right;

        if (op is GroupOperator.Or)
            return left | right;

        return left;
    }
}
