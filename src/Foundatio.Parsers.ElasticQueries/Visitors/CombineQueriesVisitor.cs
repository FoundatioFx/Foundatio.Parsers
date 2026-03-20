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

        QueryBase query = await node.GetQueryAsync(() => node.GetDefaultQueryAsync(context)).ConfigureAwait(false);
        QueryBase container = query;
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

        foreach (var (path, pathQueries) in nestedQueries)
        {
            QueryContainer combinedInner = null;
            QueryContainer nestedFilter = null;
            foreach (var (child, innerQuery) in pathQueries)
            {
                QueryContainer q = innerQuery;
                if (child.IsExcluded())
                    q = !q;

                combinedInner = Combine(combinedInner, q, op);
                nestedFilter ??= child.GetNestedFilter();
            }

            if (nestedFilter is not null)
                combinedInner &= nestedFilter;

            QueryBase combinedNested = new NestedQuery { Path = path, Query = combinedInner };
            container = Combine(container, combinedNested, op);
        }

        if (nested is not null)
        {
            var nestedFilter = node.GetNestedFilter();
            if (nestedFilter is not null)
            {
                QueryContainer inner = container;
                inner &= nestedFilter;
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
        if (op == GroupOperator.Or && node.IsRequired())
            op = GroupOperator.And;
        return op;
    }

    private static QueryBase Combine(QueryBase left, QueryBase right, GroupOperator op)
    {
        if (op == GroupOperator.And)
            return left & right;

        if (op == GroupOperator.Or)
            return left | right;

        return left;
    }

    private static QueryContainer Combine(QueryContainer left, QueryContainer right, GroupOperator op)
    {
        if (op == GroupOperator.And)
            return left & right;

        if (op == GroupOperator.Or)
            return left | right;

        return left;
    }
}
