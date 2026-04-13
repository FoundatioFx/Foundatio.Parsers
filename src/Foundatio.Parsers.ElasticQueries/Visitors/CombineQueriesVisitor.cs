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
            Query q = childQuery;
            if (child.IsExcluded())
                q = !q;

            container = Combine(container, q, op, useScoring);
        }

        foreach (var (path, pathQueries) in nestedQueries)
        {
            Query? combinedIncluded = null;
            Query? nestedFilter = null;
            foreach (var (child, innerQuery) in pathQueries)
            {
                if (child.IsExcluded())
                {
                    Query negatedNested = !(Query)(new NestedQuery(path, innerQuery));
                    container = Combine(container, negatedNested, op, useScoring);
                }
                else
                {
                    combinedIncluded = Combine(combinedIncluded, innerQuery, op, useScoring);
                }

                nestedFilter ??= child.GetNestedFilter();
            }

            if (combinedIncluded is not null)
            {
                if (nestedFilter is not null)
                    combinedIncluded = combinedIncluded & nestedFilter;

                Query combinedNested = new NestedQuery(path, combinedIncluded);
                container = Combine(container, combinedNested, op, useScoring);
            }
        }

        // If we have OR clauses and the container is a BoolQuery with only should clauses,
        // set minimum_should_match = 1 so at least one clause must match.
        // Apply on root queries OR parens groups within an AND context.
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
                node.SetQuery(null);
                return;
            }

            var groupNestedFilter = node.GetNestedFilter();
            if (groupNestedFilter is not null)
            {
                Query inner = container & groupNestedFilter;
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

    private static Query? Combine(Query? left, Query right, GroupOperator op, bool useScoring = true)
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

        if (query.IsFilterOnlyBoolQuery())
        {
            filters.AddRange(query.Bool!.Filter!);
        }
        else
        {
            filters.Add(query);
        }
    }
}
