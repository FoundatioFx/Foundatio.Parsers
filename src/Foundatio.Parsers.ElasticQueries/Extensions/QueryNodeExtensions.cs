using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class QueryNodeExtensions
{
    private const string QueryKey = "@Query";
    public static Task<Query> GetQueryAsync(this IQueryNode node, Func<Task<Query>> getDefaultValue = null)
    {
        if (!node.Data.TryGetValue(QueryKey, out object value))
        {
            if (getDefaultValue == null)
                return Task.FromResult<Query>(null);

            return getDefaultValue?.Invoke();
        }

        return Task.FromResult(value as Query);
    }

    public static void SetQuery(this IQueryNode node, Query container)
    {
        node.Data[QueryKey] = container;
    }

    public static void RemoveQuery(this IQueryNode node)
    {
        if (node.Data.ContainsKey(QueryKey))
            node.Data.Remove(QueryKey);
    }

    private const string SourceFilterKey = "@SourceFilter";
    public static SourceFilter GetSourceFilter(this IQueryNode node, Func<SourceFilter> getDefaultValue = null)
    {
        if (!node.Data.TryGetValue(SourceFilterKey, out object value))
            return getDefaultValue?.Invoke();

        return value as SourceFilter;
    }

    private const string AggregationKey = "@Aggregation";
    public static Task<AggregationMap> GetAggregationAsync(this IQueryNode node, Func<Task<AggregationMap>> getDefaultValue = null)
    {
        if (!node.Data.TryGetValue(AggregationKey, out object value))
        {
            if (getDefaultValue == null)
                return Task.FromResult<AggregationMap>(null);

            return getDefaultValue?.Invoke();
        }

        return Task.FromResult(value as AggregationMap);
    }

    public static void SetAggregation(this IQueryNode node, AggregationMap aggregation)
    {
        node.Data[AggregationKey] = aggregation;
    }

    public static void RemoveAggregation(this IQueryNode node)
    {
        node.Data.Remove(AggregationKey);
    }

    private const string SortKey = "@Sort";
    public static SortOptions GetSort(this IQueryNode node, Func<SortOptions> getDefaultValue = null)
    {
        if (!node.Data.TryGetValue(SortKey, out object value))
            return getDefaultValue?.Invoke();

        return value as SortOptions;
    }

    public static void SetSort(this IQueryNode node, SortOptions sort)
    {
        node.Data[SortKey] = sort;
    }

    public static void RemoveSort(this IQueryNode node)
    {
        node.Data.Remove(SortKey);
    }

    private const string NestedPathKey = "@NestedPath";
    public static string GetNestedPath(this IQueryNode node)
    {
        if (!node.Data.TryGetValue(NestedPathKey, out object value))
            return null;

        return value as string;
    }

    public static void SetNestedPath(this IQueryNode node, string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        node.Data[NestedPathKey] = path;
    }

    public static void RemoveNestedPath(this IQueryNode node)
    {
        node.Data.Remove(NestedPathKey);
    }

    private const string NestedFilterKey = "@NestedFilter";
    public static Query GetNestedFilter(this IQueryNode node)
    {
        if (!node.Data.TryGetValue(NestedFilterKey, out object value))
            return null;

        return value as Query;
    }

    public static void SetNestedFilter(this IQueryNode node, Query filter)
    {
        node.Data[NestedFilterKey] = filter;
    }

    public static void RemoveNestedFilter(this IQueryNode node)
    {
        node.Data.Remove(NestedFilterKey);
    }
}
