using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class QueryNodeExtensions {
        private const string QueryKey = "@Query";
        public static Task<QueryBase> GetQueryAsync(this IQueryNode node, Func<Task<QueryBase>> getDefaultValue = null) {
            if (!node.Data.TryGetValue(QueryKey, out object value)) {
                if (getDefaultValue == null)
                    return Task.FromResult<QueryBase>(null);

                return getDefaultValue?.Invoke();
            }

            return Task.FromResult(value as QueryBase);
        }

        public static void SetQuery(this IQueryNode node, QueryBase container) {
            node.Data[QueryKey] = container;
        }

        public static void RemoveQuery(this IQueryNode node) {
            if (node.Data.ContainsKey(QueryKey))
                node.Data.Remove(QueryKey);
        }

        private const string SourceFilterKey = "@SourceFilter";
        public static SourceFilter GetSourceFilter(this IQueryNode node, Func<SourceFilter> getDefaultValue = null) {
            if (!node.Data.TryGetValue(SourceFilterKey, out object value))
                return getDefaultValue?.Invoke();

            return value as SourceFilter;
        }

        private const string AggregationKey = "@Aggregation";
        public static Task<AggregationBase> GetAggregationAsync(this IQueryNode node, Func<Task<AggregationBase>> getDefaultValue = null) {
            if (!node.Data.TryGetValue(AggregationKey, out object value)) {
                if (getDefaultValue == null)
                    return Task.FromResult<AggregationBase>(null);

                return getDefaultValue?.Invoke();
            }

            return Task.FromResult(value as AggregationBase);
        }

        public static void SetAggregation(this IQueryNode node, AggregationBase aggregation) {
            node.Data[AggregationKey] = aggregation;
        }

        public static void RemoveAggregation(this IQueryNode node) {
            if (node.Data.ContainsKey(AggregationKey))
                node.Data.Remove(AggregationKey);
        }

        private const string SortKey = "@Sort";
        public static IFieldSort GetSort(this IQueryNode node, Func<IFieldSort> getDefaultValue = null) {
            if (!node.Data.TryGetValue(SortKey, out object value))
                return getDefaultValue?.Invoke();

            return value as IFieldSort;
        }

        public static void SetSort(this IQueryNode node, IFieldSort sort) {
            node.Data[SortKey] = sort;
        }

        public static void RemoveSort(this IQueryNode node) {
            if (node.Data.ContainsKey(SortKey))
                node.Data.Remove(SortKey);
        }
    }
}
