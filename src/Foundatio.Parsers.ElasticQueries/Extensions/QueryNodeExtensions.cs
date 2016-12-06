using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class QueryNodeExtensions {
        public static Operator GetOperator(this IQueryNode node, Operator defaultOperator) {
            var groupNode = node as GroupNode;
            if (groupNode == null)
                return defaultOperator;

            switch (groupNode.Operator) {
                case GroupOperator.And:
                    return Operator.And;
                case GroupOperator.Or:
                    return Operator.Or;
                default:
                    return defaultOperator;
            }
        }

        private const string QueryKey = "@Query";
        public static QueryBase GetQuery(this IQueryNode node, Func<QueryBase> getDefaultValue = null) {
            object value = null;
            if (!node.Data.TryGetValue(QueryKey, out value))
                return getDefaultValue?.Invoke();

            return value as QueryBase;
        }

        public static void SetQuery(this IQueryNode node, QueryBase container) {
            node.Data[QueryKey] = container;
        }

        public static void RemoveQuery(this IQueryNode node) {
            if (node.Data.ContainsKey(QueryKey))
                node.Data.Remove(QueryKey);
        }

        private const string AggregationTypeKey = "@AggregationType";
        public static string GetAggregationType(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(AggregationTypeKey, out value))
                return null;

            return (string)value;
        }

        public static void SetAggregationType(this IQueryNode node, string aggregationType) {
            node.Data[AggregationTypeKey] = aggregationType;
        }

        public static void RemoveAggregationType(this IQueryNode node) {
            if (node.Data.ContainsKey(AggregationTypeKey))
                node.Data.Remove(AggregationTypeKey);
        }

        private const string AggregationKey = "@Aggregation";
        public static AggregationBase GetAggregation(this IQueryNode node, Func<AggregationBase> getDefaultValue = null) {
            object value = null;
            if (!node.Data.TryGetValue(AggregationKey, out value))
                return getDefaultValue?.Invoke();

            return value as AggregationBase;
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
            object value = null;
            if (!node.Data.TryGetValue(SortKey, out value))
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
