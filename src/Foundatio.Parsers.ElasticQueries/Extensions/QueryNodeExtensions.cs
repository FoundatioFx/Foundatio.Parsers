using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
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

        private const string FilterContainerKey = "@FilterContainer";
        public static FilterContainer GetFilterContainer(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(FilterContainerKey, out value))
                return null;

            return value as FilterContainer;
        }

        public static void SetFilterContainer(this IQueryNode node, FilterContainer container) {
            node.Data[FilterContainerKey] = container;
        }

        public static void RemoveFilterContainer(this IQueryNode node) {
            if (node.Data.ContainsKey(FilterContainerKey))
                node.Data.Remove(FilterContainerKey);
        }

        private const string FilterKey = "@Filter";
        public static PlainFilter GetFilter(this IQueryNode node, Func<PlainFilter> getDefaultValue = null) {
            object value = null;
            if (!node.Data.TryGetValue(FilterKey, out value))
                return getDefaultValue?.Invoke();

            return value as PlainFilter;
        }

        public static void SetFilter(this IQueryNode node, PlainFilter filter) {
            node.Data[FilterKey] = filter;
        }

        public static void RemoveFilter(this IQueryNode node) {
            if (node.Data.ContainsKey(FilterKey))
                node.Data.Remove(FilterKey);
        }

        private const string QueryKey = "@Query";
        public static PlainQuery GetQuery(this IQueryNode node, Func<PlainQuery> getDefaultValue = null) {
            object value = null;
            if (!node.Data.TryGetValue(QueryKey, out value))
                return getDefaultValue?.Invoke();

            return value as PlainQuery;
        }

        public static void SetQuery(this IQueryNode node, PlainQuery container) {
            node.Data[QueryKey] = container;
        }

        public static void RemoveQuery(this IQueryNode node) {
            if (node.Data.ContainsKey(QueryKey))
                node.Data.Remove(QueryKey);
        }

        private const string QueryContainerKey = "@QueryContainer";
        public static QueryContainer GetQueryContainer(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(QueryContainerKey, out value))
                return null;

            return value as QueryContainer;
        }

        public static void SetQueryContainer(this IQueryNode node, QueryContainer container) {
            node.Data[QueryContainerKey] = container;
        }

        public static void RemoveQueryContainer(this IQueryNode node) {
            if (node.Data.ContainsKey(QueryContainerKey))
                node.Data.Remove(QueryContainerKey);
        }

        private const string AggregationTypeKey = "@AggregationType";
        public static AggregationType GetAggregationType(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(AggregationTypeKey, out value))
                return AggregationType.None;

            return (AggregationType)value;
        }

        public static void SetAggregationType(this IQueryNode node, AggregationType container) {
            node.Data[AggregationTypeKey] = container;
        }

        public static void RemoveAggregationType(this IQueryNode node) {
            if (node.Data.ContainsKey(AggregationTypeKey))
                node.Data.Remove(AggregationTypeKey);
        }

        private const string AggregationContainerKey = "@AggregationContainer";
        public static AggregationContainer GetAggregationContainer(this IQueryNode node, Func<AggregationContainer> getDefaultValue = null) {
            object value = null;
            if (!node.Data.TryGetValue(AggregationContainerKey, out value))
                return getDefaultValue?.Invoke();

            return value as AggregationContainer;
        }

        public static void SetAggregationContainer(this IQueryNode node, AggregationContainer container) {
            node.Data[AggregationContainerKey] = container;
        }

        public static void RemoveAggregationContainer(this IQueryNode node) {
            if (node.Data.ContainsKey(AggregationContainerKey))
                node.Data.Remove(AggregationContainerKey);
        }
    }
}
