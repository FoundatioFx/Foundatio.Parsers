using System;
using System.Collections.Generic;
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
        public static NamedAggregationContainer GetAggregation(this IQueryNode node, Func<NamedAggregationContainer> getDefaultValue = null) {
            object value = null;
            if (!node.Data.TryGetValue(AggregationKey, out value))
                return getDefaultValue?.Invoke();

            return value as NamedAggregationContainer;
        }

        public static void SetAggregation(this IQueryNode node, string name, IAggregationContainer container) {
            node.Data[AggregationKey] = new NamedAggregationContainer(name, container);
        }

        public static void SetAggregation(this IQueryNode node, NamedAggregationContainer namedContainer) {
            node.Data[AggregationKey] = namedContainer;
        }

        public static void RemoveAggregation(this IQueryNode node) {
            if (node.Data.ContainsKey(AggregationKey))
                node.Data.Remove(AggregationKey);
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
    }
}
