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

        public static PlainFilter GetFilterOrDefault(this IQueryNode node) {
            var f = node.GetFilter();
            if (f != null)
                return f;

            return node.GetDefaultFilter();
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
        public static PlainFilter GetFilter(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(FilterKey, out value))
                return null;

            return value as PlainFilter;
        }

        public static void SetFilter(this IQueryNode node, PlainFilter filter) {
            node.Data[FilterKey] = filter;
        }

        public static void RemoveFilter(this IQueryNode node) {
            if (node.Data.ContainsKey(FilterKey))
                node.Data.Remove(FilterKey);
        }

        private const string DefaultFilterKey = "@DefaultFilter";
        public static PlainFilter GetDefaultFilter(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(DefaultFilterKey, out value))
                return null;

            return value as PlainFilter;
        }

        public static void SetDefaultFilter(this IQueryNode node, PlainFilter filter) {
            node.Data[DefaultFilterKey] = filter;
        }

        public static void RemoveDefaultFilter(this IQueryNode node) {
            if (node.Data.ContainsKey(DefaultFilterKey))
                node.Data.Remove(DefaultFilterKey);
        }

        public static PlainQuery GetQueryOrDefault(this IQueryNode node) {
            var q = node.GetQuery();
            if (q != null)
                return q;

            return node.GetDefaultQuery();
        }

        private const string QueryKey = "@Query";
        public static PlainQuery GetQuery(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(QueryKey, out value))
                return null;

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

        private const string DefaultQueryKey = "@DefaultQuery";
        public static PlainQuery GetDefaultQuery(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(DefaultQueryKey, out value))
                return null;

            return value as PlainQuery;
        }

        public static void SetDefaultQuery(this IQueryNode node, PlainQuery query) {
            node.Data[DefaultQueryKey] = query;
        }

        public static void RemoveDefaultQuery(this IQueryNode node) {
            if (node.Data.ContainsKey(DefaultQueryKey))
                node.Data.Remove(DefaultQueryKey);
        }
    }
}
