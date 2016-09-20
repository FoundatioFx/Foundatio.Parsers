using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class QueryNodeExtensions {
        public static void InvalidateFilter(this IQueryNode node) {
            IQueryNode current = node;
            while (current != null) {
                if (current is GroupNode)
                    current.RemoveFilter();

                current = current.Parent;
            }
        }

        public static void InvalidateQuery(this IQueryNode node) {
            IQueryNode current = node;
            while (current != null) {
                if (current is GroupNode)
                    current.RemoveQuery();

                current = current.Parent;
            }
        }

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

        private const string FILTER_KEY = "@filter";
        public static FilterContainer GetFilter(this IQueryNode node) {
            object value = null;
            if (!node.Meta.TryGetValue(FILTER_KEY, out value))
                return null;

            return value as FilterContainer;
        }

        public static void SetFilter(this IQueryNode node, FilterContainer container) {
            node.Meta[FILTER_KEY] = container;
        }

        public static void RemoveFilter(this IQueryNode node) {
            if (node.Meta.ContainsKey(FILTER_KEY))
                node.Meta.Remove(FILTER_KEY);
        }

        private const string QUERY_KEY = "@query";
        public static QueryContainer GetQuery(this IQueryNode node) {
            object value = null;
            if (!node.Meta.TryGetValue(QUERY_KEY, out value))
                return null;

            return value as QueryContainer;
        }

        public static void SetQuery(this IQueryNode node, QueryContainer container) {
            node.Meta[QUERY_KEY] = container;
        }

        public static void RemoveQuery(this IQueryNode node) {
            if (node.Meta.ContainsKey(QUERY_KEY))
                node.Meta.Remove(QUERY_KEY);
        }
    }
}
