using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
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

        public static QueryBase GetQueryOrDefault(this IQueryNode node) {
            var q = node.GetQuery();
            if (q != null)
                return q;

            return node.GetDefaultQuery();
        }

        private const string QueryKey = "@query";
        public static QueryBase GetQuery(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(QueryKey, out value))
                return null;

            return value as QueryBase;
        }

        public static void SetQuery(this IQueryNode node, QueryBase container) {
            node.Data[QueryKey] = container;
        }

        public static void RemoveQuery(this IQueryNode node) {
            if (node.Data.ContainsKey(QueryKey))
                node.Data.Remove(QueryKey);
        }

        private const string DefaultQueryKey = "@default_query";
        public static QueryBase GetDefaultQuery(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(DefaultQueryKey, out value))
                return null;

            return value as QueryBase;
        }

        public static void SetDefaultQuery(this IQueryNode node, QueryBase container) {
            node.Data[DefaultQueryKey] = container;
        }

        public static void RemoveDefaultQuery(this IQueryNode node) {
            if (node.Data.ContainsKey(DefaultQueryKey))
                node.Data.Remove(DefaultQueryKey);
        }

        private const string DEFAULT_OPERATOR_KEY = "@default_operator";
        public static Operator GetDefaultOperator(this IQueryVisitorContext context) {
            object value = null;
            if (!context.Data.TryGetValue(DEFAULT_OPERATOR_KEY, out value))
                return Operator.And;

            if (value == null)
                return Operator.And;

            return (Operator)value;
        }

        public static void SetDefaultOperator(this IQueryVisitorContext context, Operator defaultOperator) {
            context.Data[DEFAULT_OPERATOR_KEY] = defaultOperator;
        }

        public static void RemoveDefaultOperator(this IQueryVisitorContext context) {
            if (context.Data.ContainsKey(DEFAULT_OPERATOR_KEY))
                context.Data.Remove(DEFAULT_OPERATOR_KEY);
        }
    }
}
