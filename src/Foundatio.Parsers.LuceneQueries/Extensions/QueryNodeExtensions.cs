using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryNodeExtensions {
        public static string[] GetNameParts(this IFieldQueryNode node) {
            var nameParts = new List<string>();
            var current = node;
            while (current != null) {
                string field = current.Field;
                if (field != null)
                    nameParts.AddRange(field.Split('.').Reverse());

                current = current.Parent as IFieldQueryNode;
            }

            nameParts.Reverse();

            return nameParts.ToArray();
        }

        public static string GetFullName(this IFieldQueryNode node) {
            var parts = node.GetNameParts();
            if (parts.Length == 0)
                return null;

            return String.Join(".", parts);
        }

        public static bool IsNodeNegated(this IFieldQueryNode node) {
            return (node.IsNegated.HasValue && node.IsNegated.Value == true) || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-");
        }

        public static bool IsNodeOrGroupedParentNegated(this IFieldQueryNode node) {
            if (!String.IsNullOrEmpty(node.Prefix))
                return node.IsNodeNegated();

            IQueryNode current = node;
            do {
                var groupNode = current as GroupNode;
                if (groupNode != null && !groupNode.HasParens) {
                    current = current.Parent;
                    continue;
                }

                var fieldQueryNode = current as IFieldQueryNode;
                if (fieldQueryNode != null
                    && ((fieldQueryNode.IsNegated.HasValue && fieldQueryNode.IsNegated.Value == true)
                        || (!String.IsNullOrEmpty(fieldQueryNode.Prefix) && fieldQueryNode.Prefix == "-")))
                    return true;

                current = current.Parent;
            } while (current.Parent != null);

            return false;
        }

        public static IQueryNode GetScopedNode(this IFieldQueryNode node) {
            IQueryNode current = node;
            do {
                var groupNode = current as GroupNode;
                
                if (groupNode != null && groupNode.HasParens) return groupNode;

                current = current.Parent;
            } while (current.Parent != null);

            return current;
        }

        private const string ResolvedFieldKey = "@ResolvedField";
        public static string GetResolvedField(this IFieldQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(ResolvedFieldKey, out value))
                return node.GetFullName();

            return value as string;
        }

        public static void SetResolvedField(this IFieldQueryNode node, string field) {
            node.Data[ResolvedFieldKey] = field;
        }

        private const string OperationTypeKey = "@OperationType";
        public static string GetOperationType(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(OperationTypeKey, out value))
                return null;

            return (string)value;
        }

        public static void SetOperationType(this IQueryNode node, string aggregationType) {
            node.Data[OperationTypeKey] = aggregationType;
        }

        public static void RemoveOperationType(this IQueryNode node) {
            if (node.Data.ContainsKey(OperationTypeKey))
                node.Data.Remove(OperationTypeKey);
        }

        private const string QueryTypeKey = "@QueryType";
        public static string GetQueryType(this IQueryNode node) {
            node = node.GetRootNode();

            object value = null;
            if (!node.Data.TryGetValue(QueryTypeKey, out value))
                return QueryType.Unknown;

            return value as string;
        }

        public static void SetQueryType(this IQueryNode node, string queryType) {
            node = node.GetRootNode();
            node.Data[QueryTypeKey] = queryType;
        }

        public static void RemoveQueryType(this IQueryNode node) {
            node = node.GetRootNode();
            if (node.Data.ContainsKey(QueryTypeKey))
                node.Data.Remove(QueryTypeKey);
        }

        public static IQueryNode GetRootNode(this IQueryNode node) {
            IQueryNode current = node;
            while (current.Parent != null)
                current = current.Parent;

            return current;
        }
    }
}