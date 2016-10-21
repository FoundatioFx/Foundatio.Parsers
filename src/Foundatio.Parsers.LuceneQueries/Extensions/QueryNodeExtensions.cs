using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryNodeExtensions {
        public static string GetDefaultField(this IFieldQueryNode node, string defaultField) {
            var field = node.Field;

            if (!String.IsNullOrEmpty(field))
                return field;

            IQueryNode current = node;
            while (current != null) {
                var groupNode = node as GroupNode;
                if (groupNode != null && groupNode.HasParens && !String.IsNullOrEmpty(groupNode.Field))
                    return groupNode.Field;

                current = current.Parent;
            }

            return defaultField;
        }

        public static string[] GetNameParts(this IFieldQueryNode node) {
            var nameParts = new List<string>();
            var current = node;
            while (current != null) {
                var field = current.Field;
                if (field != null)
                    nameParts.AddRange(field.Split('.').Reverse());

                current = current.Parent as IFieldQueryNode;
            }

            nameParts.Reverse();

            return nameParts.ToArray();
        }

        public static IFieldQueryNode[] GetFieldNodes(this IFieldQueryNode node) {
            var nodes = new List<IFieldQueryNode>();
            var current = node;
            while (current != null) {
                nodes.Add(current);
                current = current.Parent as IFieldQueryNode;
            }

            nodes.Reverse();

            return nodes.ToArray();
        }

        public static string GetFullName(this IFieldQueryNode node) {
            var parts = node.GetNameParts();
            if (parts.Length == 0)
                return null;

            return String.Join(".", parts);
        }
        
        public static string GetParentFullName(this IFieldQueryNode node) {
            var nameParts = node.GetNameParts();
            return String.Join(".", nameParts.Take(nameParts.Length - 1));
        }

        public static bool IsNodeNegated(this IFieldQueryNode node) {
            return (node.IsNegated.HasValue && node.IsNegated.Value == true) || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-");
        }

        private const string AliasResolverKey = "@AliasResolver";
        public static AliasResolver GetAliasResolver(this IQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(AliasResolverKey, out value))
                return null;

            return value as AliasResolver;
        }

        public static void SetAliasResolver(this IQueryNode node, AliasResolver resolver) {
            node.Data[AliasResolverKey] = resolver;
        }

        private const string UnaliasedFieldKey = "@UnaliasedField";
        public static string GetUnaliasedField(this IFieldQueryNode node) {
            object value = null;
            if (!node.Data.TryGetValue(UnaliasedFieldKey, out value))
                return node.Field;

            return value as string;
        }

        public static void SetUnaliasedField(this IFieldQueryNode node, string field) {
            node.Data[UnaliasedFieldKey] = field;
        }
    }
}