using System;
using System.Collections.Generic;
using System.Linq;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Extensions {
    public static class QueryNodeExtensions {
        public static string GetDefaultField(this IQueryNode node, string defaultField) {
            var field = node.GetField();

            if (!String.IsNullOrEmpty(field))
                return field;

            var current = node;
            while (current != null) {
                var groupNode = node as GroupNode;
                if (groupNode != null && groupNode.HasParens && !String.IsNullOrEmpty(groupNode.Field))
                    return groupNode.Field;

                current = current.Parent;
            }

            return defaultField;
        }

        public static string[] GetNameParts(this IQueryNode node) {
            var nameParts = new List<string>();
            var current = node;
            while (current != null) {
                var field = current.GetField();
                if (field != null)
                    nameParts.AddRange(field.Split('.').Reverse());

                current = current.Parent;
            }

            nameParts.Reverse();

            return nameParts.ToArray();
        }

        public static string GetFullName(this IQueryNode node) {
            return String.Join(".", node.GetNameParts());
        }
        
        public static string GetParentFullName(this IQueryNode node) {
            var nameParts = node.GetNameParts();
            return String.Join(".", nameParts.Take(nameParts.Length - 1));
        }

        public static string GetField(this IQueryNode node) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                return groupNode.Field;

            var termNode = node as TermNode;
            if (termNode != null)
                return termNode.Field;

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                return termRangeNode.Field;

            var missingNode = node as MissingNode;
            if (missingNode != null)
                return missingNode.Field;

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                return existsNode.Field;

            return null;
        }

        public static bool IsNegated(this IQueryNode node) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                return (groupNode.IsNegated.HasValue && groupNode.IsNegated.Value == true) || (!String.IsNullOrEmpty(groupNode.Prefix) && groupNode.Prefix == "-");

            var termNode = node as TermNode;
            if (termNode != null)
                return (termNode.IsNegated.HasValue && termNode.IsNegated.Value == true) || (!String.IsNullOrEmpty(termNode.Prefix) && termNode.Prefix == "-");

            var termRangeNode = node as TermRangeNode;
            if (termRangeNode != null)
                return (termRangeNode.IsNegated.HasValue && termRangeNode.IsNegated.Value == true) || (!String.IsNullOrEmpty(termRangeNode.Prefix) && termRangeNode.Prefix == "-");

            var missingNode = node as MissingNode;
            if (missingNode != null)
                return (missingNode.IsNegated.HasValue && missingNode.IsNegated.Value == true) || (!String.IsNullOrEmpty(missingNode.Prefix) && missingNode.Prefix == "-");

            var existsNode = node as ExistsNode;
            if (existsNode != null)
                return (existsNode.IsNegated.HasValue && existsNode.IsNegated.Value == true) || (!String.IsNullOrEmpty(existsNode.Prefix) && existsNode.Prefix == "-");

            return false;
        }
    }
}