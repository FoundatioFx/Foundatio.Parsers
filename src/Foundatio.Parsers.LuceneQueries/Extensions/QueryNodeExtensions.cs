using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Extensions {
    public static class QueryNodeExtensions {
        public static bool IsExcluded(this IFieldQueryNode node) {
            if (node == null)
                return false;
            
            return (node.IsNegated.HasValue && node.IsNegated.Value == true) || (!String.IsNullOrEmpty(node.Prefix) && node.Prefix == "-");
        }
        
        public static bool IsRequired(this IFieldQueryNode node) {
            if (node == null)
                return false;
            
            return !String.IsNullOrEmpty(node.Prefix) && node.Prefix == "+";
        }
        
        public static bool IsNodeOrGroupNegated(this IFieldQueryNode node) {
            if (node.IsRequired())
                return false;
            
            return node.IsExcluded() || node.GetGroupNode().IsExcluded();
        }

        public static GroupNode GetGroupNode(this IQueryNode node) {
            if (node == null)
                return null;
            
            var current = node;
            do {
                if (current is GroupNode groupNode && (groupNode.HasParens || groupNode.Parent == null))
                    return groupNode;
                
                current = current.Parent;
            } while (current != null);

            return null;
        }

        private const string TimeZoneKey = "@TimeZone";
        public static string GetTimeZone(this IFieldQueryNode node, string defaultTimeZone = null) {
            if (!node.Data.TryGetValue(TimeZoneKey, out var value))
                return defaultTimeZone;

            return value as string;
        }

        public static void SetTimeZone(this IFieldQueryNode node, string timeZone) {
            node.Data[TimeZoneKey] = timeZone;
        }

        private const string OriginalFieldKey = "@OriginalField";
        public static string GetOriginalField(this IFieldQueryNode node) {
            if (!node.Data.TryGetValue(OriginalFieldKey, out var value))
                return node.Field;

            return value as string;
        }

        public static void SetOriginalField(this IFieldQueryNode node, string field) {
            node.Data[OriginalFieldKey] = field;
        }

        private const string OperationTypeKey = "@OperationType";
        public static string GetOperationType(this IQueryNode node) {
            if (!node.Data.TryGetValue(OperationTypeKey, out var value))
                return null;

            return (string)value;
        }

        public static void SetOperationType(this IQueryNode node, string operationType) {
            node.Data[OperationTypeKey] = operationType;
        }
        
        public static string[] GetDefaultFields(this IQueryNode node, string[] rootDefaultFields) {
            var scopedNode = GetGroupNode(node);
            return !String.IsNullOrEmpty(scopedNode?.Field) ? new[] { scopedNode.Field } : rootDefaultFields;
        }
    }
}