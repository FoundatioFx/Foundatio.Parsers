using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class FieldResolverQueryVisitor : ChainableQueryVisitor {
        private readonly QueryFieldResolver _globalResolver;

        public FieldResolverQueryVisitor(QueryFieldResolver globalResolver = null) {
            _globalResolver = globalResolver;
        }
        
        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            ResolveField(node, context);

            return base.VisitAsync(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            ResolveField(node, context);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            ResolveField(node, context);
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            ResolveField(node, context);
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            ResolveField(node, context);
        }

        private void ResolveField(IFieldQueryNode node, IQueryVisitorContext context) {
            if (node.Parent == null)
                return;
            
            var contextResolver = context.GetFieldResolver();
            var resolvedField = contextResolver?.Invoke(node.Field) ?? _globalResolver?.Invoke(node.Field) ?? node.Field;
            if (resolvedField != null && !resolvedField.Equals(node.Field, StringComparison.OrdinalIgnoreCase)) {
                node.SetOriginalField(node.Field);
                node.Field = resolvedField;
            }
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            await node.AcceptAsync(this, context).ConfigureAwait(false);
            return node;
        }

        public static Task<IQueryNode> RunAsync(IQueryNode node, QueryFieldResolver resolver, IQueryVisitorContextWithFieldResolver context = null) {
            return new FieldResolverQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContextWithFieldResolver { FieldResolver = resolver });
        }

        public static IQueryNode Run(IQueryNode node, QueryFieldResolver resolver, IQueryVisitorContextWithFieldResolver context = null) {
            return RunAsync(node, resolver, context).GetAwaiter().GetResult();
        }

        public static Task<IQueryNode> RunAsync(IQueryNode node, IDictionary<string, string> map, IQueryVisitorContextWithFieldResolver context = null) {
            return new FieldResolverQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContextWithFieldResolver { FieldResolver = map.ToHierarchicalFieldResolver() });
        }

        public static IQueryNode Run(IQueryNode node, IDictionary<string, string> map, IQueryVisitorContextWithFieldResolver context = null) {
            return RunAsync(node, map, context).GetAwaiter().GetResult();
        }
    }

    public delegate string QueryFieldResolver(string field);

    public class FieldMap : Dictionary<string, string> {}
    
    public static class FieldMapExtensions {
        public static string GetValueOrDefault(this IDictionary<string, string> map, string field) {
            if (map == null || field == null)
                return null;
            
            if (map.TryGetValue(field, out string value))
                return value;

            return null;
        }

        public static QueryFieldResolver ToHierarchicalFieldResolver(this IDictionary<string, string> map) {
            return field => {
                if (field == null)
                    return null;
                
                if (map.TryGetValue(field, out string result))
                    return result;
                
                // start at the longest path and go backwards until we find a match in the map
                int currentPart = field.LastIndexOf('.');
                while (currentPart > 0) {
                    string currentName = field.Substring(0, currentPart);
                    if (map.TryGetValue(currentName, out string currentResult))
                        return currentResult + field.Substring(currentPart);
                    
                    currentPart = field.LastIndexOf('.', currentPart - 1);
                }
                
                return field;
            };
        }
    }
}
