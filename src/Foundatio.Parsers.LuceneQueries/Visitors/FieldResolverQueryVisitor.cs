using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
            return new FieldResolverQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContextWithFieldResolver { FieldResolver = field => map.GetValueOrNull(field) });
        }

        public static IQueryNode Run(IQueryNode node, IDictionary<string, string> map, IQueryVisitorContextWithFieldResolver context = null) {
            return RunAsync(node, map, context).GetAwaiter().GetResult();
        }
    }

    public delegate string QueryFieldResolver(string field);

    public class FieldMap : Dictionary<string, string> {}

    public static class FieldMapExtensions {
        public static string GetValueOrNull(this IDictionary<string, string> map, string field) {
            if (map == null || field == null)
                return null;
            
            if (map.TryGetValue(field, out string value))
                return value;

            return null;
        }
    }
}
