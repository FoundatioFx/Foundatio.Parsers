using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public class FieldResolverQueryVisitor : ChainableQueryVisitor {
    private readonly QueryFieldResolver _globalResolver;

    public FieldResolverQueryVisitor(QueryFieldResolver globalResolver = null) {
        _globalResolver = globalResolver;
    }

    public override async Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
        await ResolveField(node, context);
        await base.VisitAsync(node, context);
    }

    public override Task VisitAsync(TermNode node, IQueryVisitorContext context) {
        return ResolveField(node, context);
    }

    public override Task VisitAsync(TermRangeNode node, IQueryVisitorContext context) {
        return ResolveField(node, context);
    }

    public override Task VisitAsync(ExistsNode node, IQueryVisitorContext context) {
        return ResolveField(node, context);
    }

    public override Task VisitAsync(MissingNode node, IQueryVisitorContext context) {
        return ResolveField(node, context);
    }

    private async Task ResolveField(IFieldQueryNode node, IQueryVisitorContext context) {
        if (node.Parent == null || node.Field == null)
            return;

        var contextResolver = context.GetFieldResolver();
        if (_globalResolver == null && contextResolver == null)
            return;

        string resolvedField = null;
        if (contextResolver != null)
            resolvedField = await contextResolver(node.Field, context).ConfigureAwait(false);
        if (resolvedField == null && _globalResolver != null)
            resolvedField = await _globalResolver(node.Field, context).ConfigureAwait(false);

        if (resolvedField == null) {
            // add field to unresolved fields list
            context.GetValidationResult().UnresolvedFields.Add(node.Field);
            return;
        }

        if (!resolvedField.Equals(node.Field)) {
            node.SetOriginalField(node.Field);
            node.Field = resolvedField;
        }
    }

    public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
        await node.AcceptAsync(this, context).ConfigureAwait(false);
        return node;
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, QueryFieldResolver resolver, IQueryVisitorContextWithFieldResolver context = null) {
        return new FieldResolverQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContext { FieldResolver = resolver });
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, Func<string, string> resolver, IQueryVisitorContextWithFieldResolver context = null) {
        return new FieldResolverQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContext { FieldResolver = (field, _) => Task.FromResult(resolver(field)) });
    }

    public static IQueryNode Run(IQueryNode node, QueryFieldResolver resolver, IQueryVisitorContextWithFieldResolver context = null) {
        return RunAsync(node, resolver, context).GetAwaiter().GetResult();
    }

    public static IQueryNode Run(IQueryNode node, Func<string, string> resolver, IQueryVisitorContextWithFieldResolver context = null) {
        return RunAsync(node, resolver, context).GetAwaiter().GetResult();
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, IDictionary<string, string> map, IQueryVisitorContextWithFieldResolver context = null) {
        return new FieldResolverQueryVisitor().AcceptAsync(node, context ?? new QueryVisitorContext { FieldResolver = map.ToHierarchicalFieldResolver() });
    }

    public static IQueryNode Run(IQueryNode node, IDictionary<string, string> map, IQueryVisitorContextWithFieldResolver context = null) {
        return RunAsync(node, map, context).GetAwaiter().GetResult();
    }
}

public delegate Task<string> QueryFieldResolver(string field, IQueryVisitorContext context);

public class FieldMap : Dictionary<string, string> { }

public static class FieldMapExtensions {
    public static string GetValueOrNull(this IDictionary<string, string> map, string field) {
        if (map == null || field == null)
            return null;

        if (map.TryGetValue(field, out string value))
            return value;

        return null;
    }

    public static QueryFieldResolver ToHierarchicalFieldResolver(this IDictionary<string, string> map, string resultPrefix = null) {
        return (field, _) => {
            if (field == null)
                return null;

            if (map.TryGetValue(field, out string result))
                return Task.FromResult($"{resultPrefix}{result}");

            // start at the longest path and go backwards until we find a match in the map
            int currentPart = field.LastIndexOf('.');
            while (currentPart > 0) {
                string currentName = field.Substring(0, currentPart);
                if (map.TryGetValue(currentName, out string currentResult))
                    return Task.FromResult($"{resultPrefix}{currentResult}{field.Substring(currentPart)}");

                currentPart = field.LastIndexOf('.', currentPart - 1);
            }

            return Task.FromResult($"{resultPrefix}{field}");
        };
    }
}
