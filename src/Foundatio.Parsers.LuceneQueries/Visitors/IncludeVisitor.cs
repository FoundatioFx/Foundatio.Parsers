using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Extensions;
using System.Collections.Generic;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public delegate bool ShouldSkipIncludeFunc(TermNode node, IQueryVisitorContext context);
public delegate Task<string> IncludeResolver(string name);

public class IncludeVisitor : ChainableMutatingQueryVisitor {
    private readonly LuceneQueryParser _parser = new();
    private readonly ShouldSkipIncludeFunc _shouldSkipInclude;

    public IncludeVisitor(ShouldSkipIncludeFunc shouldSkipInclude = null) {
        _shouldSkipInclude = shouldSkipInclude;
    }

    public override async Task<IQueryNode> VisitAsync(TermNode node, IQueryVisitorContext context) {
        if (node.Field != "@include" || (_shouldSkipInclude != null && _shouldSkipInclude(node, context)))
            return node;

        var includeResolver = context.GetIncludeResolver();
        if (includeResolver == null)
            return node;

        var includes = context.GetValidationResult().ReferencedIncludes;
        if (includes.Contains(node.Term)) {
            context.AddValidationError($"Recursive include ({node.Term})");
            return node;
        }

        includes.Add(node.Term);
        string includedQuery = await includeResolver(node.Term).ConfigureAwait(false);
        if (includedQuery == null) {
            // unresolved include
        }

        if (String.IsNullOrEmpty(includedQuery))
            return node;

        var result = (GroupNode)await _parser.ParseAsync(includedQuery).ConfigureAwait(false);
        result.HasParens = true;
        await VisitAsync(result, context).ConfigureAwait(false);

        return node.ReplaceSelf(result);
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, IncludeResolver includeResolver, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null) {
        context ??= new QueryVisitorContext();
        context.SetIncludeResolver(includeResolver);

        return new IncludeVisitor(shouldSkipInclude).AcceptAsync(node, context ?? new QueryVisitorContext { IncludeResolver = includeResolver });
    }

    public static IQueryNode Run(IQueryNode node, IncludeResolver includeResolver, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null) {
        return RunAsync(node, includeResolver, context, shouldSkipInclude).GetAwaiter().GetResult();
    }

    public static IQueryNode Run(IQueryNode node, Func<string, string> includeResolver, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null) {
        return RunAsync(node, name => Task.FromResult(includeResolver(name)), context, shouldSkipInclude).GetAwaiter().GetResult();
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, IDictionary<string, string> includes, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null) {
        return RunAsync(node, name => Task.FromResult(includes.ContainsKey(name) ? includes[name] : null), context, shouldSkipInclude);
    }

    public static IQueryNode Run(IQueryNode node, IDictionary<string, string> includes, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null) {
        return RunAsync(node, includes, context, shouldSkipInclude).GetAwaiter().GetResult();
    }
}
