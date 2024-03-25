using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public delegate bool ShouldSkipIncludeFunc(TermNode node, IQueryVisitorContext context);
public delegate Task<string> IncludeResolver(string name);

public class IncludeVisitor : ChainableMutatingQueryVisitor
{
    private readonly LuceneQueryParser _parser = new();
    private readonly ShouldSkipIncludeFunc _shouldSkipInclude;
    private readonly string _name;

    public IncludeVisitor(ShouldSkipIncludeFunc shouldSkipInclude = null, string name = "include")
    {
        _shouldSkipInclude = shouldSkipInclude;
        _name = name;
    }

    public override async Task<IQueryNode> VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        if (node.Field != "@" + _name || (_shouldSkipInclude != null && _shouldSkipInclude(node, context)))
            return node;

        var includeResolver = context.GetIncludeResolver();
        if (includeResolver == null)
            return node;

        var includes = context.GetValidationResult().ReferencedIncludes;
        includes.Add(node.Term);

        var includeStack = context.GetIncludeStack();
        if (includeStack.Contains(node.Term))
        {
            context.AddValidationError($"Recursive {_name} ({node.Term})");
            return node;
        }

        try
        {
            string includedQuery = await includeResolver(node.Term).ConfigureAwait(false);
            if (includedQuery == null)
            {
                context.AddValidationError($"Unresolved {_name} ({node.Term})");
                context.GetValidationResult().UnresolvedIncludes.Add(node.Term);
            }

            if (String.IsNullOrEmpty(includedQuery))
                return node;

            includeStack.Push(node.Term);

            var result = (GroupNode)await _parser.ParseAsync(includedQuery).ConfigureAwait(false);
            result.HasParens = true;
            await VisitAsync(result, context).ConfigureAwait(false);

            includeStack.Pop();

            return node.ReplaceSelf(result);
        }
        catch (Exception ex)
        {
            context.AddValidationError($"Error in {_name} resolver callback when resolving {_name} ({node.Term}): {ex.Message}");
            context.GetValidationResult().UnresolvedIncludes.Add(node.Term);

            return node;
        }
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, IncludeResolver includeResolver, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null)
    {
        context ??= new QueryVisitorContext();
        context.SetIncludeResolver(includeResolver);

        return new IncludeVisitor(shouldSkipInclude).AcceptAsync(node, context);
    }

    public static IQueryNode Run(IQueryNode node, IncludeResolver includeResolver, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null)
    {
        return RunAsync(node, includeResolver, context, shouldSkipInclude).GetAwaiter().GetResult();
    }

    public static IQueryNode Run(IQueryNode node, Func<string, string> includeResolver, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null)
    {
        return RunAsync(node, name => Task.FromResult(includeResolver(name)), context, shouldSkipInclude).GetAwaiter().GetResult();
    }

    public static Task<IQueryNode> RunAsync(IQueryNode node, IDictionary<string, string> includes, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null)
    {
        return RunAsync(node, name => Task.FromResult(includes.ContainsKey(name) ? includes[name] : null), context, shouldSkipInclude);
    }

    public static IQueryNode Run(IQueryNode node, IDictionary<string, string> includes, IQueryVisitorContextWithIncludeResolver context = null, ShouldSkipIncludeFunc shouldSkipInclude = null)
    {
        return RunAsync(node, includes, context, shouldSkipInclude).GetAwaiter().GetResult();
    }
}
