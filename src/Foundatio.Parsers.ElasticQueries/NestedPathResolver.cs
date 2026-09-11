using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries;

/// <summary>
/// Shared helper for resolving nested path chains from a fully-qualified field name.
/// Centralizes logic previously duplicated in NestedVisitor, CombineAggregationsVisitor,
/// DefaultSortNodeExtensions, and DefaultQueryNodeExtensions.
/// </summary>
public static class NestedPathResolver
{
    private const string NestedAggPrefix = "nested_";

    /// <summary>
    /// Returns the standard nested aggregation name for a given path (e.g., "parent" becomes "nested_parent").
    /// </summary>
    public static string GetNestedAggName(string path) => $"{NestedAggPrefix}{path}";

    /// <summary>
    /// Returns the deepest nested path for a given fully-qualified field name,
    /// or null if no nested property is found in the path segments.
    /// </summary>
    public static string? GetDeepestNestedPath(string fullName, ElasticMappingResolver mappingResolver)
    {
        string? deepestNestedPath = null;

        for (int i = 0; i <= fullName.Length; i++)
        {
            if (i < fullName.Length && fullName[i] is not '.')
                continue;

            string prefix = fullName[..i];
            if (mappingResolver.IsNestedPropertyType(prefix))
                deepestNestedPath = prefix;
        }

        return deepestNestedPath;
    }

    internal static string? GetDeepestNestedPath(string fullName, IQueryVisitorContext context)
    {
        if (context.TryGetMappingResult(fullName, out var mapping))
            return mapping?.NestedPathChain.LastOrDefault();

        return GetDeepestNestedPath(fullName, context.GetMappingResolver());
    }

    /// <summary>
    /// Returns the complete chain of nested paths from outermost to innermost
    /// for a given deepest nested path. Each entry in the returned list is a
    /// path prefix that resolves to a nested mapping type.
    /// </summary>
    public static IReadOnlyList<string> GetNestedPathChain(string deepestPath, ElasticMappingResolver mappingResolver)
    {
        var nestedPaths = new List<string>();

        for (int i = 0; i <= deepestPath.Length; i++)
        {
            if (i < deepestPath.Length && deepestPath[i] is not '.')
                continue;

            string prefix = deepestPath[..i];
            if (mappingResolver.IsNestedPropertyType(prefix))
                nestedPaths.Add(prefix);
        }

        return nestedPaths.Count > 0 ? nestedPaths : [deepestPath];
    }

    internal static IReadOnlyList<string> GetNestedPathChain(string deepestPath, IQueryVisitorContext context)
    {
        if (context.TryGetMappingResult(deepestPath, out var mapping)
            && mapping?.NestedPathChain is { Count: > 0 } nestedPathChain)
        {
            for (int i = 0; i < nestedPathChain.Count; i++)
            {
                if (nestedPathChain[i] == deepestPath)
                    return nestedPathChain.Take(i + 1).ToArray();
            }
        }

        return GetNestedPathChain(deepestPath, context.GetMappingResolver());
    }
}
