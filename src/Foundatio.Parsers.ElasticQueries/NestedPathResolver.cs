using System;
using System.Collections.Generic;

namespace Foundatio.Parsers.ElasticQueries;

/// <summary>
/// Shared helper for resolving nested path chains from a fully-qualified field name.
/// Centralizes logic previously duplicated in NestedVisitor, CombineAggregationsVisitor,
/// DefaultSortNodeExtensions, and DefaultQueryNodeExtensions.
/// </summary>
public static class NestedPathResolver
{
    /// <summary>
    /// Returns the deepest nested path for a given fully-qualified field name,
    /// or null if no nested property is found in the path segments.
    /// </summary>
    public static string? GetDeepestNestedPath(string fullName, ElasticMappingResolver mappingResolver)
    {
        var segments = fullName.Split('.');
        if (segments.Length == 0)
            return null;

        string? deepestNestedPath = null;
        string current = "";
        for (int i = 0; i < segments.Length; i++)
        {
            current = i == 0 ? segments[i] : $"{current}.{segments[i]}";
            if (mappingResolver.IsNestedPropertyType(current))
                deepestNestedPath = current;
        }

        return deepestNestedPath;
    }

    /// <summary>
    /// Returns the complete chain of nested paths from outermost to innermost
    /// for a given deepest nested path. Each entry in the returned list is a
    /// path prefix that resolves to a nested mapping type.
    /// </summary>
    public static IReadOnlyList<string> GetNestedPathChain(string deepestPath, ElasticMappingResolver mappingResolver)
    {
        var segments = deepestPath.Split('.');
        var nestedPaths = new List<string>();
        string current = "";
        for (int i = 0; i < segments.Length; i++)
        {
            current = i == 0 ? segments[i] : $"{current}.{segments[i]}";
            if (mappingResolver.IsNestedPropertyType(current))
                nestedPaths.Add(current);
        }

        return nestedPaths.Count > 0 ? nestedPaths : [deepestPath];
    }
}
