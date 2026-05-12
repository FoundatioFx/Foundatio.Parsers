using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultSortNodeExtensions
{
    public static IFieldSort GetDefaultSort(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string? field = elasticContext.MappingResolver.GetSortFieldName(node.UnescapedField);
        var fieldType = elasticContext.MappingResolver.GetFieldType(field);

        var sort = new FieldSort
        {
            Field = field,
            UnmappedType = fieldType == FieldType.None ? FieldType.Keyword : fieldType,
            Order = node.IsNodeOrGroupNegated() ? SortOrder.Descending : SortOrder.Ascending
        };

        string? nestedPath = node.GetNestedPath();
        if (nestedPath is not null)
        {
            sort.Nested = BuildHierarchicalNestedSort(nestedPath, node.GetNestedFilter(), elasticContext);
        }

        return sort;
    }

    private static NestedSort BuildHierarchicalNestedSort(
        string deepestPath, QueryContainer? filter, IElasticQueryVisitorContext context)
    {
        var nestedPaths = NestedPathResolver.GetNestedPathChain(deepestPath, context.MappingResolver);

        if (nestedPaths.Count <= 1)
        {
            var nestedSort = new NestedSort { Path = deepestPath };
            if (filter is not null)
                nestedSort.Filter = filter;
            return nestedSort;
        }

        NestedSort? innermost = null;
        for (int i = nestedPaths.Count - 1; i >= 0; i--)
        {
            var nestedSort = new NestedSort { Path = nestedPaths[i] };
            if (i == nestedPaths.Count - 1 && filter is not null)
                nestedSort.Filter = filter;

            if (innermost is not null)
                nestedSort.Nested = innermost;

            innermost = nestedSort;
        }

        return innermost!;
    }
}
