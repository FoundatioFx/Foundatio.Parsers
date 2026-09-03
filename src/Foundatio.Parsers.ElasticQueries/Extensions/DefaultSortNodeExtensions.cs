using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultSortNodeExtensions
{
    public static SortOptions GetDefaultSort(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        using var mappingScope = context.BeginMappingScope();
        var mapping = context.GetMappingResult(node.UnescapedField);
        return GetDefaultSort(node, context, elasticContext, mapping);
    }

    internal static async ValueTask PrepareSortMappingAsync(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        var mapping = await context.GetMappingResultAsync(node.UnescapedField).AnyContext();
        string? field = elasticContext.MappingResolver.GetNonAnalyzedFieldName(
            node.UnescapedField, mapping, ElasticMapping.SortFieldName);
        await context.GetMappingResultAsync(field).AnyContext();
        if (node.GetNestedPath() is { } nestedPath)
            await context.GetMappingResultAsync(nestedPath).AnyContext();
    }

    private static SortOptions GetDefaultSort(TermNode node, IQueryVisitorContext visitorContext,
        IElasticQueryVisitorContext elasticContext, FieldMapping? mapping)
    {
        string? field = elasticContext.MappingResolver.GetNonAnalyzedFieldName(
            node.UnescapedField, mapping, ElasticMapping.SortFieldName);
        var fieldType = ElasticMappingResolver.GetFieldType(visitorContext.GetMappingResult(field)?.Property);

        if (fieldType == FieldType.GeoPoint && !String.IsNullOrEmpty(node.UnescapedTerm))
            return GetGeoDistanceSort(node, elasticContext, field!)!;

        var fieldSort = new FieldSort(field!)
        {
            UnmappedType = fieldType == FieldType.None ? FieldType.Keyword : fieldType,
            Order = node.IsNodeOrGroupNegated() ? SortOrder.Desc : SortOrder.Asc
        };

        string? nestedPath = node.GetNestedPath();
        if (nestedPath is not null)
        {
            fieldSort.Nested = BuildHierarchicalNestedSort(nestedPath, node.GetNestedFilter(), visitorContext);
        }

        return new SortOptions
        {
            Field = fieldSort
        };
    }

    private static SortOptions? GetGeoDistanceSort(TermNode node, IElasticQueryVisitorContext context, string field)
    {
        string? location = node.UnescapedTerm;
        var geoLocations = ParseGeoLocations(location);
        if (geoLocations is null || geoLocations.Count == 0)
            return null;

        return new SortOptions
        {
            GeoDistance = new GeoDistanceSort
            {
                Field = field,
                Location = geoLocations,
                Order = node.IsNodeOrGroupNegated() ? SortOrder.Desc : SortOrder.Asc,
                DistanceType = GeoDistanceType.Arc
            }
        };
    }

    private static IList<GeoLocation>? ParseGeoLocations(string? location)
    {
        if (String.IsNullOrEmpty(location))
            return null;

        var parts = location.Split(',');
        if (parts.Length == 2 &&
            Double.TryParse(parts[0].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out double lat) &&
            Double.TryParse(parts[1].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out double lon))
        {
            return [GeoLocation.LatitudeLongitude(new LatLonGeoLocation { Lat = lat, Lon = lon })];
        }

        return [GeoLocation.Text(location)];
    }

    private static NestedSortValue BuildHierarchicalNestedSort(
        string deepestPath, Query? filter, IQueryVisitorContext context)
    {
        var nestedPaths = NestedPathResolver.GetNestedPathChain(deepestPath, context);

        if (nestedPaths.Count <= 1)
        {
            var nestedSort = new NestedSortValue { Path = deepestPath };
            if (filter is not null)
                nestedSort.Filter = filter;
            return nestedSort;
        }

        NestedSortValue? innermost = null;
        for (int i = nestedPaths.Count - 1; i >= 0; i--)
        {
            var nestedSort = new NestedSortValue { Path = nestedPaths[i] };
            if (i == nestedPaths.Count - 1 && filter is not null)
                nestedSort.Filter = filter;

            if (innermost is not null)
                nestedSort.Nested = innermost;

            innermost = nestedSort;
        }

        return innermost!;
    }
}
