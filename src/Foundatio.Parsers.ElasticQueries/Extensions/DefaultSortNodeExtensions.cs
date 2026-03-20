using System;
using System.Collections.Generic;
using System.Globalization;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
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

        string field = elasticContext.MappingResolver.GetSortFieldName(node.UnescapedField);
        var fieldType = elasticContext.MappingResolver.GetFieldType(field);

        if (fieldType == FieldType.GeoPoint && !String.IsNullOrEmpty(node.UnescapedTerm))
            return GetGeoDistanceSort(node, elasticContext, field);

        var fieldSort = new FieldSort(field)
        {
            UnmappedType = fieldType == FieldType.None ? FieldType.Keyword : fieldType,
            Order = node.IsNodeOrGroupNegated() ? SortOrder.Desc : SortOrder.Asc
        };

        string nestedPath = node.GetNestedPath();
        if (nestedPath is not null)
        {
            var nestedSort = new NestedSortValue { Path = nestedPath };
            var nestedFilter = node.GetNestedFilter();
            if (nestedFilter is not null)
                nestedSort.Filter = nestedFilter;

            fieldSort.Nested = nestedSort;
        }

        return new SortOptions
        {
            Field = fieldSort
        };
    }

    private static SortOptions GetGeoDistanceSort(TermNode node, IElasticQueryVisitorContext context, string field)
    {
        string location = node.UnescapedTerm;
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

    private static IList<GeoLocation> ParseGeoLocations(string location)
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
}
