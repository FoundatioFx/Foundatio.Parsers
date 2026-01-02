using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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

        // ES 9.x doesn't support simple field sorting on geo_point fields
        // Geo_point fields require geo_distance sort with a reference point
        if (fieldType == FieldType.GeoPoint)
            return GetGeoDistanceSortAsync(node, elasticContext, field).GetAwaiter().GetResult();

        return new SortOptions
        {
            Field = new FieldSort(field)
            {
                UnmappedType = fieldType == FieldType.None ? FieldType.Keyword : fieldType,
                Order = node.IsNodeOrGroupNegated() ? SortOrder.Desc : SortOrder.Asc
            }
        };
    }

    private static async Task<SortOptions> GetGeoDistanceSortAsync(TermNode node, IElasticQueryVisitorContext context, string field)
    {
        // For geo_distance sort, we need a reference location
        // If no term is provided, we can't create a geo_distance sort
        if (String.IsNullOrEmpty(node.UnescapedTerm))
            return null;

        // Resolve the location using the geo resolver if available
        string location = node.UnescapedTerm;
        if (context.GeoLocationResolver != null)
            location = await context.GeoLocationResolver(location).ConfigureAwait(false) ?? location;

        // Parse the location string (expected format: "lat,lon" or similar)
        var geoLocations = ParseGeoLocations(location);
        if (geoLocations == null || geoLocations.Count == 0)
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

        // Try parsing as "lat,lon" format
        var parts = location.Split(',');
        if (parts.Length == 2 &&
            Double.TryParse(parts[0].Trim(), out double lat) &&
            Double.TryParse(parts[1].Trim(), out double lon))
        {
            return new List<GeoLocation>
            {
                GeoLocation.LatitudeLongitude(new LatLonGeoLocation { Lat = lat, Lon = lon })
            };
        }

        // Return as text format (geohash or other string representation)
        return new List<GeoLocation> { GeoLocation.Text(location) };
    }
}
