using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class GeoVisitor : ChainableQueryVisitor
{
    private readonly Func<string, Task<string>>? _resolveGeoLocation;

    public GeoVisitor(Func<string, Task<string>>? resolveGeoLocation = null)
    {
        _resolveGeoLocation = resolveGeoLocation;
    }

    public override async Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        if (context.QueryType != QueryTypes.Query || context is not IElasticQueryVisitorContext elasticContext)
            return;

        string? fieldName = node.Field;
        if (String.IsNullOrEmpty(fieldName)
            || (await context.GetMappingResultAsync(fieldName).AnyContext())?.Property is not GeoPointProperty)
            return;

        string? location = null;

        if (node.Term is not null && elasticContext.GeoLocationResolver is not null)
            location = await elasticContext.GeoLocationResolver(node.Term).AnyContext();

        if (location is null && node.Term is not null && _resolveGeoLocation is not null)
            location = await _resolveGeoLocation(node.Term).AnyContext();

        location ??= node.Term;
        if (String.IsNullOrWhiteSpace(location))
            return;

        var query = new GeoDistanceQuery(node.Proximity ?? "10mi", fieldName, location);
        node.SetQuery(query);
    }

    public override async Task VisitAsync(TermRangeNode node, IQueryVisitorContext context)
    {
        using var mappingScope = context.BeginMappingScope();
        if (context is IElasticQueryVisitorContext && !String.IsNullOrEmpty(node.Field))
            await context.GetMappingResultAsync(node.Field).AnyContext();

        Visit(node, context);
    }

    public override void Visit(TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext)
            return;

        string? fieldName = node.Field;
        if (String.IsNullOrEmpty(fieldName)
            || context.GetMappingResult(fieldName)?.Property is not GeoPointProperty)
            return;

        if (node.Min is null || node.Max is null)
            return;

        // GeoBoundingBoxQuery ergonomic feedback: https://github.com/elastic/elasticsearch-net/issues/8496
        var box = GeoBounds.TopLeftBottomRight(new TopLeftBottomRightGeoBounds { TopLeft = GeoLocation.Text(node.Min), BottomRight = GeoLocation.Text(node.Max) });
        var query = new GeoBoundingBoxQuery(box, fieldName);
        node.SetQuery(query);
    }
}
