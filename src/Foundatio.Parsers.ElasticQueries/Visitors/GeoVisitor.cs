using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class GeoVisitor : ChainableQueryVisitor
{
    private readonly Func<string, Task<string>> _resolveGeoLocation;

    public GeoVisitor(Func<string, Task<string>> resolveGeoLocation = null)
    {
        _resolveGeoLocation = resolveGeoLocation;
    }

    public override async Task VisitAsync(TermNode node, IQueryVisitorContext context)
    {
        if (context.QueryType != QueryTypes.Query || context is not IElasticQueryVisitorContext elasticContext || !elasticContext.MappingResolver.IsGeoPropertyType(node.Field))
            return;

        string location = null;

        if (elasticContext.GeoLocationResolver != null)
            location = await elasticContext.GeoLocationResolver(node.Term).ConfigureAwait(false);

        if (location == null && _resolveGeoLocation != null)
            location = await _resolveGeoLocation(node.Term).ConfigureAwait(false);

        location ??= node.Term;

        var query = new GeoDistanceQuery(node.Proximity ?? "10mi", node.Field, location);
        node.SetQuery(query);
    }

    public override void Visit(TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext || !elasticContext.MappingResolver.IsGeoPropertyType(node.Field))
            return;

        // GeoBoundingBoxQuery ergonomic feedback: https://github.com/elastic/elasticsearch-net/issues/8496
        var box = GeoBounds.TopLeftBottomRight(new TopLeftBottomRightGeoBounds { TopLeft = node.Min, BottomRight = node.Max });
        var query = new GeoBoundingBoxQuery(box, node.Field);
        node.SetQuery(query);
    }
}
