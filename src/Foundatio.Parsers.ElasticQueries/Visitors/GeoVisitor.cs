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

        string location = _resolveGeoLocation != null ? await _resolveGeoLocation(node.Term).ConfigureAwait(false) ?? node.Term : node.Term;
        var query = new GeoDistanceQuery { Field = node.Field, Location = location, Distance = node.Proximity ?? "10mi" };
        node.SetQuery(query);
    }

    public override void Visit(TermRangeNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext || !elasticContext.MappingResolver.IsGeoPropertyType(node.Field))
            return;

        var box = GeoBounds.TopLeftBottomRight(new TopLeftBottomRightGeoBounds { TopLeft = node.Min, BottomRight = node.Max });
        var query = new GeoBoundingBoxQuery { BoundingBox = box, Field = node.Field };
        node.SetQuery(query);
    }
}
