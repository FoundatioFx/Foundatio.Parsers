using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class GeoVisitor: ChainableQueryVisitor {
        private readonly Func<string, string> _resolveGeoLocation;

        public GeoVisitor(Func<string, string> resolveGeoLocation = null) {
            _resolveGeoLocation = resolveGeoLocation;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null || !elasticContext.IsGeoPropertyType(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            var query = new GeoDistanceQuery { Field = node.Field, Location = location, Distance = node.Proximity };
            node.SetQuery(query);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null || !elasticContext.IsGeoPropertyType(node.Field))
                return;

            var box = new BoundingBox { TopLeft = node.Min, BottomRight = node.Max };
            var query = new GeoBoundingBoxQuery { BoundingBox = box, Field = node.Field };
            node.SetQuery(query);
        }
    }
}
