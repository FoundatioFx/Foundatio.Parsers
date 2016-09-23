using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class GeoVisitor: ChainableQueryVisitor {
        private readonly Func<string, bool> _isGeoField;
        private readonly Func<string, string> _resolveGeoLocation;

        public GeoVisitor(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation = null) {
            _isGeoField = isGeoField;
            _resolveGeoLocation = resolveGeoLocation;
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (!_isGeoField(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            var query = new GeoDistanceQuery { Field = node.Field, Location = location, Distance = node.Proximity };
            node.SetQuery(query);
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            if (!_isGeoField(node.Field))
                return;

            var query = new GeoBoundingBoxQuery { BoundingBox = { TopLeft = node.Min, BottomRight = node.Max }, Field = node.Field };
            node.SetQuery(query);
        }
    }
}
