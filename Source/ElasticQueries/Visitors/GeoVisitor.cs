using System;
using Foundatio.Parsers.ElasticQueries.Filter.Nodes;
using Foundatio.Parsers.ElasticQueries.Query.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class GeoVisitor: ElasticCombinedNodeVisitorBase {
        private readonly Func<string, bool> _isGeoField;
        private readonly Func<string, string> _resolveGeoLocation;

        public GeoVisitor(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation = null) {
            _isGeoField = isGeoField;
            _resolveGeoLocation = resolveGeoLocation;
        }

        public override void Visit(FilterTermNode node) {
            if (!_isGeoField(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            node.Filter = new GeoDistanceFilter { Field = node.Field, Location = location, Distance = node.Proximity };
        }

        public override void Visit(FilterTermRangeNode node) {
            if (!_isGeoField(node.Field))
                return;

            node.Filter = new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field };
        }

        public override void Visit(QueryTermNode node) {
            if (!_isGeoField(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            node.Query = new FilteredQuery {
                Filter = new GeoDistanceFilter { Field = node.Field, Location = location, Distance = node.Proximity }.ToContainer()
            };
        }

        public override void Visit(QueryTermRangeNode node) {
            if (!_isGeoField(node.Field))
                return;

            node.Query = new FilteredQuery {
                Filter = new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field }.ToContainer()
            };
        }
    }
}
