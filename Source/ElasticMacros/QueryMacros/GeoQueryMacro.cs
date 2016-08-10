using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros.QueryMacros {
    public class GeoQueryMacro: ElasticQueryMacroBase {
        private readonly Func<string, bool> _isGeoField;
        private readonly Func<string, string> _resolveGeoLocation;

        public GeoQueryMacro(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation = null) {
            _isGeoField = isGeoField;
            _resolveGeoLocation = resolveGeoLocation;
        }

        public override void Expand(TermNode node, ElasticQueryMacroContext ctx) {
            if (!_isGeoField(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            ctx.Query = new FilteredQuery {
                Filter = new GeoDistanceFilter { Field = node.Field, Location = location, Distance = node.Proximity }.ToContainer()
            };
        }

        public override void Expand(TermRangeNode node, ElasticQueryMacroContext ctx) {
            if (!_isGeoField(node.Field))
                return;

            ctx.Query = new FilteredQuery {
                Filter = new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field }.ToContainer()
            };
        }
    }
}
