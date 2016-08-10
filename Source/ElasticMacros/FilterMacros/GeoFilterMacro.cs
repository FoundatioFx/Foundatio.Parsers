using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros.FilterMacros {
    public class GeoFilterMacro: ElasticFilterMacroBase {
        private readonly Func<string, bool> _isGeoField;
        private readonly Func<string, string> _resolveGeoLocation;

        public GeoFilterMacro(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation = null) {
            _isGeoField = isGeoField;
            _resolveGeoLocation = resolveGeoLocation;
        }

        public override void Expand(TermNode node, ElasticFilterMacroContext ctx) {
            if (!_isGeoField(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            ctx.Filter = new GeoDistanceFilter { Field = node.Field, Location = location, Distance = node.Proximity };
        }

        public override void Expand(TermRangeNode node, ElasticFilterMacroContext ctx) {
            if (!_isGeoField(node.Field))
                return;
            
            ctx.Filter = new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field };
        }
    }
}
