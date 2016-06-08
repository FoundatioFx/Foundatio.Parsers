using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros {
    public class GeoMacro: ElasticMacroBase {
        private readonly Func<string, bool> _isGeoField;
        private readonly Func<string, string> _resolveGeoLocation;

        public GeoMacro(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation = null) {
            _isGeoField = isGeoField;
            _resolveGeoLocation = resolveGeoLocation;
        }

        public override PlainFilter Expand(TermNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            if (!_isGeoField(node.Field))
                return currentFilter;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;

            return new GeoDistanceFilter { Field = node.Field, Location = location, Distance = node.Proximity };
        }

        public override PlainFilter Expand(TermRangeNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            if (!_isGeoField(node.Field))
                return currentFilter;

            return new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field };
        }
    }
}
