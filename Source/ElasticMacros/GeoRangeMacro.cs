using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros {
    public class GeoRangeMacro: ElasticMacroBase {
        private readonly Func<string, bool> _isGeoField;

        public GeoRangeMacro(Func<string, bool> isGeoField) {
            _isGeoField = isGeoField;
        }

        public override PlainFilter Expand(TermRangeNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            if (!_isGeoField(node.Field))
                return currentFilter;

            return new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field };
        }
    }
}
