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
            if (elasticContext == null || !elasticContext.IsGeoFieldType(node.Field))
                return;

            string location = _resolveGeoLocation != null ? _resolveGeoLocation(node.Term) ?? node.Term : node.Term;
            var filter = new GeoDistanceFilter { Field = node.Field, Location = location, Distance = node.Proximity };
            node.SetFilter(filter);
            node.SetQuery(new FilteredQuery { Filter = filter.ToContainer() });
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null || !elasticContext.IsGeoFieldType(node.Field))
                return;

            var filter = new GeoBoundingBoxFilter { TopLeft = node.Min, BottomRight = node.Max, Field = node.Field };
            node.SetFilter(filter);
            node.SetQuery(new FilteredQuery { Filter = filter.ToContainer() });
        }
    }
}
