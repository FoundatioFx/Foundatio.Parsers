using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class AssignOperationTypeVisitor: ChainableQueryVisitor {
        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Field))
                return base.VisitAsync(node, context);
            
            if (!(node.Left is TermNode leftTerm) || !String.IsNullOrEmpty(leftTerm.Field))
                throw new ApplicationException("The first item in an aggregation group must be the name of the target field.");
            
            node.SetOperationType(node.Field);
            node.Field = leftTerm.Term;
            node.Boost = leftTerm.Boost;
            node.Proximity = leftTerm.Proximity;
            node.Left = null;

            return base.VisitAsync(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Term))
                return;
            
            node.SetOperationType(node.Field);
            node.Field = node.Term;
            node.Term = null;
        }
    }

    public static class AggregationType {
        public const string Min = "min";
        public const string Max = "max";
        public const string Avg = "avg";
        public const string Sum = "sum";
        public const string Cardinality = "cardinality";
        public const string Missing = "missing";
        public const string DateHistogram = "date";
        public const string Histogram = "histogram";
        public const string Percentiles = "percentiles";
        public const string GeoHashGrid = "geogrid";
        public const string Terms = "terms";
        public const string Stats = "stats";
        public const string TopHits = "tophits";
        public const string ExtendedStats = "exstats";
    }
}
