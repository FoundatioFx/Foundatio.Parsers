using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class AssignOperationTypeVisitor: ChainableQueryVisitor {
        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Field))
                return base.VisitAsync(node, context);
            
            if (!(node.Left is TermNode leftTerm) || !String.IsNullOrEmpty(leftTerm.Field))
                throw new FormatException("The first item in an aggregation group must be the name of the target field.");

            if (node.Field.StartsWith("@"))
                return base.VisitAsync(node, context);
            
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

            if (node.Field.StartsWith("@"))
                return;
            
            node.SetOperationType(node.Field);
            node.Field = node.Term;
            node.Term = null;
        }
        
        private bool IsKnownAggregationType(string type) {
            switch (type) {
                case AggregationType.Min:
                case AggregationType.Max:
                case AggregationType.Avg:
                case AggregationType.Sum:
                case AggregationType.Cardinality:
                case AggregationType.Missing:
                case AggregationType.DateHistogram:
                case AggregationType.Histogram:
                case AggregationType.Percentiles:
                case AggregationType.GeoHashGrid:
                case AggregationType.Terms:
                case AggregationType.Stats:
                case AggregationType.TopHits:
                case AggregationType.ExtendedStats:
                    return true;
                default:
                    return false;
            }
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
