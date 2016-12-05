using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class AssignAggregationTypeVisitor: ChainableQueryVisitor {
        public override Task VisitAsync(GroupNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field)) {
                var leftTerm = node.Left as TermNode;
                if (leftTerm == null || !String.IsNullOrEmpty(leftTerm.Field))
                    throw new ApplicationException("The first item in an aggregation group must be the name of the target field.");

                if (IsKnownAggregationType(node.Field)) {
                    node.SetAggregationType(node.Field);
                    node.Field = leftTerm.Term;
                    node.Boost = leftTerm.Boost;
                    node.Proximity = leftTerm.Proximity;
                    node.Left = null;
                }
            }

            return base.VisitAsync(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Term))
                return;

            if (IsKnownAggregationType(node.Field)) {
                node.SetAggregationType(node.Field);
                node.Field = node.Term;
                node.Term = null;
            }
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
                case AggregationType.Percentiles:
                case AggregationType.GeoHashGrid:
                case AggregationType.Terms:
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
        public const string Percentiles = "percentiles";
        public const string GeoHashGrid = "geogrid";
        public const string Terms = "terms";
    }
}
