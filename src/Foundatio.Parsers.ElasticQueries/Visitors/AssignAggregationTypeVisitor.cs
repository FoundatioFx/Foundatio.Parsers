using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class AssignAggregationTypeVisitor: ChainableQueryVisitor {
        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field)) {
                var leftTerm = node.Left as TermNode;
                if (leftTerm == null || !String.IsNullOrEmpty(leftTerm.Field))
                    throw new ApplicationException("The first item in an aggregation group must be the name of the target field.");

                node.Field = leftTerm.Term;
                node.Boost = leftTerm.Boost;
                node.Proximity = leftTerm.Proximity;
                node.Left = null;
                node.SetAggregationType(GetAggregationType(node.Field));
            }

            base.Visit(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (String.IsNullOrEmpty(node.Term))
                return;

            node.SetAggregationType(GetAggregationType(node.Field));
            node.Field = node.Term;
            node.Term = null;
        }

        private AggregationType GetAggregationType(string aggregationType) {
            switch (aggregationType.ToLower()) {
                case "min": return AggregationType.Min;
                case "max": return AggregationType.Max;
                case "avg": return AggregationType.Avg;
                case "sum": return AggregationType.Sum;
                case "cardinality": return AggregationType.Cardinality;
                case "missing": return AggregationType.Missing;
                case "date": return AggregationType.DateHistogram;
                case "percentiles": return AggregationType.Percentiles;
                case "geogrid": return AggregationType.GeoHashGrid;
                case "terms": return AggregationType.Terms;
            }

            return AggregationType.None;
        }
    }

    public enum AggregationType {
        None = 0,
        Min = 1,
        Max = 2,
        Avg = 3,
        Sum = 4,
        Cardinality = 5,
        Missing = 6,
        DateHistogram = 7,
        Percentiles = 8,
        GeoHashGrid = 9,
        Terms = 10
    }
}
