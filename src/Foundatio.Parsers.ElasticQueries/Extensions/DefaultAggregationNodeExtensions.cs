using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultAggregationNodeExtensions {
        public static AggregationContainer GetDefaultAggregation(this IQueryNode node, IQueryVisitorContext context) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                return groupNode.GetDefaultAggregation(context);

            var termNode = node as TermNode;
            if (termNode != null)
                return termNode.GetDefaultAggregation(context);

            return null;
        }

        public static AggregationContainer GetDefaultAggregation(this GroupNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if (!node.HasParens || String.IsNullOrEmpty(node.Field) || node.Left != null)
                return null;

            switch (node.GetAggregationType()) {
                case AggregationType.DateHistogram:
                    return new AggregationContainer {
                        DateHistogram = new DateHistogramAggregator {
                            Field = node.Field,
                            Interval = node.Proximity,
                            Offset = node.UnescapedBoost
                        }
                    };
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new AggregationContainer {
                        GeoHash = new GeoHashAggregator {
                            Field = node.Field,
                            Precision = precision
                        }
                    };
            }

            return null;
        }

        public static AggregationContainer GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            switch (node.GetAggregationType()) {
                case AggregationType.Min:
                    return new AggregationContainer { Min = new MinAggregator { Field = node.Field } };
                case AggregationType.Max:
                    return new AggregationContainer { Max = new MaxAggregator { Field = node.Field } };
            }

            return null;
        }
    }
}
