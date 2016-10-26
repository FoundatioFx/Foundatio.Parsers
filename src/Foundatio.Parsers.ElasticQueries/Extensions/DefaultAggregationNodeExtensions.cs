using System;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultAggregationNodeExtensions {
        public static AggregationBase GetDefaultAggregation(this IQueryNode node, IQueryVisitorContext context) {
            var groupNode = node as GroupNode;
            if (groupNode != null)
                return groupNode.GetDefaultAggregation(context);

            var termNode = node as TermNode;
            if (termNode != null)
                return termNode.GetDefaultAggregation(context);

            return null;
        }

        public static AggregationBase GetDefaultAggregation(this GroupNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if (!node.HasParens || String.IsNullOrEmpty(node.Field) || node.Left != null)
                return null;

            switch (node.GetAggregationType()) {
                case AggregationType.DateHistogram:
                    return new DateHistogramAggregation("date_" + node.GetUnaliasedField()) {
                        Field = node.Field,
                        Interval = new Union<DateInterval, Time>(node.Proximity ?? "1d"),
                        Format = "date_optional_time",
                        Offset = node.UnescapedBoost
                    };
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + node.GetUnaliasedField()) {
                        Field = node.Field,
                        Precision = precision,
                        Aggregations = new AverageAggregation("avg_lat", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lat")
                        } && new AverageAggregation("avg_lon", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lon")
                        }
                    };
                case AggregationType.Terms:
                    int? size = null;
                    int parsedSize;
                    if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                        size = parsedSize;

                    return new TermsAggregation("terms_" + node.GetUnaliasedField()) { Field = node.Field, Size = size };
            }

            return null;
        }

        public static AggregationBase GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            switch (node.GetAggregationType()) {
                case AggregationType.Min:
                    return new MinAggregation("min_" + node.GetUnaliasedField(), node.Field);
                case AggregationType.Max:
                    return new MaxAggregation("max_" + node.GetUnaliasedField(), node.Field);
                case AggregationType.Avg:
                    return new AverageAggregation("avg_" + node.GetUnaliasedField(), node.Field);
                case AggregationType.Sum:
                    return new SumAggregation("sum_" + node.GetUnaliasedField(), node.Field);
                case AggregationType.Cardinality:
                    return new CardinalityAggregation("cardinality_" + node.GetUnaliasedField(), node.Field);
                case AggregationType.Missing:
                    return new MissingAggregation("missing_" + node.GetUnaliasedField()) { Field = node.Field };
                case AggregationType.DateHistogram:
                    return new DateHistogramAggregation("date_" + node.GetUnaliasedField()) {
                        Field = node.Field,
                        Interval = new Union<DateInterval, Time>(node.Proximity ?? "1d"),
                        Format = "date_optional_time",
                        Offset = node.UnescapedBoost
                    };
                case AggregationType.Percentiles:
                    return new PercentilesAggregation("percentiles_" + node.GetUnaliasedField(), node.Field);
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + node.GetUnaliasedField()) {
                        Field = node.Field,
                        Precision = precision,
                        Aggregations = new AverageAggregation("avg_lat", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lat")
                        } && new AverageAggregation("avg_lon", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lon")
                        }
                    };
                case AggregationType.Terms:
                    int? size = null;
                    int parsedSize;
                    if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                        size = parsedSize;

                    return new TermsAggregation("terms_" + node.GetUnaliasedField()) { Field = node.Field, Size = size };
            }

            return null;
        }
    }
}
