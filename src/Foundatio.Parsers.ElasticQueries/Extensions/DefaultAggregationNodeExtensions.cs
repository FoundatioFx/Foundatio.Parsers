using System;
using System.Collections.Generic;
using Exceptionless.DateTimeExtensions;
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

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field);
            var mapping = elasticContext.GetPropertyMapping(field);

            switch (node.GetAggregationType()) {
                case AggregationType.DateHistogram:
                    // TODO: Look into memoizing this lookup
                    TimeSpan? timezone = node.UnescapedBoost != null ? Exceptionless.DateTimeExtensions.TimeUnit.Parse(node.UnescapedBoost) : (TimeSpan?)null;
                    return new DateHistogramAggregation("date_" + node.GetOriginalField()) {
                        Field = field,
                        Interval = new Union<DateInterval, Time>(node.Proximity ?? "1d"),
                        Format = "date_optional_time",
                        TimeZone = timezone.HasValue ? (timezone.Value < TimeSpan.Zero ? "-" : "+") + timezone.Value.ToString("hh\\:mm") : null,
                        Meta = !String.IsNullOrEmpty(node.UnescapedBoost) ? new Dictionary<string, object> { { "@offset", node.UnescapedBoost } } : null
                    };
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + node.GetOriginalField()) {
                        Field = field,
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

                    return new TermsAggregation("terms_" + node.GetOriginalField()) { Field = field, Size = size, Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() } } };
            }

            return null;
        }

        public static AggregationBase GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field);
            var mapping = elasticContext.GetPropertyMapping(field);

            switch (node.GetAggregationType()) {
                case AggregationType.Min:
                    return new MinAggregation("min_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() }, { "@offset", node.UnescapedBoost } } };
                case AggregationType.Max:
                    return new MaxAggregation("max_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() }, { "@offset", node.UnescapedBoost } } };
                case AggregationType.Avg:
                    return new AverageAggregation("avg_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() } } };
                case AggregationType.Sum:
                    return new SumAggregation("sum_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() } } };
                case AggregationType.Stats:
                    return new StatsAggregation("stats_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() } } };
                case AggregationType.ExtendedStats:
                    return new ExtendedStatsAggregation("exstats_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() } } };
                case AggregationType.Cardinality:
                    return new CardinalityAggregation("cardinality_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble() };
                case AggregationType.Missing:
                    return new MissingAggregation("missing_" + node.GetOriginalField()) { Field = field };
                case AggregationType.DateHistogram:
                    // TODO: Look into memoizing this lookup
                    TimeSpan? timezone = node.UnescapedBoost != null ? Exceptionless.DateTimeExtensions.TimeUnit.Parse(node.UnescapedBoost) : (TimeSpan?)null;
                    return new DateHistogramAggregation("date_" + node.GetOriginalField()) {
                        Field = field,
                        Interval = new Union<DateInterval, Time>(node.Proximity ?? "1d"),
                        Format = "date_optional_time",
                        TimeZone = timezone.HasValue ? (timezone.Value < TimeSpan.Zero ? "-" : "+") + timezone.Value.ToString("hh\\:mm") : null,
                        Meta = !String.IsNullOrEmpty(node.UnescapedBoost) ? new Dictionary <string, object> { { "@offset", node.UnescapedBoost } } : null
                    };
                case AggregationType.Percentiles:
                    return new PercentilesAggregation("percentiles_" + node.GetOriginalField(), field);
                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.Proximity))
                        Enum.TryParse(node.Proximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + node.GetOriginalField()) {
                        Field = field,
                        Precision = precision,
                        Aggregations = new AverageAggregation("avg_lat", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lat")
                        } && new AverageAggregation("avg_lon", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lon")
                        }
                    };
                case AggregationType.Terms:
                    return new TermsAggregation("terms_" + node.GetOriginalField()) { Field = field, Size = node.GetProximityAsInt32(), Meta = new Dictionary<string, object> { { "@type", mapping?.Type?.ToString() } } };
            }

            return null;
        }

        public static int? GetProximityAsInt32(this TermNode node) {
            int parsedSize;
            if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out parsedSize))
                return parsedSize;

            return null;
        }

        public static double? GetProximityAsDouble(this TermNode node) {
            double parsedSize;
            if (!String.IsNullOrEmpty(node.Proximity) && Double.TryParse(node.Proximity, out parsedSize))
                return parsedSize;

            return null;
        }
    }
}