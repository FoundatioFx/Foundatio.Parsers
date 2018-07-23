using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;
using System.Collections.Generic;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultAggregationNodeExtensions {

        public static AggregationBase GetDefaultAggregation(this IQueryNode node, IQueryVisitorContext context) {
            if (node is GroupNode groupNode)
                return groupNode.GetDefaultAggregation(context);

            if (node is TermNode termNode)
                return termNode.GetDefaultAggregation(context);

            return null;
        }

        public static AggregationBase GetDefaultAggregation(this GroupNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if (!node.HasParens || String.IsNullOrEmpty(node.Field) || node.Left != null)
                return null;

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field, "keyword");
            var mapping = elasticContext.GetPropertyMapping(field);

            switch (node.GetOperationType()) {
                case AggregationType.DateHistogram:
                    return GetDateHistogramAggregation("date_" + node.GetOriginalField(), field, node.Proximity, node.UnescapedBoost, context);

                case AggregationType.Histogram:
                    return GetHistogramAggregation("histogram_" + node.GetOriginalField(), field, node.Proximity, node.UnescapedBoost, context);

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
                    return new TermsAggregation("terms_" + node.GetOriginalField()) { Field = field, Size = node.GetProximityAsInt32(), MinimumDocumentCount = node.GetBoostAsInt32(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() } } };

                case AggregationType.TopHits:
                    return new TopHitsAggregation("tophits") { Size = node.GetProximityAsInt32() };
            }

            return null;
        }

        public static AggregationBase GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string field = elasticContext.GetNonAnalyzedFieldName(node.Field, "keyword");
            var mapping = elasticContext.GetPropertyMapping(field);
            string timezone = !String.IsNullOrWhiteSpace(node.UnescapedBoost) ? node.UnescapedBoost: GetString(context, "TimeZone");

            switch (node.GetOperationType()) {
                case AggregationType.Min:
                    return new MinAggregation("min_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() }, { "@timezone", timezone } } };

                case AggregationType.Max:
                    return new MaxAggregation("max_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() }, { "@timezone", timezone } } };

                case AggregationType.Avg:
                    return new AverageAggregation("avg_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() } } };

                case AggregationType.Sum:
                    return new SumAggregation("sum_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() } } };

                case AggregationType.Stats:
                    return new StatsAggregation("stats_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() } } };

                case AggregationType.ExtendedStats:
                    return new ExtendedStatsAggregation("exstats_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() } } };

                case AggregationType.Cardinality:
                    return new CardinalityAggregation("cardinality_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble() };

                case AggregationType.TopHits:
                    return new TopHitsAggregation("tophits") { Size = node.GetProximityAsInt32() };

                case AggregationType.Missing:
                    return new MissingAggregation("missing_" + node.GetOriginalField()) { Field = field };

                case AggregationType.DateHistogram:
                    return GetDateHistogramAggregation("date_" + node.GetOriginalField(), field, node.Proximity, node.UnescapedBoost, context);

                case AggregationType.Histogram:
                    return GetHistogramAggregation("histogram_" + node.GetOriginalField(), field, node.Proximity, node.UnescapedBoost, context);

                case AggregationType.Percentiles:
                    return GetPercentilesAggregation("percentiles_" + node.GetOriginalField(), field, node.Proximity, node.UnescapedBoost, context);

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
                    return new TermsAggregation("terms_" + node.GetOriginalField()) { Field = field, Size = node.GetProximityAsInt32(), MinimumDocumentCount = node.GetBoostAsInt32(), Meta = new Dictionary<string, object> { { "@field_type", mapping?.Type?.ToString() } } };
            }

            return null;
        }

        private static AggregationBase GetPercentilesAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context) {
            List<double> percents = null;
            if (!String.IsNullOrWhiteSpace(proximity)) {
                var percentStrings = proximity.Split(',');
                percents = new List<double>();
                foreach  (var ps in percentStrings) {
                    if (double.TryParse(ps, out double outPerc))
                        percents.Add(outPerc);
                }
            }

            return new PercentilesAggregation(originalField, field) {
                Percents = percents
            };
        }

        private static AggregationBase GetHistogramAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context) {
            double interval = 50;
            if (double.TryParse(proximity, out double prox))
                interval = prox;

            return new HistogramAggregation(originalField) {
                Field = field,
                MinimumDocumentCount = 0,
                Interval = interval,
            };
        }

        private static AggregationBase GetDateHistogramAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context) {
            var start = GetDate(context, "StartDate");
            var end = GetDate(context, "EndDate");
            var bounds = start.HasValue && end.HasValue ? new ExtendedBounds<DateMath> { Minimum = start.Value, Maximum = end.Value } : null;

            // TODO: Look into memoizing this lookup
            // TODO: Should we validate the interval range.
            return new DateHistogramAggregation(originalField) {
                Field = field,
                MinimumDocumentCount = 0,
                Format = "date_optional_time",
                Interval = GetInterval(proximity, start, end),
                TimeZone = GetTimeZone(boost, context),
                Meta = !String.IsNullOrEmpty(boost) ? new Dictionary<string, object> { { "@timezone", boost } } : null,
                ExtendedBoundsDateMath = bounds
            };
        }

        private static string GetTimeZone(string boost, IQueryVisitorContext context) {
            TimeSpan? timezoneOffset = null;
            if (boost != null && !Exceptionless.DateTimeExtensions.TimeUnit.TryParse(boost, out timezoneOffset)) {
                // assume if it doesn't parse as time, that it's Olson time
                return boost;
            }

            return timezoneOffset.HasValue ? (timezoneOffset.Value < TimeSpan.Zero ? "-" : "+") + timezoneOffset.Value.ToString("hh\\:mm") : GetString(context, "TimeZone");
        }

        private static Union<DateInterval, Time> GetInterval(string proximity, DateTime? start, DateTime? end) {
            if (String.IsNullOrEmpty(proximity))
                return new Union<DateInterval, Time>(GetInterval(start, end));

            switch (proximity.ToLower().Trim()) {
                case "second":
                    return DateInterval.Second;
                case "minute":
                    return DateInterval.Minute;
                case "hour":
                    return DateInterval.Hour;
                case "day":
                    return DateInterval.Day;
                case "week":
                    return DateInterval.Week;
                case "month":
                    return DateInterval.Month;
                case "quarter":
                    return DateInterval.Quarter;
                case "year":
                    return DateInterval.Year;
                default:
                    return new Union<DateInterval, Time>(proximity);
            }
        }

        private static DateTime? GetDate(IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out object value) && value is DateTime)
                return (DateTime)value;

            return null;
        }

        private static string GetString(IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out object value) && value is string)
                return (string)value;

            return null;
        }

        private static string GetInterval(DateTime? utcStart, DateTime? utcEnd, int desiredDataPoints = 100) {
            if (!utcStart.HasValue || !utcEnd.HasValue)
                return "1d";

            var totalTime = utcEnd.Value - utcStart.Value;
            var timePerBlock = TimeSpan.FromMinutes(totalTime.TotalMinutes / desiredDataPoints);
            if (timePerBlock.TotalDays > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromDays(1));
                return $"{timePerBlock.TotalDays:0}d";
            }

            if (timePerBlock.TotalHours > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromHours(1));
                return $"{timePerBlock.TotalHours:0}h";
            }

            if (timePerBlock.TotalMinutes > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromMinutes(1));
                return $"{timePerBlock.TotalMinutes:0}m";
            }

            timePerBlock = timePerBlock.Round(TimeSpan.FromSeconds(15));
            if (timePerBlock.TotalSeconds < 1)
                timePerBlock = TimeSpan.FromSeconds(15);

            return $"{timePerBlock.TotalSeconds:0}s";
        }

        public static int? GetProximityAsInt32(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out int parsedSize))
                return parsedSize;

            return null;
        }

        public static int? GetBoostAsInt32(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Boost) && Int32.TryParse(node.Boost, out int parsedSize))
                return parsedSize;

            return null;
        }

        public static double? GetProximityAsDouble(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Proximity) && Double.TryParse(node.Proximity, out double parsedSize))
                return parsedSize;

            return null;
        }

        public static double? GetBoostAsDouble(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Boost) && Double.TryParse(node.Boost, out double parsedSize))
                return parsedSize;

            return null;
        }
    }
}