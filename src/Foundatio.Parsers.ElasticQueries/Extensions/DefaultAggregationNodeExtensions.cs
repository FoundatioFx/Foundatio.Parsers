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
            var property = elasticContext.GetPropertyMapping(field);

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
                    return new TermsAggregation("terms_" + node.GetOriginalField()) { Field = field, Size = node.GetProximityAsInt32(), MinimumDocumentCount = node.GetBoostAsInt32(), Meta = new Dictionary<string, object> { { "@field_type", property.Mapping?.Type } } };

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
            string timezone = !String.IsNullOrWhiteSpace(node.UnescapedBoost) ? node.UnescapedBoost: elasticContext.DefaultTimeZone;

            switch (node.GetOperationType()) {
                case AggregationType.Min:
                    return new MinAggregation("min_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type }, { "@timezone", timezone } } };

                case AggregationType.Max:
                    return new MaxAggregation("max_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type }, { "@timezone", timezone } } };

                case AggregationType.Avg:
                    return new AverageAggregation("avg_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type } } };

                case AggregationType.Sum:
                    return new SumAggregation("sum_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type } } };

                case AggregationType.Stats:
                    return new StatsAggregation("stats_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type } } };

                case AggregationType.ExtendedStats:
                    return new ExtendedStatsAggregation("exstats_" + node.GetOriginalField(), field) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type } } };

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
                    return new TermsAggregation("terms_" + node.GetOriginalField()) { Field = field, Size = node.GetProximityAsInt32(), MinimumDocumentCount = node.GetBoostAsInt32(), Meta = new Dictionary<string, object> { { "@field_type", mapping.Mapping?.Type } } };
            }

            return null;
        }

        private static AggregationBase GetPercentilesAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context) {
            List<double> percents = null;
            if (!String.IsNullOrWhiteSpace(proximity)) {
                var percentStrings = proximity.Split(',');
                percents = new List<double>();
                foreach  (string ps in percentStrings) {
                    if (Double.TryParse(ps, out var percent))
                        percents.Add(percent);
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
            // NOTE: StartDate and EndDate are set in the Repositories QueryBuilderContext.
            var start = GetDate(context, "StartDate");
            var end = GetDate(context, "EndDate");
            var bounds = start.HasValue && end.HasValue ? new ExtendedBounds<DateMath> { Minimum = start.Value, Maximum = end.Value } : null;

            var interval = GetInterval(proximity, start, end);
            var agg = new DateHistogramAggregation(originalField) {
                Field = field,
                MinimumDocumentCount = 0,
                Format = "date_optional_time",
                TimeZone = GetTimeZone(boost, context),
                Meta = !String.IsNullOrEmpty(boost) ? new Dictionary<string, object> { { "@timezone", boost } } : null,
                ExtendedBounds = bounds
            };

            interval.Match(d => agg.CalendarInterval = d, f => agg.FixedInterval = f);

            return agg;
        }

        private static string GetTimeZone(string boost, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;

            TimeSpan? timezoneOffset = null;
            if (boost != null && !Exceptionless.DateTimeExtensions.TimeUnit.TryParse(boost, out timezoneOffset)) {
                // assume if it doesn't parse as time, that it's Olson time
                return boost;
            }

            return timezoneOffset.HasValue ? (timezoneOffset.Value < TimeSpan.Zero ? "-" : "+") + timezoneOffset.Value.ToString("hh\\:mm") : elasticContext?.DefaultTimeZone;
        }

        private static Union<DateInterval, Time> GetInterval(string proximity, DateTime? start, DateTime? end) {
            if (String.IsNullOrEmpty(proximity))
                return GetInterval(start, end);

            switch (proximity.Trim()) {
                case "s":
                case "1s":
                case "second":
                    return DateInterval.Second;
                case "m":
                case "1m":
                case "minute":
                    return DateInterval.Minute;
                case "h":
                case "1h":
                case "hour":
                    return DateInterval.Hour;
                case "d":
                case "1d":
                case "day":
                    return DateInterval.Day;
                case "w":
                case "1w":
                case "week":
                    return DateInterval.Week;
                case "M":
                case "1M":
                case "month":
                    return DateInterval.Month;
                case "q":
                case "1q":
                case "quarter":
                    return DateInterval.Quarter;
                case "y":
                case "1y":
                case "year":
                    return DateInterval.Year;
            }

            return new Union<DateInterval, Time>(proximity);
        }

        private static DateTime? GetDate(IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out var value) && value is DateTime date)
                return date;

            return null;
        }

        private static string GetString(IQueryVisitorContext context, string key) {
            if (context.Data.TryGetValue(key, out var value) && value is string str)
                return str;

            return null;
        }

        private static Union<DateInterval, Time> GetInterval(DateTime? utcStart, DateTime? utcEnd, int desiredDataPoints = 100) {
            if (!utcStart.HasValue || !utcEnd.HasValue)
                return DateInterval.Day;

            var totalTime = utcEnd.Value - utcStart.Value;
            var timePerBlock = TimeSpan.FromMinutes(totalTime.TotalMinutes / desiredDataPoints);
            if (timePerBlock.TotalDays > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromDays(1));
                if (timePerBlock.TotalDays > 365)
                    timePerBlock = TimeSpan.FromDays(365);
                return (Time)timePerBlock;
            }

            if (timePerBlock.TotalHours > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromHours(1));
                return (Time)timePerBlock;
            }

            if (timePerBlock.TotalMinutes > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromMinutes(1));
                return (Time)timePerBlock;
            }

            timePerBlock = timePerBlock.Round(TimeSpan.FromSeconds(15));
            if (timePerBlock.TotalSeconds < 1)
                timePerBlock = TimeSpan.FromSeconds(15);

            return (Time)timePerBlock;
        }

        public static int? GetProximityAsInt32(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Proximity) && Int32.TryParse(node.Proximity, out var parsedSize))
                return parsedSize;

            return null;
        }

        public static int? GetBoostAsInt32(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Boost) && Int32.TryParse(node.Boost, out var parsedSize))
                return parsedSize;

            return null;
        }

        public static double? GetProximityAsDouble(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Proximity) && Double.TryParse(node.Proximity, out var parsedSize))
                return parsedSize;

            return null;
        }

        public static double? GetBoostAsDouble(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.Boost) && Double.TryParse(node.Boost, out var parsedSize))
                return parsedSize;

            return null;
        }
    }
}