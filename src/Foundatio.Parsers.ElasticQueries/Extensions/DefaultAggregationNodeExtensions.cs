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
        // NOTE: We may want to read this dynamically from server settings.
        public const int MAX_BUCKET_SIZE = 10000;
        
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

            string field = elasticContext.MappingResolver.GetAggregationsFieldName(node.Field);
            var property = elasticContext.MappingResolver.GetMappingProperty(field, true);

            switch (node.GetOperationType()) {
                case AggregationType.DateHistogram:
                    return GetDateHistogramAggregation("date_" + node.GetOriginalField(), field, node.UnescapedProximity, node.UnescapedBoost ?? node.GetTimeZone(elasticContext.DefaultTimeZone), context);

                case AggregationType.Histogram:
                    return GetHistogramAggregation("histogram_" + node.GetOriginalField(), field, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.UnescapedProximity))
                        Enum.TryParse(node.UnescapedProximity, out precision);

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
                    var agg = new TermsAggregation("terms_" + node.GetOriginalField()) {
                        Field = field, 
                        Size = node.GetProximityAsInt32(), 
                        MinimumDocumentCount = node.GetBoostAsInt32(), 
                        Meta = new Dictionary<string, object> { { "@field_type", property?.Type } }
                    };
                    
                    if (agg.Size.HasValue && (agg.Size * 1.5 + 10) > MAX_BUCKET_SIZE)
                        agg.ShardSize = Math.Max((int)agg.Size, MAX_BUCKET_SIZE);

                    return agg;

                case AggregationType.TopHits:
                    return new TopHitsAggregation("tophits") { Size = node.GetProximityAsInt32() };
            }

            return null;
        }

        public static AggregationBase GetDefaultAggregation(this TermNode node, IQueryVisitorContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string aggField = elasticContext.MappingResolver.GetAggregationsFieldName(node.Field);
            var property = elasticContext.MappingResolver.GetMappingProperty(node.Field, true);
            string timezone = !String.IsNullOrWhiteSpace(node.UnescapedBoost) ? node.UnescapedBoost: node.GetTimeZone(elasticContext.DefaultTimeZone);

            switch (node.GetOperationType()) {
                case AggregationType.Min:
                    return new MinAggregation("min_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type }, { "@timezone", timezone } } };

                case AggregationType.Max:
                    return new MaxAggregation("max_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type }, { "@timezone", timezone } } };

                case AggregationType.Avg:
                    return new AverageAggregation("avg_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.Sum:
                    return new SumAggregation("sum_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.Stats:
                    return new StatsAggregation("stats_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.ExtendedStats:
                    return new ExtendedStatsAggregation("exstats_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.Cardinality:
                    return new CardinalityAggregation("cardinality_" + node.GetOriginalField(), aggField) { Missing = node.GetProximityAsDouble(), PrecisionThreshold = node.GetBoostAsInt32() };

                case AggregationType.TopHits:
                    return new TopHitsAggregation("tophits") { Size = node.GetProximityAsInt32() };

                case AggregationType.Missing:
                    return new MissingAggregation("missing_" + node.GetOriginalField()) { Field = aggField };

                case AggregationType.DateHistogram:
                    return GetDateHistogramAggregation("date_" + node.GetOriginalField(), aggField, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.Histogram:
                    return GetHistogramAggregation("histogram_" + node.GetOriginalField(), aggField, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.Percentiles:
                    return GetPercentilesAggregation("percentiles_" + node.GetOriginalField(), aggField, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.UnescapedProximity))
                        Enum.TryParse(node.UnescapedProximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + node.GetOriginalField()) {
                        Field = aggField,
                        Precision = precision,
                        Aggregations = new AverageAggregation("avg_lat", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lat")
                        } && new AverageAggregation("avg_lon", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lon")
                        }
                    };

                case AggregationType.Terms:
                    var agg = new TermsAggregation("terms_" + node.GetOriginalField()) {
                        Field = aggField, 
                        Size = node.GetProximityAsInt32(), 
                        MinimumDocumentCount = node.GetBoostAsInt32(), 
                        Meta = new Dictionary<string, object> { { "@field_type", property?.Type } }
                    };

                    if (agg.Size.HasValue && (agg.Size * 1.5 + 10) > MAX_BUCKET_SIZE)
                        agg.ShardSize = Math.Max((int)agg.Size, MAX_BUCKET_SIZE);

                    return agg;
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
            if (Double.TryParse(proximity, out double prox))
                interval = prox;

            return new HistogramAggregation(originalField) {
                Field = field,
                MinimumDocumentCount = 0,
                Interval = interval
            };
        }

        private static AggregationBase GetDateHistogramAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context) {
            // NOTE: StartDate and EndDate are set in the Repositories QueryBuilderContext.
            var start = context.GetDate("StartDate");
            var end = context.GetDate("EndDate");
            bool isValidRange = start.HasValue && start.Value > DateTime.MinValue && end.HasValue && end.Value < DateTime.MaxValue && start.Value <= end.Value;
            var bounds = isValidRange ? new ExtendedBounds<DateMath> { Minimum = start.Value, Maximum = end.Value } : null;

            var interval = GetInterval(proximity, start, end);
            string timezone = TryConvertTimeUnitToUtcOffset(boost);
            var agg = new DateHistogramAggregation(originalField) {
                Field = field,
                MinimumDocumentCount = 0,
                Format = "date_optional_time",
                TimeZone = timezone,
                Meta = !String.IsNullOrEmpty(boost) ? new Dictionary<string, object> { { "@timezone", boost } } : null,
                ExtendedBounds = bounds
            };

            interval.Match(d => agg.CalendarInterval = d, f => agg.FixedInterval = f);
            return agg;
        }

        private static string TryConvertTimeUnitToUtcOffset(string boost) {
            if (String.IsNullOrEmpty(boost))
                return null;
            
            if (!Exceptionless.DateTimeExtensions.TimeUnit.TryParse(boost, out var timezoneOffset)) {
                // if it fails to parse, just return it unmodified
                return boost;
            }

            if (!timezoneOffset.HasValue)
                return null;

            if (timezoneOffset.Value < TimeSpan.Zero)
                return "-" + timezoneOffset.Value.ToString("hh\\:mm");

            return "+" + timezoneOffset.Value.ToString("hh\\:mm");
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

        private static Union<DateInterval, Time> GetInterval(DateTime? utcStart, DateTime? utcEnd, int desiredDataPoints = 100) {
            if (!utcStart.HasValue || !utcEnd.HasValue || utcStart.Value == DateTime.MinValue)
                return DateInterval.Day;

            var totalTime = utcEnd.Value - utcStart.Value;
            var timePerBlock = TimeSpan.FromMinutes(totalTime.TotalMinutes / desiredDataPoints);
            if (timePerBlock.TotalDays > 1) {
                timePerBlock = timePerBlock.Round(TimeSpan.FromDays(1));
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
            if (!String.IsNullOrEmpty(node.UnescapedProximity) && Int32.TryParse(node.UnescapedProximity, out var parsedValue))
                return parsedValue;

            return null;
        }

        public static int? GetBoostAsInt32(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.UnescapedBoost) && Int32.TryParse(node.UnescapedBoost, out var parsedValue))
                return parsedValue;

            return null;
        }

        public static double? GetProximityAsDouble(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.UnescapedProximity) && Double.TryParse(node.UnescapedProximity, out var parsedValue))
                return parsedValue;

            return null;
        }

        public static double? GetBoostAsDouble(this IFieldQueryWithProximityAndBoostNode node) {
            if (!String.IsNullOrEmpty(node.UnescapedBoost) && Double.TryParse(node.UnescapedBoost, out var parsedValue))
                return parsedValue;

            return null;
        }
    }
}