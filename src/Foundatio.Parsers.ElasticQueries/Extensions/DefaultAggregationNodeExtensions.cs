using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class DefaultAggregationNodeExtensions {
        // NOTE: We may want to read this dynamically from server settings.
        public const int MAX_BUCKET_SIZE = 10000;
        
        public static Task<AggregationBase> GetDefaultAggregationAsync(this IQueryNode node, IQueryVisitorContext context) {
            if (node is GroupNode groupNode)
                return groupNode.GetDefaultAggregationAsync(context);

            if (node is TermNode termNode)
                return termNode.GetDefaultAggregationAsync(context);

            return null;
        }

        public static async Task<AggregationBase> GetDefaultAggregationAsync(this GroupNode node, IQueryVisitorContext context) {
            if (context is not IElasticQueryVisitorContext elasticContext)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            if (!node.HasParens || String.IsNullOrEmpty(node.Field) || node.Left != null)
                return null;

            string field = elasticContext.MappingResolver.GetAggregationsFieldName(node.UnescapedField);
            var property = elasticContext.MappingResolver.GetMappingProperty(field, true);
            string originalField = node.GetOriginalField().Unescape();

            switch (node.GetOperationType()) {
                case AggregationType.DateHistogram:
                    return GetDateHistogramAggregation("date_" + originalField, field, node.UnescapedProximity, node.UnescapedBoost ?? node.GetTimeZone(await elasticContext.GetTimeZoneAsync()), context);

                case AggregationType.Histogram:
                    return GetHistogramAggregation("histogram_" + originalField, field, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.UnescapedProximity))
                        Enum.TryParse(node.UnescapedProximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + originalField) {
                        Field = field,
                        Precision = precision,
                        Aggregations = new AverageAggregation("avg_lat", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lat")
                        } && new AverageAggregation("avg_lon", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lon")
                        }
                    };

                case AggregationType.Terms:
                    var agg = new TermsAggregation("terms_" + originalField) {
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

        public static async Task<AggregationBase> GetDefaultAggregationAsync(this TermNode node, IQueryVisitorContext context) {
            if (context is not IElasticQueryVisitorContext elasticContext)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            string aggField = elasticContext.MappingResolver.GetAggregationsFieldName(node.UnescapedField);
            var property = elasticContext.MappingResolver.GetMappingProperty(node.UnescapedField, true);
            string timezone = !String.IsNullOrWhiteSpace(node.UnescapedBoost) ? node.UnescapedBoost: node.GetTimeZone(await elasticContext.GetTimeZoneAsync());
            string originalField = node.GetOriginalField().Unescape();

            switch (node.GetOperationType()) {
                case AggregationType.Min:
                    return new MinAggregation("min_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type }, { "@timezone", timezone } } };

                case AggregationType.Max:
                    return new MaxAggregation("max_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type }, { "@timezone", timezone } } };

                case AggregationType.Avg:
                    return new AverageAggregation("avg_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.Sum:
                    return new SumAggregation("sum_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.Stats:
                    return new StatsAggregation("stats_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.ExtendedStats:
                    return new ExtendedStatsAggregation("exstats_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), Meta = new Dictionary<string, object> { { "@field_type", property?.Type } } };

                case AggregationType.Cardinality:
                    return new CardinalityAggregation("cardinality_" + originalField, aggField) { Missing = node.GetProximityAsDouble(), PrecisionThreshold = node.GetBoostAsInt32() };

                case AggregationType.TopHits:
                    return new TopHitsAggregation("tophits") { Size = node.GetProximityAsInt32() };

                case AggregationType.Missing:
                    return new MissingAggregation("missing_" + originalField) { Field = aggField };

                case AggregationType.DateHistogram:
                    return GetDateHistogramAggregation("date_" + originalField, aggField, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.Histogram:
                    return GetHistogramAggregation("histogram_" + originalField, aggField, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.Percentiles:
                    return GetPercentilesAggregation("percentiles_" + originalField, aggField, node.UnescapedProximity, node.UnescapedBoost, context);

                case AggregationType.GeoHashGrid:
                    var precision = GeoHashPrecision.Precision1;
                    if (!String.IsNullOrEmpty(node.UnescapedProximity))
                        Enum.TryParse(node.UnescapedProximity, out precision);

                    return new GeoHashGridAggregation("geogrid_" + originalField) {
                        Field = aggField,
                        Precision = precision,
                        Aggregations = new AverageAggregation("avg_lat", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lat")
                        } && new AverageAggregation("avg_lon", null) {
                            Script = new InlineScript($"doc['{node.Field}'].lon")
                        }
                    };

                case AggregationType.Terms:
                    var agg = new TermsAggregation("terms_" + originalField) {
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

            return proximity.Trim() switch {
                "s" or "1s" or "second" => DateInterval.Second,
                "m" or "1m" or "minute" => DateInterval.Minute,
                "h" or "1h" or "hour" => DateInterval.Hour,
                "d" or "1d" or "day" => DateInterval.Day,
                "w" or "1w" or "week" => DateInterval.Week,
                "M" or "1M" or "month" => DateInterval.Month,
                "q" or "1q" or "quarter" => DateInterval.Quarter,
                "y" or "1y" or "year" => DateInterval.Year,
                _ => new Union<DateInterval, Time>(proximity),
            };
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