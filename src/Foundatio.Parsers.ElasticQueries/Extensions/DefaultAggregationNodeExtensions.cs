using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Extensions;

public static class DefaultAggregationNodeExtensions
{
    // NOTE: We may want to read this dynamically from server settings.
    public const int MAX_BUCKET_SIZE = 10000;

    public static async Task<AggregationMap> GetDefaultAggregationAsync(this IQueryNode node, IQueryVisitorContext context)
    {
        AggregationMap aggregation = null;
        if (node is GroupNode groupNode)
            aggregation = await groupNode.GetDefaultAggregationAsync(context);

        if (node is TermNode termNode)
            aggregation = await termNode.GetDefaultAggregationAsync(context);

        if (aggregation is null)
            return null;

        if (aggregation is TermsAggregation termsAggregation)
        {
            PopulateTermsAggregation(termsAggregation, node);
        }

        if (aggregation is TopHitsAggregation topHitsAggregation)
        {
            PopulateTopHitsAggregation(topHitsAggregation, node);
        }

        if (aggregation is DateHistogramAggregation histogramAggregation)
        {
            PopulateDateHistogramAggregation(histogramAggregation, node);
        }

        return aggregation;
    }

    public static async Task<AggregationMap> GetDefaultAggregationAsync(this GroupNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        if (!node.HasParens || String.IsNullOrEmpty(node.Field) || node.Left != null)
            return null;

        string field = elasticContext.MappingResolver.GetAggregationsFieldName(node.UnescapedField);
        var property = elasticContext.MappingResolver.GetMappingProperty(field, true);
        string originalField = node.GetOriginalField().Unescape();

        switch (node.GetOperationType())
        {
            case AggregationType.DateHistogram:
                return GetDateHistogramAggregation($"date_{originalField}", field, node.UnescapedProximity, node.UnescapedBoost ?? node.GetTimeZone(await elasticContext.GetTimeZoneAsync()), context);

            case AggregationType.Histogram:
                return GetHistogramAggregation($"histogram_{originalField}", field, node.UnescapedProximity, node.UnescapedBoost, context);

            case AggregationType.GeoHashGrid:
                var precision = new GeohashPrecision(1);
                if (!String.IsNullOrEmpty(node.UnescapedProximity) && Int64.TryParse(node.UnescapedProximity, out long parsedPrecision))
                {
                    if (parsedPrecision is < 1 or > 12)
                        throw new ArgumentOutOfRangeException(nameof(node.UnescapedProximity), "Precision must be between 1 and 12");

                    precision = new GeohashPrecision(parsedPrecision);
                }

                return new AggregationMap($"geogrid_{originalField}", new GeohashGridAggregation { Field = field, Precision = precision })
                {
                    Aggregations =
                    {
                        new AggregationMap("avg_lat", new AverageAggregation { Script = new Script { Source = $"doc['{node.Field}'].lat" } }),
                        new AggregationMap("avg_lon", new AverageAggregation { Script = new Script { Source = $"doc['{node.Field}'].lon" } })
                    }
                };

            case AggregationType.Terms:
                var termsAggregation = new TermsAggregation
                {
                    Field = field,
                    Size = node.GetProximityAsInt32(),
                    MinDocCount = node.GetBoostAsInt32()
                };

                if (termsAggregation.Size.HasValue && (termsAggregation.Size * 1.5 + 10) > MAX_BUCKET_SIZE)
                    termsAggregation.ShardSize = Math.Max((int)termsAggregation.Size, MAX_BUCKET_SIZE);

                return new AggregationMap($"terms_{originalField}", termsAggregation)
                {
                    Meta = { { "@field_type", property?.Type } }
                };

            case AggregationType.TopHits:
                return new AggregationMap("tophits", new TopHitsAggregation { Size = node.GetProximityAsInt32() });
        }

        return null;
    }

    public static async Task<AggregationMap> GetDefaultAggregationAsync(this TermNode node, IQueryVisitorContext context)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        string aggField = elasticContext.MappingResolver.GetAggregationsFieldName(node.UnescapedField);
        var property = elasticContext.MappingResolver.GetMappingProperty(node.UnescapedField, true);
        string timezone = !String.IsNullOrWhiteSpace(node.UnescapedBoost) ? node.UnescapedBoost : node.GetTimeZone(await elasticContext.GetTimeZoneAsync());
        string originalField = node.GetOriginalField().Unescape();

        switch (node.GetOperationType())
        {
            case AggregationType.Min:
                return new AggregationMap($"min_{originalField}", new MinAggregation { Field = aggField, Missing = node.GetProximityAsDouble() })
                {
                    Meta = { { "@field_type", property?.Type }, { "@timezone", timezone } }
                };

            case AggregationType.Max:
                return new AggregationMap($"max_{originalField}", new MaxAggregation { Field = aggField, Missing = node.GetProximityAsDouble() })
                {
                    Meta = { { "@field_type", property?.Type }, { "@timezone", timezone } }
                };

            case AggregationType.Avg:
                return new AggregationMap($"avg_{originalField}", new AverageAggregation { Field = aggField, Missing = node.GetProximityAsDouble() })
                {
                    Meta = { { "@field_type", property?.Type } }
                };

            case AggregationType.Sum:
                return new AggregationMap($"sum_{originalField}", new SumAggregation { Field = aggField, Missing = node.GetProximityAsDouble() })
                {
                    Meta = { { "@field_type", property?.Type } }
                };

            case AggregationType.Stats:
                return new AggregationMap($"stats_{originalField}", new StatsAggregation { Field = aggField, Missing = node.GetProximityAsDouble() })
                {
                    Meta = { { "@field_type", property?.Type } }
                };

            case AggregationType.ExtendedStats:
                return new AggregationMap($"exstats_{originalField}", new ExtendedStatsAggregation { Field = aggField, Missing = node.GetProximityAsDouble() })
                {
                    Meta = { { "@field_type", property?.Type } }
                };

            case AggregationType.Cardinality:
                return new AggregationMap($"cardinality_{originalField}", new CardinalityAggregation
                {
                    Field = aggField,
                    Missing = node.GetProximityAsDouble(),
                    PrecisionThreshold = node.GetBoostAsInt32()
                });

            case AggregationType.TopHits:
                return new AggregationMap("tophits", new TopHitsAggregation { Size = node.GetProximityAsInt32() });

            case AggregationType.Missing:
                return new AggregationMap($"missing_{originalField}", new MissingAggregation { Field = aggField });

            case AggregationType.DateHistogram:
                return GetDateHistogramAggregation($"date_{originalField}", aggField, node.UnescapedProximity, node.UnescapedBoost, context);

            case AggregationType.Histogram:
                return GetHistogramAggregation($"histogram_{originalField}", aggField, node.UnescapedProximity, node.UnescapedBoost, context);

            case AggregationType.Percentiles:
                return GetPercentilesAggregation($"percentiles_{originalField}", aggField, node.UnescapedProximity, node.UnescapedBoost, context);

            case AggregationType.GeoHashGrid:
                var precision = new GeohashPrecision(1);
                if (!String.IsNullOrEmpty(node.UnescapedProximity) && Int64.TryParse(node.UnescapedProximity, out long parsedPrecision))
                {
                    if (parsedPrecision is < 1 or > 12)
                        throw new ArgumentOutOfRangeException(nameof(node.UnescapedProximity), "Precision must be between 1 and 12");

                    precision = new GeohashPrecision(parsedPrecision);
                }

                return new AggregationMap($"geogrid_{originalField}", new GeohashGridAggregation { Field = aggField, Precision = precision })
                {
                    Aggregations =
                    {
                        new AggregationMap("avg_lat", new AverageAggregation { Script = new Script { Source = $"doc['{node.Field}'].lat" } }),
                        new AggregationMap("avg_lon", new AverageAggregation { Script = new Script { Source = $"doc['{node.Field}'].lon" } })
                    }
                };

            case AggregationType.Terms:
                var termsAggregation = new TermsAggregation
                {
                    Field = aggField,
                    Size = node.GetProximityAsInt32(),
                    MinDocCount = node.GetBoostAsInt32()
                };

                if (termsAggregation.Size.HasValue && (termsAggregation.Size * 1.5 + 10) > MAX_BUCKET_SIZE)
                    termsAggregation.ShardSize = Math.Max((int)termsAggregation.Size, MAX_BUCKET_SIZE);

                return new AggregationMap($"terms_{originalField}", termsAggregation)
                {
                    Meta = { { "@field_type", property?.Type } }
                };
        }

        return null;
    }

    private static AggregationMap GetPercentilesAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context)
    {
        List<double> percents = null;
        if (!String.IsNullOrWhiteSpace(proximity))
        {
            string[] percentStrings = proximity.Split(',');
            percents = new List<double>();
            foreach (string ps in percentStrings)
            {
                if (Double.TryParse(ps, out double percent))
                    percents.Add(percent);
            }
        }

        return new AggregationMap(originalField, new PercentilesAggregation
        {
            Field = field,
            Percents = percents
        });
    }

    private static AggregationMap GetHistogramAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context)
    {
        double interval = 50;
        if (Double.TryParse(proximity, out double prox))
            interval = prox;

        return new AggregationMap(originalField, new HistogramAggregation
        {
            Field = field,
            MinDocCount = 0,
            Interval = interval
        });
    }

    private static AggregationMap GetDateHistogramAggregation(string originalField, string field, string proximity, string boost, IQueryVisitorContext context)
    {
        // NOTE: StartDate and EndDate are set in the Repositories QueryBuilderContext.
        var start = context.GetDate("StartDate");
        var end = context.GetDate("EndDate");
        bool isValidRange = start.HasValue && start.Value > DateTime.MinValue && end.HasValue && end.Value < DateTime.MaxValue && start.Value <= end.Value;
        var bounds = isValidRange ? new ExtendedBounds<DateTime> { Min = start.Value, Max = end.Value } : null;

        var interval = GetInterval(proximity, start, end);
        string timezone = TryConvertTimeUnitToUtcOffset(boost);
        var agg = new DateHistogramAggregation
        {
            Field = field,
            MinDocCount = 0,
            Format = "date_optional_time",
            TimeZone = timezone,
            //ExtendedBounds = bounds
            // TODO: https://github.com/elastic/elasticsearch-net/issues/8496
        };

        interval.Match(d => agg.CalendarInterval = d, f => agg.FixedInterval = f);

        var aggregationMap = new AggregationMap(originalField, agg);
        if (!String.IsNullOrEmpty(boost))
        {
            aggregationMap.Meta.Add("@timezone", boost);
        }

        return aggregationMap;
    }

    private static string TryConvertTimeUnitToUtcOffset(string boost)
    {
        if (String.IsNullOrEmpty(boost))
            return null;

        if (!Exceptionless.DateTimeExtensions.TimeUnit.TryParse(boost, out var timezoneOffset))
        {
            // if it fails to parse, just return it unmodified
            return boost;
        }

        if (!timezoneOffset.HasValue)
            return null;

        if (timezoneOffset.Value < TimeSpan.Zero)
            return $"-{timezoneOffset.Value:hh\\:mm}";

        return $"+{timezoneOffset.Value:hh\\:mm}";
    }

    private static Union<CalendarInterval, Duration> GetInterval(string proximity, DateTime? start, DateTime? end)
    {
        if (String.IsNullOrEmpty(proximity))
            return GetInterval(start, end);

        return proximity.Trim() switch
        {
            "s" or "1s" or "second" => CalendarInterval.Second,
            "m" or "1m" or "minute" => CalendarInterval.Minute,
            "h" or "1h" or "hour" => CalendarInterval.Hour,
            "d" or "1d" or "day" => CalendarInterval.Day,
            "w" or "1w" or "week" => CalendarInterval.Week,
            "M" or "1M" or "month" => CalendarInterval.Month,
            "q" or "1q" or "quarter" => CalendarInterval.Quarter,
            "y" or "1y" or "year" => CalendarInterval.Year,
            _ => new Union<CalendarInterval, Duration>(proximity),
        };
    }

    private static Union<CalendarInterval, Duration> GetInterval(DateTime? utcStart, DateTime? utcEnd, int desiredDataPoints = 100)
    {
        if (!utcStart.HasValue || !utcEnd.HasValue || utcStart.Value == DateTime.MinValue)
            return CalendarInterval.Day;

        var totalTime = utcEnd.Value - utcStart.Value;
        var timePerBlock = TimeSpan.FromMinutes(totalTime.TotalMinutes / desiredDataPoints);
        if (timePerBlock.TotalDays > 1)
        {
            timePerBlock = timePerBlock.Round(TimeSpan.FromDays(1));
            return (Duration)timePerBlock;
        }

        if (timePerBlock.TotalHours > 1)
        {
            timePerBlock = timePerBlock.Round(TimeSpan.FromHours(1));
            return (Duration)timePerBlock;
        }

        if (timePerBlock.TotalMinutes > 1)
        {
            timePerBlock = timePerBlock.Round(TimeSpan.FromMinutes(1));
            return (Duration)timePerBlock;
        }

        timePerBlock = timePerBlock.Round(TimeSpan.FromSeconds(15));
        if (timePerBlock.TotalSeconds < 1)
            timePerBlock = TimeSpan.FromSeconds(15);

        return (Duration)timePerBlock;
    }

    public static int? GetProximityAsInt32(this IFieldQueryWithProximityAndBoostNode node)
    {
        if (!String.IsNullOrEmpty(node.UnescapedProximity) && Int32.TryParse(node.UnescapedProximity, out int parsedValue))
            return parsedValue;

        return null;
    }

    public static int? GetBoostAsInt32(this IFieldQueryWithProximityAndBoostNode node)
    {
        if (!String.IsNullOrEmpty(node.UnescapedBoost) && Int32.TryParse(node.UnescapedBoost, out int parsedValue))
            return parsedValue;

        return null;
    }

    public static double? GetProximityAsDouble(this IFieldQueryWithProximityAndBoostNode node)
    {
        if (!String.IsNullOrEmpty(node.UnescapedProximity) && Double.TryParse(node.UnescapedProximity, out double parsedValue))
            return parsedValue;

        return null;
    }

    public static double? GetBoostAsDouble(this IFieldQueryWithProximityAndBoostNode node)
    {
        if (!String.IsNullOrEmpty(node.UnescapedBoost) && Double.TryParse(node.UnescapedBoost, out double parsedValue))
            return parsedValue;

        return null;
    }

    private static void PopulateTermsAggregation(TermsAggregation termsAggregation, IQueryNode node)
    {
        if (termsAggregation is null)
            return;

        foreach (var child in node.Children)
        {
            if (child is GroupNode groupNode && groupNode.HasParens && !String.IsNullOrEmpty(groupNode.Field) && groupNode.Left == null)
                continue;

            var termNode = child as TermNode;
            switch (termNode?.Field)
            {
                case "@exclude":
                    if (termNode.IsRegexTerm)
                    {
                        termsAggregation.Exclude = new TermsExclude(termNode.UnescapedTerm);
                    }
                    else
                    {
                        termsAggregation.Exclude = termsAggregation.Exclude.AddValue(termNode.UnescapedTerm);
                    }
                    break;
                case "@include":
                    if (termNode.IsRegexTerm)
                    {
                        termsAggregation.Include = new TermsInclude(termNode.UnescapedTerm);
                    }
                    else
                    {
                        termsAggregation.Include = termsAggregation.Include.AddValue(termNode.UnescapedTerm);
                    }
                    break;
                case "@missing":
                    termsAggregation.Missing = termNode.UnescapedTerm; break;
                case "@min":
                    {
                        if (!String.IsNullOrEmpty(termNode.Term) && Int32.TryParse(termNode.UnescapedTerm, out int minCount))
                            termsAggregation.MinDocCount = minCount;
                        break;
                    }
            }

            PopulateTermsAggregation(termsAggregation, child);
        }
    }

    private static void PopulateTopHitsAggregation(TopHitsAggregation topHitsAggregation, IQueryNode node)
    {
        if (topHitsAggregation is null)
            return;

        foreach (var child in node.Children)
        {
            if (child is GroupNode groupNode && groupNode.HasParens && !String.IsNullOrEmpty(groupNode.Field) && groupNode.Left == null)
                continue;

            if (child is TermNode termNode)
            {
                if (topHitsAggregation.Source is null)
                {
                    topHitsAggregation.Source = node.GetSourceFilter(() => new SourceFilter());
                }

                topHitsAggregation.Source.Match(
                    b => { },
                    filter =>
                    {
                        switch (termNode.Field)
                        {
                            case "@exclude":
                                {
                                    if (filter.Excludes == null)
                                        filter.Excludes = termNode.UnescapedTerm;
                                    else
                                        filter.Excludes.And(termNode.UnescapedTerm);
                                    break;
                                }
                            case "@include":
                                {
                                    if (filter.Includes == null)
                                        filter.Includes = termNode.UnescapedTerm;
                                    else
                                        filter.Includes.And(termNode.UnescapedTerm);
                                    break;
                                }
                        }
                    });
            }

            PopulateTopHitsAggregation(topHitsAggregation, child);
        }
    }

    private static void PopulateDateHistogramAggregation(DateHistogramAggregation dateHistogramAggregation, IQueryNode node)
    {
        if (dateHistogramAggregation is null)
            return;

        foreach (var child in node.Children)
        {
            if (child is GroupNode groupNode && groupNode.HasParens && !String.IsNullOrEmpty(groupNode.Field) && groupNode.Left == null)
                continue;

            var termNode = child as TermNode;
            switch (termNode?.Field)
            {
                case "@missing":
                    {
                        DateTime? missingValue = null;
                        if (!String.IsNullOrEmpty(termNode.Term) && DateTime.TryParse(termNode.Term, out var parsedMissingDate))
                            missingValue = parsedMissingDate;

                        dateHistogramAggregation.Missing = missingValue;
                        break;
                    }
                case "@offset":
                    dateHistogramAggregation.Offset = termNode.IsExcluded() ? "-" + termNode.Term : termNode.Term;
                    break;
            }

            PopulateDateHistogramAggregation(dateHistogramAggregation, child);
        }
    }
}
