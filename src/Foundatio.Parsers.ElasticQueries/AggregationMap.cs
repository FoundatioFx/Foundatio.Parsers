using System;
using System.Collections.Generic;
using Elastic.Clients.Elasticsearch.Aggregations;

namespace Foundatio.Parsers.ElasticQueries;

public class AggregationMap
{
    public AggregationMap(string name, object value)
    {
        Name = name;
        Value = value;
    }

    public string Name { get; set; }
    public object Value { get; set; }
    public List<AggregationMap> Aggregations { get; } = new();
    public Dictionary<string, object> Meta { get; } = new();

    public IDictionary<string, Aggregation> ToDictionary()
    {
        var result = new Dictionary<string, Aggregation>();

        if (Value is null)
        {
            foreach (var subAgg in Aggregations)
            {
                var subAggregation = CreateAggregation(subAgg);
                if (subAggregation is not null)
                    result[subAgg.Name] = subAggregation;
            }
        }
        else
        {
            AddToDictionary(this, result);
        }

        return result;
    }

    private static void AddToDictionary(AggregationMap map, Dictionary<string, Aggregation> result)
    {
        if (map?.Value is null)
            return;

        var aggregation = CreateAggregation(map);
        if (aggregation is not null)
            result[map.Name] = aggregation;
    }

    private static Aggregation CreateAggregation(AggregationMap map)
    {
        if (map?.Value is null)
            return null;

        var aggregation = map.Value switch
        {
            TermsAggregation terms => new Aggregation { Terms = terms },
            DateHistogramAggregation dateHistogram => new Aggregation { DateHistogram = dateHistogram },
            HistogramAggregation histogram => new Aggregation { Histogram = histogram },
            MinAggregation min => new Aggregation { Min = min },
            MaxAggregation max => new Aggregation { Max = max },
            AverageAggregation avg => new Aggregation { Avg = avg },
            SumAggregation sum => new Aggregation { Sum = sum },
            StatsAggregation stats => new Aggregation { Stats = stats },
            ExtendedStatsAggregation extendedStats => new Aggregation { ExtendedStats = extendedStats },
            CardinalityAggregation cardinality => new Aggregation { Cardinality = cardinality },
            MissingAggregation missing => new Aggregation { Missing = missing },
            TopHitsAggregation topHits => new Aggregation { TopHits = topHits },
            PercentilesAggregation percentiles => new Aggregation { Percentiles = percentiles },
            GeohashGridAggregation geohashGrid => new Aggregation { GeohashGrid = geohashGrid },
            NestedAggregation nested => new Aggregation { Nested = nested },
            _ => throw new NotSupportedException($"Aggregation type '{map.Value.GetType().Name}' is not supported by AggregationMap. Add a case to CreateAggregation.")
        };

        if (map.Aggregations.Count > 0)
        {
            aggregation.Aggregations = new Dictionary<string, Aggregation>();
            foreach (var subAgg in map.Aggregations)
            {
                var subAggregation = CreateAggregation(subAgg);
                if (subAggregation is not null)
                    aggregation.Aggregations[subAgg.Name] = subAggregation;
            }
        }

        if (map.Meta.Count > 0)
        {
            aggregation.Meta = new Dictionary<string, object>();
            foreach (var kvp in map.Meta)
            {
                if (kvp.Value is not null)
                    aggregation.Meta[kvp.Key] = kvp.Value;
            }

            if (aggregation.Meta.Count == 0)
                aggregation.Meta = null;
        }

        return aggregation;
    }
}
