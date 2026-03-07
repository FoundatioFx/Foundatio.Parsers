using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch.Aggregations;

namespace Foundatio.Parsers.ElasticQueries;

/// <summary>
/// Intermediate representation for building aggregation trees during query parsing.
/// <para>
/// The Elastic.Clients.Elasticsearch <see cref="Aggregation"/> type is a discriminated union
/// that does not support attaching child aggregations during tree construction. This class
/// provides a mutable tree structure that is converted to
/// <see cref="IDictionary{TKey,TValue}">IDictionary&lt;string, Aggregation&gt;</see> via
/// <see cref="ToDictionary"/> once the tree is fully built.
/// </para>
/// </summary>
public class AggregationMap
{
    public AggregationMap(string name, object value)
    {
        Name = name;
        Value = value;
    }

    /// <summary>The aggregation name used as the dictionary key in the Elasticsearch request.</summary>
    public string Name { get; set; }

    /// <summary>The concrete aggregation instance (e.g. <see cref="TermsAggregation"/>, <see cref="MinAggregation"/>).</summary>
    public object Value { get; set; }

    /// <summary>Child (sub) aggregations nested under this bucket aggregation.</summary>
    public List<AggregationMap> Aggregations { get; } = [];

    /// <summary>Metadata key/value pairs attached to this aggregation via the <c>meta</c> field.</summary>
    public Dictionary<string, object> Meta { get; } = [];

    /// <summary>
    /// Converts the tree into an Elasticsearch aggregation dictionary suitable for
    /// passing to <c>SearchRequestDescriptor&lt;T&gt;.Aggregations()</c>.
    /// </summary>
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
            aggregation.Meta = map.Meta
                .Where(kvp => kvp.Value is not null)
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            if (aggregation.Meta.Count == 0)
                aggregation.Meta = null;
        }

        return aggregation;
    }
}
