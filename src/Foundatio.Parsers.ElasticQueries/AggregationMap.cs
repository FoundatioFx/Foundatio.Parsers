using System.Collections.Generic;

namespace Foundatio.Parsers.ElasticQueries;

public record AggregationMap(string Name, object Value)
{
    public string Name { get; set; } = Name;
    public object Value { get; set; } = Value;
    public List<AggregationMap> Aggregations { get; } = new();
    public Dictionary<string, object> Meta { get; } = new();
}
