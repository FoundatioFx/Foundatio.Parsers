using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors;

public class ElasticQueryVisitorContext : QueryVisitorContext, IElasticQueryVisitorContext
{
    /// <summary>Async factory returning the default time zone (IANA or UTC offset) for date queries.</summary>
    public Func<Task<string>>? DefaultTimeZone { get; set; }

    /// <summary>When true, queries use scoring context (bool.must); when false, filter context (bool.filter).</summary>
    public bool UseScoring { get; set; }

    /// <summary>Resolves field names to Elasticsearch mapping metadata (types, nested paths, analyzers).</summary>
    public ElasticMappingResolver MappingResolver { get; set; } = ElasticMappingResolver.NullInstance;

    /// <summary>Runtime fields accumulated during query building, added to the search request.</summary>
    public ICollection<ElasticRuntimeField> RuntimeFields { get; } = new List<ElasticRuntimeField>();

    /// <summary>Enables or disables the runtime field resolver for this query context.</summary>
    public bool? EnableRuntimeFieldResolver { get; set; }

    /// <summary>Delegate that generates runtime field definitions for unmapped or computed fields.</summary>
    public RuntimeFieldResolver? RuntimeFieldResolver { get; set; }

    /// <summary>Resolves geo-point string values (e.g., place names) to lat/lon coordinates for geo queries and sorts.</summary>
    public Func<string, Task<string>>? GeoLocationResolver { get; set; }
}
