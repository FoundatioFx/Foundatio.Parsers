using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors
{
    /// <summary>
    /// Extends <see cref="IQueryVisitorContext"/> with Elasticsearch-specific configuration.
    /// </summary>
    public interface IElasticQueryVisitorContext : IQueryVisitorContext
    {
        /// <summary>
        /// Provides the default timezone for date queries when not explicitly specified.
        /// </summary>
        Func<Task<string>> DefaultTimeZone { get; set; }

        /// <summary>
        /// Whether to use scoring queries (query context) instead of filters (filter context).
        /// </summary>
        bool UseScoring { get; set; }

        /// <summary>
        /// Resolves field names to their Elasticsearch mapping definitions.
        /// </summary>
        ElasticMappingResolver MappingResolver { get; set; }

        /// <summary>
        /// Runtime fields to include in the search request.
        /// </summary>
        ICollection<ElasticRuntimeField> RuntimeFields { get; }

        /// <summary>
        /// Whether to automatically resolve unmapped fields as runtime fields.
        /// </summary>
        bool? EnableRuntimeFieldResolver { get; set; }

        /// <summary>
        /// Resolves field names to runtime field definitions for dynamic field support.
        /// </summary>
        RuntimeFieldResolver RuntimeFieldResolver { get; set; }
    }
}

namespace Foundatio.Parsers
{
    /// <summary>
    /// Resolves a field name to an Elasticsearch runtime field definition.
    /// </summary>
    /// <param name="field">The field name to resolve.</param>
    /// <returns>The runtime field definition, or null if the field should not be a runtime field.</returns>
    public delegate Task<ElasticRuntimeField> RuntimeFieldResolver(string field);

    /// <summary>
    /// Defines an Elasticsearch runtime field for dynamic field computation.
    /// </summary>
    public class ElasticRuntimeField
    {
        /// <summary>
        /// The name of the runtime field.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The data type of the runtime field.
        /// </summary>
        public ElasticRuntimeFieldType FieldType { get; set; } = ElasticRuntimeFieldType.Keyword;

        /// <summary>
        /// The Painless script that computes the field value.
        /// </summary>
        public string Script { get; set; }
    }

    /// <summary>
    /// Supported data types for Elasticsearch runtime fields.
    /// </summary>
    /// <remarks>
    /// See https://www.elastic.co/guide/en/elasticsearch/reference/current/runtime-mapping-fields.html
    /// </remarks>
    public enum ElasticRuntimeFieldType
    {
        Boolean,
        Date,
        Double,
        GeoPoint,
        Ip,
        Keyword,
        Long
    }
}
