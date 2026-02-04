using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// Provides context and configuration for query visitor traversal.
/// </summary>
public interface IQueryVisitorContext
{
    /// <summary>
    /// The default boolean operator (AND/OR) when not explicitly specified in the query.
    /// </summary>
    GroupOperator DefaultOperator { get; set; }

    /// <summary>
    /// The default fields to search when no field is specified in a term.
    /// </summary>
    string[] DefaultFields { get; set; }

    /// <summary>
    /// The type of query being processed (query, aggregation, sort).
    /// </summary>
    /// <seealso cref="LuceneQueries.QueryTypes"/>
    string QueryType { get; set; }

    /// <summary>
    /// Extensible property bag for storing custom data during visitor traversal.
    /// </summary>
    IDictionary<string, object> Data { get; }
}
