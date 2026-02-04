using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.SqlQueries.Visitors;

/// <summary>
/// Extends <see cref="IQueryVisitorContext"/> with SQL query generation configuration.
/// </summary>
public interface ISqlQueryVisitorContext : IQueryVisitorContext
{
    /// <summary>
    /// Metadata about available entity fields for query generation.
    /// </summary>
    List<EntityFieldInfo> Fields { get; set; }

    /// <summary>
    /// The default operator for text searches (Equals, Contains, StartsWith).
    /// </summary>
    SqlSearchOperator DefaultSearchOperator { get; set; }

    /// <summary>
    /// Fields that support full-text search operations.
    /// </summary>
    string[] FullTextFields { get; set; }

    /// <summary>
    /// Tokenizes search terms for full-text search processing.
    /// </summary>
    Action<SearchTerm> SearchTokenizer { get; set; }

    /// <summary>
    /// Converts date/time strings to SQL-compatible format.
    /// </summary>
    Func<string, string> DateTimeParser { get; set; }

    /// <summary>
    /// Converts date-only strings to SQL-compatible format.
    /// </summary>
    Func<string, string> DateOnlyParser { get; set; }
}
