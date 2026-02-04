namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// Extends <see cref="IQueryVisitorContext"/> with query include/macro expansion.
/// </summary>
/// <remarks>
/// Includes allow reusable query fragments to be referenced by name (e.g., @myfilter).
/// </remarks>
public interface IQueryVisitorContextWithIncludeResolver : IQueryVisitorContext
{
    /// <summary>
    /// Resolves include names to their query string definitions.
    /// </summary>
    IncludeResolver IncludeResolver { get; set; }
}
