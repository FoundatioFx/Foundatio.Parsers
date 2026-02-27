namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// Extends <see cref="IQueryVisitorContext"/> with field name resolution capabilities.
/// </summary>
/// <remarks>
/// Use this to implement field aliasing or dynamic field name mapping.
/// </remarks>
public interface IQueryVisitorContextWithFieldResolver : IQueryVisitorContext
{
    /// <summary>
    /// Resolves field names to their actual storage names or aliases.
    /// </summary>
    QueryFieldResolver FieldResolver { get; set; }
}
