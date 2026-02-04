using Foundatio.Parsers.LuceneQueries;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

/// <summary>
/// Extends <see cref="IQueryVisitorContext"/> with query validation capabilities.
/// </summary>
public interface IQueryVisitorContextWithValidation : IQueryVisitorContext
{
    /// <summary>
    /// Configuration options controlling validation behavior.
    /// </summary>
    QueryValidationOptions ValidationOptions { get; set; }

    /// <summary>
    /// The validation results populated after validation completes.
    /// </summary>
    QueryValidationResult ValidationResult { get; set; }
}
