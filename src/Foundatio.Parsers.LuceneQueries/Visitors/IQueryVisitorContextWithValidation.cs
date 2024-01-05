namespace Foundatio.Parsers.LuceneQueries.Visitors;

public interface IQueryVisitorContextWithValidation : IQueryVisitorContext
{
    QueryValidationOptions ValidationOptions { get; set; }
    QueryValidationResult ValidationResult { get; set; }
}
