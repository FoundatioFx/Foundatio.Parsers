namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryVisitorContextWithValidation : IQueryVisitorContext {
        QueryValidationOptions ValidationOptions { get; set; }
        QueryValidationInfo ValidationInfo { get; set; }
    }
}