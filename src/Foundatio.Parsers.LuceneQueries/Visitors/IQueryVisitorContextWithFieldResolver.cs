namespace Foundatio.Parsers.LuceneQueries.Visitors;

public interface IQueryVisitorContextWithFieldResolver : IQueryVisitorContext
{
    QueryFieldResolver FieldResolver { get; set; }
}
