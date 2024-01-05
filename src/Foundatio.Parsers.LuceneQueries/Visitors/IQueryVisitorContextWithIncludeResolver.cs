namespace Foundatio.Parsers.LuceneQueries.Visitors;

public interface IQueryVisitorContextWithIncludeResolver : IQueryVisitorContext
{
    IncludeResolver IncludeResolver { get; set; }
}
