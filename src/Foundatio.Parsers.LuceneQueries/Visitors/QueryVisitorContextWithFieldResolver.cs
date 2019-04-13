namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContextWithFieldResolver : QueryVisitorContext, IQueryVisitorContextWithFieldResolver {
        public QueryFieldResolver FieldResolver { get; set; }
    }
}