namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContextWithAliasResolver : QueryVisitorContext, IQueryVisitorContextWithAliasResolver {
        public AliasResolver RootAliasResolver { get; set; }
    }
}