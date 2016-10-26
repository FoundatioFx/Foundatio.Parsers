namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryVisitorContextWithAliasResolver : IQueryVisitorContext {
        AliasResolver RootAliasResolver { get; set; }
    }
}