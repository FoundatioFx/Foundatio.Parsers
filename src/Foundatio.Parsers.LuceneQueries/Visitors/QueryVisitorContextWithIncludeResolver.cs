using System;
using System.Threading.Tasks;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContextWithIncludeResolver : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver {
        public Func<string, Task<string>> IncludeResolver { get; set; }
    }
}