using System;
using System.Threading.Tasks;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContextWithIncludeResolver : QueryVisitorContext, IQueryVisitorContextWithIncludeResolver {
        public IncludeResolver IncludeResolver { get; set; }
    }
}