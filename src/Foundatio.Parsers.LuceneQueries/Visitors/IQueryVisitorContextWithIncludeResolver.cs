using System;
using System.Threading.Tasks;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryVisitorContextWithIncludeResolver : IQueryVisitorContext {
        Func<string, Task<string>> IncludeResolver { get; set; }
    }
}