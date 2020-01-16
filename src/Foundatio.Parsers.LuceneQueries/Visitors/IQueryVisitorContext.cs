using System.Collections.Generic;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryVisitorContext {
        string[] DefaultFields { get; set; }
        string QueryType { get; set; }
        IDictionary<string, object> Data { get; }
    }
}