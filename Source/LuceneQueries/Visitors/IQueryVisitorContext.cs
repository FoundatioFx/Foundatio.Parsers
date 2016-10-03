using System.Collections.Generic;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryVisitorContext {
        IDictionary<string, object> Data { get; }
    }
}