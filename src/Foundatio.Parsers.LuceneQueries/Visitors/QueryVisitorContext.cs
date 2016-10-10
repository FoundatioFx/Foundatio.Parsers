using System;
using System.Collections.Generic;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContext : IQueryVisitorContext {
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
    }
}