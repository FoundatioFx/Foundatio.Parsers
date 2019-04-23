using System;
using System.Collections.Generic;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContext : IQueryVisitorContext {
        public string[] DefaultFields { get; set; }
        public string QueryType { get; set; } = Foundatio.Parsers.LuceneQueries.Visitors.QueryType.Query;
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
    }
}
