using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorContext : IQueryVisitorContext, IQueryVisitorContextWithFieldResolver, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithValidation {
        public GroupOperator DefaultOperator { get; set; } = GroupOperator.And;
        public string[] DefaultFields { get; set; }
        public string QueryType { get; set; } = Visitors.QueryType.Query;
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
        public QueryFieldResolver FieldResolver { get; set; }
        public IncludeResolver IncludeResolver { get; set; }
        public QueryValidationOptions ValidationOptions { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
    }
}
