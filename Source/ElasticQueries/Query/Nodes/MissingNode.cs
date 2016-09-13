using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Query.Nodes {
    public class QueryMissingNode : MissingNode, IElasticQueryNode {
        public QueryContainer Query { get; set; }
    }
}