using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Query.Nodes {
    public class QueryTermRangeNode : TermRangeNode, IElasticQueryNode {
        public QueryContainer Query { get; set; }
    }
}