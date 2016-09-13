using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter.Nodes {
    public class FilterTermRangeNode : TermRangeNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}