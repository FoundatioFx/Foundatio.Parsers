using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter.Nodes {
    public class FilterTermNode : TermNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}