using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter.Nodes {
    public class FilterGroupNode : GroupNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}