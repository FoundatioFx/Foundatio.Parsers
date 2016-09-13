using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter.Nodes {
    public class FilterExistsNode : ExistsNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}