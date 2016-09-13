using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Filter.Nodes {
    public class FilterMissingNode : MissingNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}