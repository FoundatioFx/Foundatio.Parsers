using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter.Nodes {
    public class FilterMissingNode : MissingNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}