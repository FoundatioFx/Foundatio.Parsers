using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter.Nodes {
    public class FilterTermRangeNode : TermRangeNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}