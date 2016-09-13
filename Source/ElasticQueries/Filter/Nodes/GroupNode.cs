using System;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter.Nodes {
    public class FilterGroupNode : GroupNode, IElasticFilterNode {
        public FilterContainer Filter { get; set; }
    }
}