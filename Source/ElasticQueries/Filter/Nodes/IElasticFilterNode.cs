using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Filter.Nodes {
    public interface IElasticFilterNode : IQueryNode {
        FilterContainer Filter { get; set; }
    }
}
