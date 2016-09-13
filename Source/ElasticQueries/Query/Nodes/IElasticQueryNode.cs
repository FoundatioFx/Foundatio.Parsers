using Foundatio.Parsers.LuceneQueries.Nodes;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Query.Nodes {
    public interface IElasticQueryNode: IQueryNode {
        QueryContainer Query { get; set; }
    }
}
