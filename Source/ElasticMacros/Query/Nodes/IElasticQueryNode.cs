using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace Exceptionless.ElasticQueryParser.Query.Nodes {
    public interface IElasticQueryNode: IQueryNode {
        QueryContainer Query { get; set; }
    }
}
