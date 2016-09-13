using Nest;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public interface IElasticFilterNode : IQueryNode {
        FilterContainer Filter { get; set; }
    }
}
