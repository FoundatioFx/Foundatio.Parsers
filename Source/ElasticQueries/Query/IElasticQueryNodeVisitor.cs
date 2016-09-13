using Foundatio.Parsers.ElasticQueries.Query.Nodes;

namespace Foundatio.Parsers.ElasticQueries.Query {
    public interface IElasticQueryNodeVisitor {
        void Visit(QueryGroupNode node);
        void Visit(QueryTermNode node);
        void Visit(QueryTermRangeNode node);
        void Visit(QueryExistsNode node);
        void Visit(QueryMissingNode node);
    }
}
