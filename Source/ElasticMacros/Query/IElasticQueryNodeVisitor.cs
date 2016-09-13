using Exceptionless.ElasticQueryParser.Query.Nodes;

namespace Exceptionless.ElasticQueryParser.Query {
    public interface IElasticQueryNodeVisitor {
        void Visit(QueryGroupNode node);
        void Visit(QueryTermNode node);
        void Visit(QueryTermRangeNode node);
        void Visit(QueryExistsNode node);
        void Visit(QueryMissingNode node);
    }
}
