using Foundatio.Parsers.ElasticQueries.Filter.Nodes;

namespace Foundatio.Parsers.ElasticQueries.Filter {
    public interface IElasticFilterNodeVisitor {
        void Visit(FilterGroupNode node);
        void Visit(FilterTermNode node);
        void Visit(FilterTermRangeNode node);
        void Visit(FilterExistsNode node);
        void Visit(FilterMissingNode node);
    }
}
