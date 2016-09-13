using Exceptionless.ElasticQueryParser.Filter.Nodes;

namespace Exceptionless.ElasticQueryParser.Filter {
    public interface IElasticFilterNodeVisitor {
        void Visit(FilterGroupNode node);
        void Visit(FilterTermNode node);
        void Visit(FilterTermRangeNode node);
        void Visit(FilterExistsNode node);
        void Visit(FilterMissingNode node);
    }
}
