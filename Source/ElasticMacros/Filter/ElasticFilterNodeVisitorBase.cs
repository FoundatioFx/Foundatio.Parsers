using Exceptionless.ElasticQueryParser.Filter.Nodes;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.ElasticQueryParser.Filter {
    public abstract class ElasticFilterNodeVisitorBase : ChainableQueryVisitor, IQueryNodeVisitor, IElasticFilterNodeVisitor {
        public virtual void Visit(FilterGroupNode node) { }
        public virtual void Visit(FilterTermNode node) { }
        public virtual void Visit(FilterTermRangeNode node) { }
        public virtual void Visit(FilterExistsNode node) { }
        public virtual void Visit(FilterMissingNode node) { }

        void IQueryNodeVisitor.Visit(GroupNode node) {
            var filterNode = node as FilterGroupNode;
            if (filterNode != null)
                Visit(filterNode);
        }

        void IQueryNodeVisitor.Visit(TermNode node) {
            var filterNode = node as FilterTermNode;
            if (filterNode != null)
                Visit(filterNode);
        }

        void IQueryNodeVisitor.Visit(TermRangeNode node) {
            var filterNode = node as FilterTermRangeNode;
            if (filterNode != null)
                Visit(filterNode);
        }

        void IQueryNodeVisitor.Visit(ExistsNode node) {
            var filterNode = node as FilterExistsNode;
            if (filterNode != null)
                Visit(filterNode);
        }

        void IQueryNodeVisitor.Visit(MissingNode node) {
            var filterNode = node as FilterMissingNode;
            if (filterNode != null)
                Visit(filterNode);
        }
    }
}
