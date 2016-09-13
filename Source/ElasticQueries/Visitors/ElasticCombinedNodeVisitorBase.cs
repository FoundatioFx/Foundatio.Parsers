using Foundatio.Parsers.ElasticQueries.Filter;
using Foundatio.Parsers.ElasticQueries.Filter.Nodes;
using Foundatio.Parsers.ElasticQueries.Query;
using Foundatio.Parsers.ElasticQueries.Query.Nodes;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public abstract class ElasticCombinedNodeVisitorBase : ChainableQueryVisitor, IElasticFilterNodeVisitor, IElasticQueryNodeVisitor {
        public virtual void Visit(FilterGroupNode node) {
            foreach (var child in node.Children)
                Visit(child);
        }

        public virtual void Visit(FilterTermNode node) { }
        public virtual void Visit(FilterTermRangeNode node) { }
        public virtual void Visit(FilterExistsNode node) { }
        public virtual void Visit(FilterMissingNode node) { }

        public virtual void Visit(QueryGroupNode node) {
            foreach (var child in node.Children)
                Visit(child);
        }

        public virtual void Visit(QueryTermNode node) { }
        public virtual void Visit(QueryTermRangeNode node) { }
        public virtual void Visit(QueryExistsNode node) { }
        public virtual void Visit(QueryMissingNode node) { }

        public override void Visit(GroupNode node) {
            var filterNode = node as FilterGroupNode;
            if (filterNode != null)
                Visit(filterNode);

            var queryNode = node as QueryGroupNode;
            if (queryNode != null)
                Visit(queryNode);
        }

        public override void Visit(TermNode node) {
            var filterNode = node as FilterTermNode;
            if (filterNode != null)
                Visit(filterNode);

            var queryNode = node as QueryTermNode;
            if (queryNode != null)
                Visit(queryNode);
        }

        public override void Visit(TermRangeNode node) {
            var filterNode = node as FilterTermRangeNode;
            if (filterNode != null)
                Visit(filterNode);

            var queryNode = node as QueryTermRangeNode;
            if (queryNode != null)
                Visit(queryNode);
        }

        public override void Visit(ExistsNode node) {
            var filterNode = node as FilterExistsNode;
            if (filterNode != null)
                Visit(filterNode);

            var queryNode = node as QueryExistsNode;
            if (queryNode != null)
                Visit(queryNode);
        }

        public override void Visit(MissingNode node) {
            var filterNode = node as FilterMissingNode;
            if (filterNode != null)
                Visit(filterNode);

            var queryNode = node as QueryMissingNode;
            if (queryNode != null)
                Visit(queryNode);
        }
    }
}
