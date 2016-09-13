using Foundatio.Parsers.ElasticQueries.Query.Nodes;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.ElasticQueries.Query {
    public abstract class ElasticQueryNodeVisitorBase : ChainableQueryVisitor, IQueryNodeVisitor, IElasticQueryNodeVisitor {
        public virtual void Visit(QueryGroupNode node) { }
        public virtual void Visit(QueryTermNode node) { }
        public virtual void Visit(QueryTermRangeNode node) { }
        public virtual void Visit(QueryExistsNode node) { }
        public virtual void Visit(QueryMissingNode node) { }

        void IQueryNodeVisitor.Visit(GroupNode node) {
            var groupNode = node as QueryGroupNode;
            if (groupNode != null)
                Visit(groupNode);
        }

        void IQueryNodeVisitor.Visit(TermNode node) {
            var termNode = node as QueryTermNode;
            if (termNode != null)
                Visit(termNode);
        }

        void IQueryNodeVisitor.Visit(TermRangeNode node) {
            var termRangeNode = node as QueryTermRangeNode;
            if (termRangeNode != null)
                Visit(termRangeNode);
        }

        void IQueryNodeVisitor.Visit(ExistsNode node) {
            var existsNode = node as QueryExistsNode;
            if (existsNode != null)
                Visit(existsNode);
        }

        void IQueryNodeVisitor.Visit(MissingNode node) {
            var missingNode = node as QueryMissingNode;
            if (missingNode != null)
                Visit(missingNode);
        }
    }
}
