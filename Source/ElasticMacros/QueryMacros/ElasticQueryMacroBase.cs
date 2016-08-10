using Exceptionless.LuceneQueryParser.Nodes;

namespace ElasticMacros.QueryMacros {
    public abstract class ElasticQueryMacroBase : IElasticQueryMacro {
        public virtual void Expand(GroupNode node, ElasticQueryMacroContext context) { }

        public virtual void Expand(TermNode node, ElasticQueryMacroContext context) { }

        public virtual void Expand(TermRangeNode node, ElasticQueryMacroContext context) { }

        public virtual void Expand(MissingNode node, ElasticQueryMacroContext context) { }

        public virtual void Expand(ExistsNode node, ElasticQueryMacroContext context) { }
    }
}