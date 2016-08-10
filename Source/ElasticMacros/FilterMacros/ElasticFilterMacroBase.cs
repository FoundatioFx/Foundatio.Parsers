using Exceptionless.LuceneQueryParser.Nodes;

namespace ElasticMacros.FilterMacros {
    public abstract class ElasticFilterMacroBase : IElasticFilterMacro {
        public virtual void Expand(GroupNode node, ElasticFilterMacroContext context) {}

        public virtual void Expand(TermNode node, ElasticFilterMacroContext context) {}

        public virtual void Expand(TermRangeNode node, ElasticFilterMacroContext context) {}

        public virtual void Expand(MissingNode node, ElasticFilterMacroContext context) {}

        public virtual void Expand(ExistsNode node, ElasticFilterMacroContext context) {}
    }
}