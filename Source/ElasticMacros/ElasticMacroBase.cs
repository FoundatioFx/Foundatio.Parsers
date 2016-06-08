using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros {
    public abstract class ElasticMacroBase : IElasticMacro {
        public virtual PlainFilter Expand(GroupNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return currentFilter;
        }

        public virtual PlainFilter Expand(TermNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return currentFilter;
        }

        public virtual PlainFilter Expand(TermRangeNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return currentFilter;
        }

        public virtual PlainFilter Expand(MissingNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return currentFilter;
        }

        public virtual PlainFilter Expand(ExistsNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return currentFilter;
        }
    }
}