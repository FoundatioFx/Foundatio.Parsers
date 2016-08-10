using System;
using Exceptionless.LuceneQueryParser.Nodes;

namespace ElasticMacros.FilterMacros {
    public interface IElasticFilterMacro {
        void Expand(GroupNode node, ElasticFilterMacroContext context);
        void Expand(TermNode node, ElasticFilterMacroContext context);
        void Expand(TermRangeNode node, ElasticFilterMacroContext context);
        void Expand(MissingNode node, ElasticFilterMacroContext context);
        void Expand(ExistsNode node, ElasticFilterMacroContext context);
    }
}
