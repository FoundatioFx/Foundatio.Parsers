using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros {
    public interface IElasticMacro {
        PlainFilter Expand(GroupNode node, PlainFilter currentFilter, ElasticMacroContext context);
        PlainFilter Expand(TermNode node, PlainFilter currentFilter, ElasticMacroContext context);
        PlainFilter Expand(TermRangeNode node, PlainFilter currentFilter, ElasticMacroContext context);
        PlainFilter Expand(MissingNode node, PlainFilter currentFilter, ElasticMacroContext context);
        PlainFilter Expand(ExistsNode node, PlainFilter currentFilter, ElasticMacroContext context);
    }
}
