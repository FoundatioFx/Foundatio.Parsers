using Exceptionless.LuceneQueryParser.Nodes;

namespace ElasticMacros.QueryMacros {
    public interface IElasticQueryMacro {
        void Expand(GroupNode node, ElasticQueryMacroContext context);
        void Expand(TermNode node, ElasticQueryMacroContext context);
        void Expand(TermRangeNode node, ElasticQueryMacroContext context);
        void Expand(MissingNode node, ElasticQueryMacroContext context);
        void Expand(ExistsNode node, ElasticQueryMacroContext context);
    }
}