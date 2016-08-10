using Nest;

namespace ElasticMacros.QueryMacros {
    public class ElasticQueryMacroContext {
        public string DefaultField { get; set; }
        public PlainQuery Query { get; set; }
    }
}