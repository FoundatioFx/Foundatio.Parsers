using Nest;

namespace ElasticMacros.FilterMacros {
    public class ElasticFilterMacroContext {
        public string DefaultField { get; set; }
        public PlainFilter Filter { get; set; }
    }
}