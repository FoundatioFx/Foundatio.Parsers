using System.Text;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros.FilterMacros {
    public class ElasticFilterMacroContext {
        public ElasticMacrosConfiguration Config { get; set; }
        public string DefaultField { get; set; }
        public FilterContainer Filter { get; set; }
        public GroupNode Group { get; set; }
        public Operator Operator { get; set; }
        public string[] FieldPrefixParts { get; set; }

        public string GetFullFieldName(string field) {
            if (FieldPrefixParts.Length == 0)
                return field;

            var sb = new StringBuilder();
            foreach (var f in FieldPrefixParts) {
                sb.Append(f);
                sb.Append('.');
            }

            sb.Append(field);

            return sb.ToString();
        }
    }
}