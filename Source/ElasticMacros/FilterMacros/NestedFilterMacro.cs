using System;
using Exceptionless.LuceneQueryParser.Nodes;
using Nest;

namespace ElasticMacros.FilterMacros {
    public class NestedFilterMacro: ElasticFilterMacroBase {
        private readonly Func<string, bool> _isNestedField;

        public NestedFilterMacro(Func<string, bool> isNestedField) {
            _isNestedField = isNestedField;
        }

        public override void Expand(TermNode node, ElasticFilterMacroContext ctx) {
            if (!IsFieldNested(ctx.GetFullFieldName(node.Field)))
                return;
            
            ctx.Filter = new NestedFilter { Path = ctx.GetFullFieldName(node.Field), Filter = ctx.Filter };
        }

        public override void Expand(TermRangeNode node, ElasticFilterMacroContext ctx) {
            if (!IsFieldNested(ctx.GetFullFieldName(node.Field)))
                return;

            ctx.Filter = new NestedFilter { Path = ctx.GetFullFieldName(node.Field), Filter = ctx.Filter };
        }

        private bool IsFieldNested(string name) {
            if (_isNestedField == null || String.IsNullOrEmpty(name))
                return false;

            string[] fieldParts = name.Split('.');
            string fieldName = String.Empty;
            for (int i = 0; i < fieldParts.Length; i++) {
                if (i > 0)
                    fieldName += ".";

                fieldName += fieldParts[i];

                if (_isNestedField(fieldName))
                    return true;
            }

            return false;
        }

    }
}
