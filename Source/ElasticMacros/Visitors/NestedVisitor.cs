using System;
using Exceptionless.ElasticQueryParser.Extensions;
using Exceptionless.ElasticQueryParser.Filter.Nodes;
using Exceptionless.ElasticQueryParser.Query.Nodes;
using Exceptionless.LuceneQueryParser.Extensions;
using Nest;

namespace Exceptionless.ElasticQueryParser.Visitors {
    public class NestedVisitor: ElasticCombinedNodeVisitorBase {
        private readonly Func<string, bool> _isNestedField;

        public NestedVisitor(Func<string, bool> isNestedField) {
            _isNestedField = isNestedField;
        }

        public override void Visit(FilterGroupNode node) {
            if (!IsFieldNested(node.GetNameParts())) {
                base.Visit(node);
                return;
            }

            node.Filter = new NestedFilter { Path = node.GetFullName(), Filter = node.Filter };
            node.Parent.InvalidateFilter();

            base.Visit(node);
        }

        public override void Visit(QueryGroupNode node) {
            if (!IsFieldNested(node.GetNameParts()))
                return;

            node.Query = new NestedQuery { Path = node.GetFullName(), Query = node.Query };
            node.Parent.InvalidateQuery();

            base.Visit(node);
        }

        public override void Visit(FilterTermNode node) {
            if (!IsFieldNested(node.Field?.Split('.')))
                return;

            node.Filter = new NestedFilter { Path = node.GetParentFullName(), Filter = node.Filter };
            node.InvalidateFilter();
        }

        public override void Visit(QueryTermNode node) {
            if (!IsFieldNested(node.Field?.Split('.')))
                return;

            node.Query = new NestedQuery { Path = node.GetParentFullName(), Query = node.Query };
            node.InvalidateQuery();
        }

        private bool IsFieldNested(string[] nameParts) {
            if (nameParts == null || _isNestedField == null || nameParts.Length == 0)
                return false;

            string fieldName = String.Empty;
            for (int i = 0; i < nameParts.Length; i++) {
                if (i > 0)
                    fieldName += ".";

                fieldName += nameParts[i];

                if (_isNestedField(fieldName))
                    return true;
            }

            return false;
        }
    }
}
