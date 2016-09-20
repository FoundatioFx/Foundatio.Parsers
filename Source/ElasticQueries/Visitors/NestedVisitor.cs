using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class NestedVisitor: ChainableQueryVisitor {
        private readonly Func<string, bool> _isNestedField;

        public NestedVisitor(Func<string, bool> isNestedField) {
            _isNestedField = isNestedField;
        }

        public override void Visit(GroupNode node) {
            if (!IsFieldNested(node.GetNameParts())) {
                base.Visit(node);
                return;
            }

            node.SetFilter(new NestedFilter { Path = node.GetFullName(), Filter = node.GetFilter() });
            node.Parent.InvalidateFilter();

            node.SetQuery(new NestedQuery { Path = node.GetFullName(), Query = node.GetQuery() });
            node.Parent.InvalidateQuery();

            base.Visit(node);
        }

        public override void Visit(TermNode node) {
            if (!IsFieldNested(node.Field?.Split('.')))
                return;

            node.SetFilter(new NestedFilter { Path = node.GetParentFullName(), Filter = node.GetFilter() });
            node.InvalidateFilter();

            node.SetQuery(new NestedQuery { Path = node.GetParentFullName(), Query = node.GetQuery() });
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
