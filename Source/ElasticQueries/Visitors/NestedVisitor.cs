using System;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Visitors {
    public class NestedVisitor: ChainableQueryVisitor {
        private readonly Func<string, bool> _isNestedPropertyType;

        public NestedVisitor(Func<string, bool> isNestedPropertyType) {
            _isNestedPropertyType = isNestedPropertyType;
        }

        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            if (!IsNestedPropertyType(node.GetNameParts())) {
                base.Visit(node, context);
                return;
            }

            node.SetQuery(new NestedQuery { Path = node.GetFullName(), Query = node.GetQuery() });
            node.Parent.InvalidateQuery();

            base.Visit(node, context);
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (!IsNestedPropertyType(node.Field?.Split('.')))
                return;

            node.SetQuery(new NestedQuery { Path = node.GetParentFullName(), Query = node.GetQuery() });
            node.InvalidateQuery();
        }

        private bool IsNestedPropertyType(string[] nameParts) {
            if (nameParts == null || _isNestedPropertyType == null || nameParts.Length == 0)
                return false;

            string fieldName = String.Empty;
            for (int i = 0; i < nameParts.Length; i++) {
                if (i > 0)
                    fieldName += ".";

                fieldName += nameParts[i];

                if (_isNestedPropertyType(fieldName))
                    return true;
            }

            return false;
        }
    }
}
