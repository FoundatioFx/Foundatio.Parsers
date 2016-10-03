using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class GetReferencedFieldsQueryVisitor : QueryNodeVisitorWithResultBase<ISet<String>> {
        private readonly HashSet<string> _fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public override void Visit(GroupNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.GetFullName());
        }

        public override void Visit(TermNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.GetFullName());
        }

        public override void Visit(TermRangeNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field)) {
                _fields.Add(node.GetFullName());
            } else {
                var nameParts = node.GetNameParts();
                if (nameParts.Length == 0)
                    _fields.Add("_all");
            }
        }

        public override void Visit(ExistsNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.GetFullName());
        }

        public override void Visit(MissingNode node, IQueryVisitorContext context) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.GetFullName());
        }

        public override ISet<string> Accept(IQueryNode node, IQueryVisitorContext context) {
            node.Accept(this, context);
            return _fields;
        }

        public static ISet<string> Run(IQueryNode node) {
            return new GetReferencedFieldsQueryVisitor().Accept(node, null);
        }
    }
}
