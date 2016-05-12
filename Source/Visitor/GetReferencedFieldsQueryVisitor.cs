using System;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class GetReferencedFieldsQueryVisitor : QueryNodeVisitorWithResultBase<ISet<String>> {
        private readonly HashSet<string> _fields = new HashSet<String>(StringComparer.OrdinalIgnoreCase);

        public override void Visit(GroupNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.Field);
        }

        public override void Visit(TermNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.Field);
        }

        public override void Visit(TermRangeNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.Field);
        }

        public override void Visit(ExistsNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.Field);
        }

        public override void Visit(MissingNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                _fields.Add(node.Field);
        }

        public override ISet<string> Accept(IQueryNode node) {
            node.Accept(this, false);
            return _fields;
        }

        public static ISet<string> Run(IQueryNode node) {
            return new GetReferencedFieldsQueryVisitor().Accept(node);
        }
    }
}
