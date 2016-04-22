using System;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Nodes;

namespace Exceptionless.LuceneQueryParser.Visitor {
    public class AliasedQueryVisitor : QueryNodeVisitorBase<IQueryNode> {
        private readonly Func<string, string> _aliasFunc;

        public AliasedQueryVisitor(Func<string, string> aliasFunc) {
            _aliasFunc = aliasFunc;
        }

        public override void Visit(GroupNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                node.Field = _aliasFunc(node.Field) ?? node.Field;
        }

        public override void Visit(TermNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                node.Field = _aliasFunc(node.Field) ?? node.Field;
        }

        public override void Visit(TermRangeNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                node.Field = _aliasFunc(node.Field) ?? node.Field;
        }

        public override void Visit(ExistsNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                node.Field = _aliasFunc(node.Field) ?? node.Field;
        }

        public override void Visit(MissingNode node) {
            if (!String.IsNullOrEmpty(node.Field))
                node.Field = _aliasFunc(node.Field) ?? node.Field;
        }

        public override IQueryNode Accept(IQueryNode node) {
            node.Accept(this, false);
            return node;
        }

        public static IQueryNode Run(IQueryNode node, Func<string, string> aliasFunc) {
            return new AliasedQueryVisitor(aliasFunc).Accept(node);
        }

        public static IQueryNode Run(IQueryNode node, IDictionary<string, string> aliasMap) {
            Run(node, field => aliasMap.ContainsKey(field) ? aliasMap[field] : field);
            return node;
        }
    }
}
