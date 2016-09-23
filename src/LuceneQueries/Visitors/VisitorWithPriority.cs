using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorWithPriority : IChainableQueryVisitor {
        public int Priority { get; set; }
        public IQueryNodeVisitorWithResult<IQueryNode> Visitor { get; set; }

        public IQueryNode Accept(IQueryNode node, IQueryVisitorContext context) {
            return Visitor.Accept(node, context);
        }

        public void Visit(GroupNode node, IQueryVisitorContext context) {
            Visitor.Visit(node, context);
        }

        public void Visit(TermNode node, IQueryVisitorContext context) {
            Visitor.Visit(node, context);
        }

        public void Visit(TermRangeNode node, IQueryVisitorContext context) {
            Visitor.Visit(node, context);
        }

        public void Visit(ExistsNode node, IQueryVisitorContext context) {
            Visitor.Visit(node, context);
        }

        public void Visit(MissingNode node, IQueryVisitorContext context) {
            Visitor.Visit(node, context);
        }

        public class PriorityComparer : IComparer<QueryVisitorWithPriority> {
            public int Compare(QueryVisitorWithPriority x, QueryVisitorWithPriority y) {
                return x.Priority.CompareTo(y.Priority);
            }

            public static readonly PriorityComparer Instance = new PriorityComparer();
        }
    }
}
