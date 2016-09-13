using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class QueryVisitorWithPriority : IChainableQueryVisitor {
        public int Priority { get; set; }
        public IQueryNodeVisitorWithResult<IQueryNode> Visitor { get; set; }

        public IQueryNode Accept(IQueryNode node) {
            return Visitor.Accept(node);
        }

        public void Visit(GroupNode node) {
            Visitor.Visit(node);
        }

        public void Visit(TermNode node) {
            Visitor.Visit(node);
        }

        public void Visit(TermRangeNode node) {
            Visitor.Visit(node);
        }

        public void Visit(ExistsNode node) {
            Visitor.Visit(node);
        }

        public void Visit(MissingNode node) {
            Visitor.Visit(node);
        }

        public class PriorityComparer : IComparer<QueryVisitorWithPriority> {
            public int Compare(QueryVisitorWithPriority x, QueryVisitorWithPriority y) {
                return x.Priority.CompareTo(y.Priority);
            }

            public static readonly PriorityComparer Instance = new PriorityComparer();
        }
    }
}
