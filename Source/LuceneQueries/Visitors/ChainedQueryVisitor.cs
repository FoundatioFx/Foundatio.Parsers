using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class ChainedQueryVisitor : QueryNodeVisitorWithResultBase<IQueryNode>, IChainableQueryVisitor {
        private readonly List<QueryVisitorWithPriority> _visitors = new List<QueryVisitorWithPriority>();
        private bool _isSorted = false;

        public void AddVisitor(IQueryNodeVisitorWithResult<IQueryNode> visitor, int priority = 0) {
            AddVisitor(new QueryVisitorWithPriority {
                Priority = priority,
                Visitor = visitor
            });
        }

        public void AddVisitor(QueryVisitorWithPriority visitor) {
            _visitors.Add(visitor);
            _isSorted = false;
        }

        public override IQueryNode Accept(IQueryNode node, IQueryVisitorContext context) {
            if (!_isSorted)
                _visitors.Sort(QueryVisitorWithPriority.PriorityComparer.Instance);

            foreach (var visitor in _visitors)
                visitor.Accept(node, context);

            return node;
        }
    }
}
