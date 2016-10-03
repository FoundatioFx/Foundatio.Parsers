using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public class ChainedQueryVisitor : QueryNodeVisitorWithResultBase<IQueryNode>, IChainableQueryVisitor {
        private readonly List<QueryVisitorWithPriority> _visitors = new List<QueryVisitorWithPriority>();
        private QueryVisitorWithPriority[] _frozenVisitors;
        private bool _isDirty = true;

        public void AddVisitor(IQueryNodeVisitorWithResult<IQueryNode> visitor, int priority = 0) {
            AddVisitor(new QueryVisitorWithPriority {
                Priority = priority,
                Visitor = visitor
            });
        }

        public void AddVisitor(QueryVisitorWithPriority visitor) {
            _visitors.Add(visitor);
            _isDirty = true;
        }

        public override IQueryNode Accept(IQueryNode node, IQueryVisitorContext context) {
            if (_isDirty)
                _frozenVisitors = _visitors.OrderBy(v => v.Priority).ToArray();

            foreach (var visitor in _frozenVisitors)
                visitor.Accept(node, context);

            return node;
        }
    }
}
