using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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

        public void RemoveVisitor<T>() where T : IChainableQueryVisitor {
            var visitor = _visitors.FirstOrDefault(v => typeof(T) == v.Visitor.GetType());
            if (visitor == null) 
                return;
            
            _visitors.Remove(visitor);
            _isDirty = true;
        }

        public void ReplaceVisitor<T>(IChainableQueryVisitor visitor, int? newPriority = null) where T : IChainableQueryVisitor {
            int priority = newPriority.GetValueOrDefault(0);
            
            var referenceVisitor = _visitors.FirstOrDefault(v => typeof(T) == v.Visitor.GetType());
            if (referenceVisitor != null) {
                if (!newPriority.HasValue)
                    priority = referenceVisitor.Priority - 1;

                _visitors.Remove(referenceVisitor);
            }

            _visitors.Add(new QueryVisitorWithPriority { Visitor = visitor, Priority = priority });
            _isDirty = true;
        }

        public void AddVisitorBefore<T>(IChainableQueryVisitor visitor) {
            int priority = 0;
            var referenceVisitor = _visitors.FirstOrDefault(v => typeof(T) == v.Visitor.GetType());
            if (referenceVisitor != null)
                priority = referenceVisitor.Priority - 1;

            _visitors.Add(new QueryVisitorWithPriority { Visitor = visitor, Priority = priority });
            _isDirty = true;
        }

        public void AddVisitorAfter<T>(IChainableQueryVisitor visitor) {
            int priority = 0;
            var referenceVisitor = _visitors.FirstOrDefault(v => typeof(T) == v.Visitor.GetType());
            if (referenceVisitor != null)
                priority = referenceVisitor.Priority + 1;

            _visitors.Add(new QueryVisitorWithPriority { Visitor = visitor, Priority = priority });
            _isDirty = true;
        }

        public override async Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext context) {
            if (_isDirty)
                _frozenVisitors = _visitors.OrderBy(v => v.Priority).ToArray();

            foreach (var visitor in _frozenVisitors)
                await visitor.AcceptAsync(node, context).ConfigureAwait(false);

            return node;
        }
    }
}