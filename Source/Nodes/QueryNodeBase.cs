using System.Collections;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public abstract class QueryNodeBase : IQueryNode, IEnumerable<IQueryNode> {
        public virtual void Accept(IQueryNodeVisitor visitor) {
            visitor.Visit(this);

            foreach (var child in this) {
                if (child != null)
                    child.Accept(visitor);
            }
        }

        public abstract IEnumerable<IQueryNode> Children { get; }

        public virtual IEnumerator<IQueryNode> GetEnumerator() {
            var childEnumerator = Children.GetEnumerator();
            while (childEnumerator.MoveNext())
                yield return childEnumerator.Current;
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }
    }
}