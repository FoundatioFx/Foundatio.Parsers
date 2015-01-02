using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public abstract class QueryNodeBase : IQueryNode, IEnumerable<IQueryNode> {
        public virtual void Accept(IQueryNodeVisitor visitor, bool visitChildren = true) {
            if (this is GroupNode)
                visitor.Visit((GroupNode)this);
            else if (this is TermNode)
                visitor.Visit((TermNode)this);
            else if (this is FieldExpressionNode)
                visitor.Visit((FieldExpressionNode)this);

            if (!visitChildren)
                return;

            foreach (var child in this.Where(child => child != null))
                child.Accept(visitor);
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