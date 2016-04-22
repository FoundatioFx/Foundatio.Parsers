using System.Collections.Generic;
using System.Linq;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public abstract class QueryNodeBase : IQueryNode {
        public virtual void Accept(IQueryNodeVisitor visitor, bool visitChildren = true) {
            if (this is GroupNode)
                visitor.Visit((GroupNode)this);
            else if (this is TermNode)
                visitor.Visit((TermNode)this);
            else if (this is TermRangeNode)
                visitor.Visit((TermRangeNode)this);
            else if (this is MissingNode)
                visitor.Visit((MissingNode)this);
            else if (this is ExistsNode)
                visitor.Visit((ExistsNode)this);

            if (!visitChildren)
                return;

            foreach (var child in Children.Where(child => child != null))
                child.Accept(visitor);
        }

        public abstract IList<IQueryNode> Children { get; }

        public static readonly IList<IQueryNode> EmptyNodeList = new List<IQueryNode>();
    }
}