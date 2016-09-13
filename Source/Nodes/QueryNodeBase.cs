using System;
using System.Collections.Generic;
using Exceptionless.LuceneQueryParser.Visitor;

namespace Exceptionless.LuceneQueryParser.Nodes {
    public abstract class QueryNodeBase : IQueryNode {
        public virtual void Accept(IQueryNodeVisitor visitor) {
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
        }

        public abstract override string ToString();

        public abstract IList<IQueryNode> Children { get; }

        public GroupNode Parent { get; set; }

        public static readonly IList<IQueryNode> EmptyNodeList = new List<IQueryNode>().AsReadOnly();
    }
}