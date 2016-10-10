using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public abstract class QueryNodeBase : IQueryNode {
        public virtual void Accept(IQueryNodeVisitor visitor, IQueryVisitorContext context) {
            if (this is GroupNode)
                visitor.Visit((GroupNode)this, context);
            else if (this is TermNode)
                visitor.Visit((TermNode)this, context);
            else if (this is TermRangeNode)
                visitor.Visit((TermRangeNode)this, context);
            else if (this is MissingNode)
                visitor.Visit((MissingNode)this, context);
            else if (this is ExistsNode)
                visitor.Visit((ExistsNode)this, context);
        }

        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
        public abstract IList<IQueryNode> Children { get; }
        public IQueryNode Parent { get; set; }
        public static readonly IList<IQueryNode> EmptyNodeList = new List<IQueryNode>().AsReadOnly();

        public abstract override string ToString();
    }
}