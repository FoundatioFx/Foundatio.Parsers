using System;
using System.Collections.Generic;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
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

        public IDictionary<string, object> Meta { get; } = new Dictionary<string, object>();
        public abstract IList<IQueryNode> Children { get; }
        public IQueryNode Parent { get; set; }
        public static readonly IList<IQueryNode> EmptyNodeList = new List<IQueryNode>().AsReadOnly();

        public abstract override string ToString();
    }
}