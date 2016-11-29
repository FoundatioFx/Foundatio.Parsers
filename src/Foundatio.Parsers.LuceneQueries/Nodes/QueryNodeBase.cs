using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes {
    public abstract class QueryNodeBase : IQueryNode {
        public virtual Task AcceptAsync(IQueryNodeVisitor visitor, IQueryVisitorContext context) {
            if (this is GroupNode)
                return visitor.VisitAsync((GroupNode)this, context);

            if (this is TermNode)
                return visitor.VisitAsync((TermNode)this, context);

            if (this is TermRangeNode)
                return visitor.VisitAsync((TermRangeNode)this, context);

            if (this is MissingNode)
                return visitor.VisitAsync((MissingNode)this, context);

            if (this is ExistsNode)
                return visitor.VisitAsync((ExistsNode)this, context);

            return Task.CompletedTask;
        }

        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
        public abstract IEnumerable<IQueryNode> Children { get; }
        public IQueryNode Parent { get; set; }
        public static readonly IList<IQueryNode> EmptyNodeList = new List<IQueryNode>().AsReadOnly();

        public abstract override string ToString();
    }
}