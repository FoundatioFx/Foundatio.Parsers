using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Parsers.LuceneQueries.Nodes;

public abstract class QueryNodeBase : IQueryNode {
    public virtual Task<IQueryNode> AcceptAsync(IQueryNodeVisitor visitor, IQueryVisitorContext context) {
        if (this is GroupNode groupNode)
            return visitor.VisitAsync(groupNode, context);

        if (this is TermNode termNode)
            return visitor.VisitAsync(termNode, context);

        if (this is TermRangeNode rangeNode)
            return visitor.VisitAsync(rangeNode, context);

        if (this is MissingNode missingNode)
            return visitor.VisitAsync(missingNode, context);

        if (this is ExistsNode existsNode)
            return visitor.VisitAsync(existsNode, context);

        return Task.FromResult<IQueryNode>(this);
    }

    public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
    public abstract IEnumerable<IQueryNode> Children { get; }
    public IQueryNode Parent { get; set; }
    public static readonly IList<IQueryNode> EmptyNodeList = new List<IQueryNode>().AsReadOnly();

    public abstract IQueryNode Clone();
    public abstract override string ToString();
}
