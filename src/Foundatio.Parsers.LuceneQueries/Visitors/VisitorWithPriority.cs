using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

[DebuggerDisplay("{Priority}, {Visitor}")]
public class QueryVisitorWithPriority : IChainableQueryVisitor
{
    public int Priority { get; set; }
    public required IQueryNodeVisitorWithResult<IQueryNode> Visitor { get; set; }

    public Task<IQueryNode> AcceptAsync(IQueryNode node, IQueryVisitorContext? context)
    {
        return Visitor.AcceptAsync(node, context);
    }

    public Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context)
    {
        return Visitor.VisitAsync(node, context);
    }

    public class PriorityComparer : IComparer<QueryVisitorWithPriority>
    {
        public int Compare(QueryVisitorWithPriority? x, QueryVisitorWithPriority? y)
        {
            if (ReferenceEquals(x, y))
                return 0;

            if (x is null)
                return -1;

            if (y is null)
                return 1;

            return x.Priority.CompareTo(y.Priority);
        }

        public static readonly PriorityComparer Instance = new();
    }
}
