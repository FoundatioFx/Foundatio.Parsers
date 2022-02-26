using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public interface IQueryNodeVisitor {
    Task<IQueryNode> VisitAsync(IQueryNode node, IQueryVisitorContext context);
}
