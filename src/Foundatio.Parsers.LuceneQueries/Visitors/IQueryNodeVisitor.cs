using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors {
    public interface IQueryNodeVisitor {
        Task VisitAsync(GroupNode node, IQueryVisitorContext context);
        Task VisitAsync(TermNode node, IQueryVisitorContext context);
        Task VisitAsync(TermRangeNode node, IQueryVisitorContext context);
        Task VisitAsync(ExistsNode node, IQueryVisitorContext context);
        Task VisitAsync(MissingNode node, IQueryVisitorContext context);
    }
}
