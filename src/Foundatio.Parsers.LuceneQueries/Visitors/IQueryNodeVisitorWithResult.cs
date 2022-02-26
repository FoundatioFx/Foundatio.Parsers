using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Nodes;

namespace Foundatio.Parsers.LuceneQueries.Visitors;

public interface IQueryNodeVisitorWithResult<T> : IQueryNodeVisitor {
    Task<T> AcceptAsync(IQueryNode node, IQueryVisitorContext context);
}
